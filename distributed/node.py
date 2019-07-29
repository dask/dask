import logging
import warnings
import weakref

from tornado.ioloop import IOLoop
from tornado import gen
import dask

from .core import Server, ConnectionPool
from .versions import get_versions
from .utils import DequeHandler


class Node(object):
    """
    Base class for nodes in a distributed cluster.
    """

    def __init__(
        self,
        connection_limit=512,
        deserialize=True,
        connection_args=None,
        io_loop=None,
        serializers=None,
        deserializers=None,
        timeout=None,
    ):
        self.io_loop = io_loop or IOLoop.current()
        self.rpc = ConnectionPool(
            limit=connection_limit,
            deserialize=deserialize,
            serializers=serializers,
            deserializers=deserializers,
            connection_args=connection_args,
            timeout=timeout,
            server=self,
        )


class ServerNode(Node, Server):
    """
    Base class for server nodes in a distributed cluster.
    """

    # TODO factor out security, listening, services, etc. here

    # XXX avoid inheriting from Server? there is some large potential for confusion
    # between base and derived attribute namespaces...

    def __init__(
        self,
        handlers=None,
        blocked_handlers=None,
        stream_handlers=None,
        connection_limit=512,
        deserialize=True,
        connection_args=None,
        io_loop=None,
        serializers=None,
        deserializers=None,
        timeout=None,
    ):
        Node.__init__(
            self,
            deserialize=deserialize,
            connection_limit=connection_limit,
            connection_args=connection_args,
            io_loop=io_loop,
            serializers=serializers,
            deserializers=deserializers,
            timeout=timeout,
        )
        Server.__init__(
            self,
            handlers=handlers,
            blocked_handlers=blocked_handlers,
            stream_handlers=stream_handlers,
            connection_limit=connection_limit,
            deserialize=deserialize,
            io_loop=self.io_loop,
        )

    def versions(self, comm=None, packages=None):
        return get_versions(packages=packages)

    def start_services(self, default_listen_ip):
        if default_listen_ip == "0.0.0.0":
            default_listen_ip = ""  # for IPV6

        for k, v in self.service_specs.items():
            listen_ip = None
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            if isinstance(port, str):
                port = port.split(":")

            if isinstance(port, (tuple, list)):
                if len(port) == 2:
                    listen_ip, port = (port[0], int(port[1]))
                elif len(port) == 1:
                    [listen_ip], port = port, 0
                else:
                    raise ValueError(port)

            if isinstance(v, tuple):
                v, kwargs = v
            else:
                kwargs = {}

            try:
                service = v(self, io_loop=self.loop, **kwargs)
                service.listen(
                    (listen_ip if listen_ip is not None else default_listen_ip, port)
                )
                self.services[k] = service
            except Exception as e:
                warnings.warn(
                    "\nCould not launch service '%s' on port %s. " % (k, port)
                    + "Got the following message:\n\n"
                    + str(e),
                    stacklevel=3,
                )

    def stop_services(self):
        for service in self.services.values():
            service.stop()

    @property
    def service_ports(self):
        return {k: v.port for k, v in self.services.items()}

    def _setup_logging(self, logger):
        self._deque_handler = DequeHandler(
            n=dask.config.get("distributed.admin.log-length")
        )
        self._deque_handler.setFormatter(
            logging.Formatter(dask.config.get("distributed.admin.log-format"))
        )
        logger.addHandler(self._deque_handler)
        weakref.finalize(self, logger.removeHandler, self._deque_handler)

    def get_logs(self, comm=None, n=None):
        deque_handler = self._deque_handler
        if n is None:
            L = list(deque_handler.deque)
        else:
            L = deque_handler.deque
            L = [L[-i] for i in range(min(n, len(L)))]
        return [(msg.levelname, deque_handler.format(msg)) for msg in L]

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    def __await__(self):
        if self.status == "running":
            return gen.sleep(0).__await__()
        else:
            return self.start().__await__()

    async def start(self):  # subclasses should implement this
        return self
