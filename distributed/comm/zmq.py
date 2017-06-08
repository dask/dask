from __future__ import print_function, division, absolute_import

import logging

from tornado import gen, ioloop

import zmq
from zmq.eventloop import ioloop as zmqioloop

from ..utils import PY3, ensure_ip, get_ip, get_ipv6

from . import zmqimpl
from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, CommClosedError, Connector, Listener
from .utils import to_frames, from_frames, ensure_concrete_host


logger = logging.getLogger(__name__)


def install():
    ioloop.IOLoop.clear_current()
    ioloop.IOLoop.clear_instance()
    zmqioloop.install()

    inst = ioloop.IOLoop.instance()
    if not isinstance(inst, zmqioloop.IOLoop):
        raise RuntimeError("failed to install ZeroMQ IO Loop, got %r" % (inst,))


install()


NOCOPY_THRESHOLD = 1000 ** 2   # 1 MB


#async_ctx = Context()
## Workaround https://github.com/zeromq/pyzmq/issues/962
#async_ctx.io_loop = None

#def make_socket(sockty):
    #sock = async_ctx.socket(sockty)
    #return sock

ctx = zmqimpl.Context()


def make_socket(sockty):
    sock = ctx.socket(sockty)
    return sock


def set_socket_options(sock):
    """
    Set common options on a ZeroMQ socket.
    """


def make_zmq_url(ip, port=0):
    if not port:
        port = '*'
    return "tcp://" + unparse_host_port(ip, port)


def enable_ipv6(sock, ip):
    # Ideally, we would enable IPv6 blindly on all ZMQ sockets, but
    # we can't because of a bug on Windows:
    # https://github.com/zeromq/libzmq/issues/2124
    sock.set(zmq.IPV6, ':' in ip)


def bind_to_random_port(sock, ip):
    # ZMQ doesn't support binding to '' ("No such device").
    # It also doesn't support binding to both '0.0.0.0' and '::'
    # on the same port ("Address already in use").
    # Instead, just use IPv4.
    ip = ip or '0.0.0.0'
    enable_ipv6(sock, ip)
    sock.bind(make_zmq_url(ip))


def bind_to_port(sock, ip, port):
    ip = ip or '0.0.0.0'
    enable_ipv6(sock, ip)
    sock.bind(make_zmq_url(ip, port))


def get_last_endpoint(sock):
    """
    Get the last (host, port) the socket was bound to.
    """
    endpoint = sock.get(zmq.LAST_ENDPOINT).decode()
    scheme, sep, loc = endpoint.partition('://')
    assert scheme == 'tcp'
    host, sep, port = loc.rpartition(':')
    assert sep
    return host.strip('[]'), int(port)


class ZMQ(Comm):
    """
    An established communication based on an underlying ZeroMQ socket.
    The socket is assumed to be a DEALER socket.
    """

    def __init__(self, sock, peer_addr, deserialize=True):
        self.sock = sock
        self.deserialize = deserialize
        self._peer_addr = peer_addr
        # XXX socket timeouts

    @property
    def peer_address(self):
        return self._peer_addr

    @gen.coroutine
    def read(self):
        if self.sock is None:
            raise CommClosedError

        frames = yield self.sock.recv_multipart(copy=False)
        if PY3:
            msg = from_frames([f.buffer for f in frames],
                              deserialize=self.deserialize)
        else:
            # On Python 2, msgpack-python doesn't accept new-style buffer objects
            msg = from_frames([f.bytes for f in frames],
                              deserialize=self.deserialize)
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        if self.sock is None:
            raise CommClosedError

        frames = to_frames(msg)
        copy = all(len(f) < NOCOPY_THRESHOLD for f in frames)
        yield self.sock.send_multipart(frames, copy=copy)
        raise gen.Return(sum(map(len, frames)))

    @gen.coroutine
    def close(self):
        sock, self.sock = self.sock, None
        if sock is not None and not sock.closed:
            sock.close(linger=5000)   # 5 seconds

    def abort(self):
        sock, self.sock = self.sock, None
        if sock is not None and not sock.closed:
            sock.close(linger=0)      # no wait

    def closed(self):
        return self.sock is None


class ZMQConnector(Connector):

    @gen.coroutine
    def _do_connect(self, sock, address, listener_url, deserialize=True):
        sock.connect(listener_url)

        req = {'op': 'zmq-connect'}
        yield sock.send_multipart(to_frames(req))
        frames = yield sock.recv_multipart(copy=True)
        sock.disconnect(listener_url)

        resp = from_frames(frames)
        sock.connect(resp['zmq-url'])

        comm = ZMQ(sock, 'zmq://' + address, deserialize)
        raise gen.Return(comm)

    @gen.coroutine
    def connect(self, address, deserialize=True, **connection_args):
        host, port = parse_host_port(address)
        listener_url = make_zmq_url(host, port)
        sock = make_socket(zmq.DEALER)
        set_socket_options(sock)
        enable_ipv6(sock, host)

        comm = yield self._do_connect(sock, address, listener_url, deserialize)
        raise gen.Return(comm)


class ZMQListener(Listener):

    def __init__(self, address, comm_handler, deserialize=True, default_port=0):
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.sock = None
        self.bound_host = None
        self.bound_port = None
        self.please_stop = False

    def start(self):
        self.sock = make_socket(zmq.ROUTER)
        set_socket_options(self.sock)
        if self.port == 0:
            bind_to_random_port(self.sock, self.ip)
        else:
            bind_to_port(self.sock, self.ip, self.port)
        self.bound_host, self.bound_port = get_last_endpoint(self.sock)
        self._listen()

    @gen.coroutine
    def _listen(self):
        while not self.please_stop:
            frames = yield self.sock.recv_multipart()
            envelope = frames[0]
            req = from_frames(frames[1:])
            assert req['op'] == 'zmq-connect'

            cli_sock = make_socket(zmq.DEALER)
            set_socket_options(cli_sock)
            # Need to force a concrete IP, otherwise tests crash on Windows
            # ("Assertion failed: Can't assign requested address
            #   (bundled\zeromq\src\tcp_connecter.cpp:341)").
            bind_to_random_port(cli_sock, ensure_concrete_host(self.ip))
            cli_host, cli_port = get_last_endpoint(cli_sock)

            resp = {'zmq-url': make_zmq_url(cli_host, cli_port)}
            yield self.sock.send_multipart([envelope] + to_frames(resp))

            address = 'zmq://<unknown>'  # XXX
            comm = ZMQ(cli_sock, address, self.deserialize)
            self.comm_handler(comm)

    def stop(self):
        sock, self.sock = self.sock, None
        if sock is not None:
            self.please_stop = True
            sock.close()
            # XXX cancel listen future?

    def _check_started(self):
        if self.bound_port is None:
            raise ValueError("invalid operation on non-started ZMQListener")

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        self._check_started()
        return self.bound_host, self.bound_port

    @property
    def listen_address(self):
        """
        The listening address as a string.
        """
        return 'zmq://' + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        """
        The contact address as a string.
        """
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)
        return 'zmq://' + unparse_host_port(host, port)


class ZMQBackend(Backend):

    # I/O

    def get_connector(self):
        return ZMQConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return ZMQListener(loc, handle_comm, deserialize)

    # Address handling

    def get_address_host(self, loc):
        return parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ':' in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends['zmq'] = ZMQBackend()
