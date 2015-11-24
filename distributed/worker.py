from __future__ import print_function, division, absolute_import

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import logging
from multiprocessing.pool import ThreadPool
import traceback
import sys

from toolz import merge
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop

from .client import _gather, pack_data
from .core import rpc, connect_sync, read_sync, write_sync, connect, Server
from .sizeof import sizeof
from .utils import funcname

_ncores = ThreadPool()._processes


logger = logging.getLogger(__name__)


class Worker(Server):
    """ Worker Node

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Additionally workers keep a Center informed of their data and use that
    Center to gather data from other workers when necessary to perform a
    computation.

    You can start a worker with the ``dworker`` command line application.

    Examples
    --------

    Create centers and workers in Python:

    >>> from distributed import Center, Worker
    >>> c = Center('192.168.0.100', 8000)  # doctest: +SKIP
    >>> w = Worker('192.168.0.101', 8001,  # doctest: +SKIP
    ...            center_ip='192.168.0.100', center_port=8000)

    Or use the command line::

       $ dcenter
       Start center at 127.0.0.1:8787

       $ dworker 127.0.0.1:8787
       Start worker at:            127.0.0.1:8788
       Registered with center at:  127.0.0.1:8787

    See Also
    --------
    distributed.center.Center:
    """

    def __init__(self, ip, port, center_ip, center_port, ncores=None,
                 loop=None, nanny_port=None, **kwargs):
        self.ip = ip
        self.port = port
        self.nanny_port = nanny_port
        self.ncores = ncores or _ncores
        self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self.executor = ThreadPoolExecutor(self.ncores)
        self.center = rpc(ip=center_ip, port=center_port)

        handlers = {'compute': self.compute,
                    'get_data': self.get_data,
                    'update_data': self.update_data,
                    'delete_data': self.delete_data,
                    'terminate': self.terminate}

        super(Worker, self).__init__(handlers, **kwargs)

    @gen.coroutine
    def _start(self):
        while True:
            try:
                logger.info('Start worker at             %s:%d', self.ip, self.port)
                self.listen(self.port)
                break
            except (OSError, IOError):
                logger.info('Port %d taken. Trying %d', self.port, self.port + 1)
                self.port += 1

        logger.info('Waiting to connect to       %s:%d', self.center.ip, self.center.port)
        while True:
            try:
                resp = yield self.center.register(
                        ncores=self.ncores, address=(self.ip, self.port),
                        nanny_port=self.nanny_port)
                break
            except OSError:
                logger.debug("Unable to register with center.  Waiting")
                yield gen.sleep(0.5)
        assert resp == b'OK'
        logger.info('Registered with center at:  %s:%d',
                    self.center.ip, self.center.port)
        self.status = 'running'

    def start(self):
        self.loop.add_callback(self._start)

    @gen.coroutine
    def _close(self):
        yield self.center.unregister(address=(self.ip, self.port))
        self.center.close_streams()
        self.stop()
        self.executor.shutdown()
        self.status = 'closed'

    @gen.coroutine
    def terminate(self, stream):
        yield self._close()
        raise Return(b'OK')

    @property
    def address(self):
        return (self.ip, self.port)

    @gen.coroutine
    def compute(self, stream, function=None, key=None, args=(), kwargs={}, needed=[]):
        """ Execute function """
        needed = [n for n in needed if n not in self.data]

        # gather data from peers
        if needed:
            logger.info("gather %d keys from peers: %s", len(needed), str(needed))
            try:
                other = yield _gather(self.center, needed=needed)
            except KeyError as e:
                logger.warn("Could not find data during gather in compute", e)
                raise Return((b'missing-data', e))
            data2 = merge(self.data, dict(zip(needed, other)))
        else:
            data2 = self.data

        # Fill args with data
        args2 = pack_data(args, data2)
        kwargs2 = pack_data(kwargs, data2)

        # Log and compute in separate thread
        try:
            job_counter[0] += 1
            i = job_counter[0]
            logger.info("Start job %d: %s", i, funcname(function))
            future = self.executor.submit(function, *args2, **kwargs)
            while not future.done():
                try:
                    yield gen.with_timeout(timedelta(seconds=1), future)
                    break
                except gen.TimeoutError:
                    logger.debug("Pending job %d: %s", i, future)
            result = future.result()
            logger.info("Finish job %d: %s", i, funcname(function))
            self.data[key] = result
            response = yield self.center.add_keys(address=(self.ip, self.port),
                                                  keys=[key])
            if not response == b'OK':
                logger.warn('Could not report results of work to center: %s',
                            response.decode())
            out = (b'OK', {'nbytes': sizeof(result)})
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = traceback.format_tb(exc_traceback)
            logger.warn(" Compute Failed\n"
                "Function: %s\n"
                "args:     %s\n"
                "kwargs:   %s\n", funcname(function), str(args2), str(kwargs2),
                exc_info=True)
            out = (b'error', (e, tb))

        raise Return(out)

    @gen.coroutine
    def update_data(self, stream, data=None, report=True):
        self.data.update(data)
        if report:
            response = yield self.center.add_keys(address=(self.ip, self.port),
                                                  keys=list(data))
            assert response == b'OK'
        info = {'nbytes': {k: sizeof(v) for k, v in data.items()}}
        raise Return((b'OK', info))

    @gen.coroutine
    def delete_data(self, stream, keys=None, report=True):
        for key in keys:
            if key in self.data:
                del self.data[key]
        logger.debug("Deleted %d keys", len(keys))
        if report:
            logger.debug("Reporting loss of keys to center")
            yield self.center.remove_keys(address=(self.ip, self.port),
                                          keys=keys)
        raise Return(b'OK')

    def get_data(self, stream, keys=None):
        return {k: self.data[k] for k in keys if k in self.data}


job_counter = [0]
