from __future__ import print_function, division, absolute_import

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from importlib import import_module
import logging
from multiprocessing.pool import ThreadPool
import os
import pkg_resources
import tempfile
import shutil
import sys

from toolz import merge
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError

from .client import _gather, pack_data, gather_from_workers
from .compatibility import reload
from .core import rpc, Server, pingpong
from .sizeof import sizeof
from .utils import funcname, get_ip, get_traceback, truncate_exception

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

    You can start a worker with the ``dworker`` command line application::

        $ dworker scheduler-ip:port

    **State**

    * **data:** ``{key: object}``:
        Dictionary mapping keys to actual values
    * **active:** ``{key}``:
        Set of keys currently under computation
    * **ncores:** ``int``:
        Number of cores used by this worker process
    * **executor:** ``concurrent.futures.ThreadPoolExecutor``:
        Executor used to perform computation
    * **local_dir:** ``path``:
        Path on local machine to store temporary files
    * **center:** ``rpc``:
        Location of center or scheduler.  See ``.ip/.port`` attributes.
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:

    Examples
    --------

    Create centers and workers in Python:

    >>> from distributed import Center, Worker
    >>> c = Center('192.168.0.100', 8787)  # doctest: +SKIP
    >>> w = Worker(c.ip, c.port)  # doctest: +SKIP
    >>> yield w._start(port=8788)  # doctest: +SKIP

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

    def __init__(self, center_ip, center_port, ip=None, ncores=None,
                 loop=None, local_dir=None, services=None, service_ports=None,
                 **kwargs):
        self.ip = ip or get_ip()
        self._port = 0
        self.ncores = ncores or _ncores
        self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self.local_dir = local_dir or tempfile.mkdtemp(prefix='worker-')
        self.executor = ThreadPoolExecutor(self.ncores)
        self.center = rpc(ip=center_ip, port=center_port)
        self.active = set()
        if services is not None:
            self.services = {k: v(self) for k, v in services.items()}
        else:
            self.services = dict()
        self.service_ports = service_ports or dict()

        if not os.path.exists(self.local_dir):
            os.mkdir(self.local_dir)

        if self.local_dir not in sys.path:
            sys.path.insert(0, self.local_dir)

        handlers = {'compute': self.compute,
                    'get_data': self.get_data,
                    'update_data': self.update_data,
                    'delete_data': self.delete_data,
                    'terminate': self.terminate,
                    'ping': pingpong,
                    'upload_file': self.upload_file}

        super(Worker, self).__init__(handlers, **kwargs)

    @gen.coroutine
    def _start(self, port=0):
        self.listen(port)
        for k, v in self.services.items():
            v.listen(0)
            self.service_ports[k] = v.port

        logger.info('      Start worker at: %20s:%d', self.ip, self.port)
        for k, v in self.service_ports.items():
            logger.info('  %16s at: %20s:%d' % (k, self.ip, v))
        logger.info('Waiting to connect to: %20s:%d',
                    self.center.ip, self.center.port)
        while True:
            try:
                resp = yield self.center.register(
                        ncores=self.ncores, address=(self.ip, self.port),
                        keys=list(self.data), services=self.service_ports)
                break
            except (OSError, StreamClosedError):
                logger.debug("Unable to register with center.  Waiting")
                yield gen.sleep(0.5)
        assert resp == b'OK'
        logger.info('        Registered to: %20s:%d',
                    self.center.ip, self.center.port)
        self.status = 'running'

    def start(self, port=0):
        self.loop.add_callback(lambda: self._start(port))

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id,
                'center': (self.center.ip, self.center.port)}

    @gen.coroutine
    def _close(self, report=True, timeout=10):
        if report:
            yield gen.with_timeout(timedelta(seconds=timeout),
                    self.center.unregister(address=(self.ip, self.port)))
        self.center.close_streams()
        self.stop()
        self.executor.shutdown()
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)
        self.status = 'closed'

    @gen.coroutine
    def terminate(self, stream, report=True):
        yield self._close(report=report)
        raise Return(b'OK')

    @property
    def address(self):
        return (self.ip, self.port)

    @gen.coroutine
    def compute(self, stream, function=None, key=None, args=(), kwargs={},
            needed=[], who_has=None, report=True):
        """ Execute function """
        self.active.add(key)
        if needed:
            local_data = {k: self.data[k] for k in needed if k in self.data}
            needed = [n for n in needed if n not in self.data]
        elif who_has:
            local_data = {k: self.data[k] for k in who_has if k in self.data}
            who_has = {k: v for k, v in who_has.items() if k not in self.data}
        else:
            local_data = {}

        # gather data from peers
        if needed or who_has:
            try:
                if who_has:
                    logger.info("gather %d keys from peers: %s",
                                len(who_has), str(who_has))
                    other = yield gather_from_workers(who_has)
                elif needed:
                    logger.info("gather %d keys from peers: %s",
                                len(needed), str(needed))
                    other = yield _gather(self.center, needed=needed)
                    other = dict(zip(needed, other))
                else:
                    raise ValueError()
            except KeyError as e:
                logger.warn("Could not find data during gather in compute",
                            exc_info=True)
                self.active.remove(key)
                raise Return((b'missing-data', e))
            data2 = merge(local_data, other)
        else:
            data2 = local_data

        # Fill args with data
        args2 = pack_data(args, data2)
        kwargs2 = pack_data(kwargs, data2)

        # Log and compute in separate thread
        try:
            job_counter[0] += 1
            i = job_counter[0]
            logger.info("Start job %d: %s - %s", i, funcname(function), key)
            future = self.executor.submit(function, *args2, **kwargs)
            pc = PeriodicCallback(lambda: logger.debug("future state: %s - %s",
                key, future._state), 1000)
            pc.start()
            try:
                if sys.version_info < (3, 2):
                    yield future
                else:
                    while not future.done() and future._state != 'FINISHED':
                        try:
                            yield gen.with_timeout(timedelta(seconds=1), future)
                            break
                        except gen.TimeoutError:
                            logger.info("work queue size: %d", self.executor._work_queue.qsize())
                            logger.info("future state: %s", future._state)
                            logger.info("Pending job %d: %s", i, future)
            finally:
                pc.stop()
            result = future.result()
            logger.info("Finish job %d: %s - %s", i, funcname(function), key)
            self.data[key] = result
            if report:
                response = yield self.center.add_keys(address=(self.ip, self.port),
                                                      keys=[key])
                if not response == b'OK':
                    logger.warn('Could not report results to center: %s',
                                response.decode())
            out = (b'OK', {'nbytes': sizeof(result)})
        except Exception as e:
            tb = get_traceback()
            e2 = truncate_exception(e, 1000)

            logger.warn(" Compute Failed\n"
                "Function: %s\n"
                "args:     %s\n"
                "kwargs:   %s\n",
                str(funcname(function))[:1000], str(args2)[:1000],
                str(kwargs2)[:1000])

            out = (b'error', (e2, tb))

        logger.debug("Send compute response to client: %s, %s", key, out)
        self.active.remove(key)
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

    def upload_file(self, stream, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_dir, filename)
        with open(out_filename, 'wb') as f:
            f.write(data)
            f.flush()

        if load:
            try:
                name, ext = os.path.splitext(filename)
                if ext in ('.py', '.pyc'):
                    logger.info("Reload module %s from .py file", name)
                    name = name.split('-')[0]
                    reload(import_module(name))
                if ext == '.egg':
                    sys.path.append(out_filename)
                    pkgs = pkg_resources.find_distributions(out_filename)
                    for pkg in pkgs:
                        logger.info("Load module %s from egg", pkg.project_name)
                        reload(import_module(pkg.project_name))
                    if not pkgs:
                        logger.warning("Found no packages in egg file")
            except Exception as e:
                logger.exception(e)
                return e
        return len(data)


job_counter = [0]
