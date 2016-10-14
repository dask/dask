from __future__ import print_function, division, absolute_import

from datetime import timedelta
from importlib import import_module
import logging
from multiprocessing.pool import ThreadPool
import os
import pkg_resources
import tempfile
from threading import current_thread, Lock, local
from time import time
from timeit import default_timer
import shutil
import sys

from dask.core import istask
from dask.compatibility import apply
try:
    from cytoolz import valmap, merge
except ImportError:
    from toolz import valmap, merge
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError

from .batched import BatchedSend
from .utils_comm import pack_data, gather_from_workers
from .compatibility import reload, unicode
from .core import (rpc, Server, pingpong, dumps, loads, coerce_to_address,
        error_message, read, RPCClosed)
from .sizeof import sizeof
from .threadpoolexecutor import ThreadPoolExecutor
from .utils import funcname, get_ip, _maybe_complex, log_errors, All, ignoring

_ncores = ThreadPool()._processes

thread_state = local()

logger = logging.getLogger(__name__)

try:
    import psutil
    TOTAL_MEMORY = psutil.virtual_memory().total
except ImportError:
    logger.warn("Please install psutil to estimate worker memory use")
    TOTAL_MEMORY = 8e9

class Worker(Server):
    """ Worker Node

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Additionally workers keep a scheduler informed of their data and use that
    scheduler to gather data from other workers when necessary to perform a
    computation.

    You can start a worker with the ``dask-worker`` command line application::

        $ dask-worker scheduler-ip:port

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
    * **scheduler:** ``rpc``:
        Location of scheduler.  See ``.ip/.port`` attributes.
    * **name:** ``string``:
        Alias
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:

    Examples
    --------

    Use the command line to start a worker::

        $ dask-scheduler
        Start scheduler at 127.0.0.1:8786

        $ dask-worker 127.0.0.1:8786
        Start worker at:               127.0.0.1:1234
        Registered with scheduler at:  127.0.0.1:8786

    See Also
    --------
    distributed.scheduler.Scheduler
    distributed.nanny.Nanny
    """

    def __init__(self, scheduler_ip, scheduler_port, ip=None, ncores=None,
                 loop=None, local_dir=None, services=None, service_ports=None,
                 name=None, heartbeat_interval=5000, memory_limit=TOTAL_MEMORY,
                 **kwargs):
        self.ip = ip or get_ip()
        self._port = 0
        self.ncores = ncores or _ncores
        self.local_dir = local_dir or tempfile.mkdtemp(prefix='worker-')
        if not os.path.exists(self.local_dir):
            os.mkdir(self.local_dir)
        self.memory_limit = memory_limit
        if memory_limit:
            try:
                from zict import Buffer, File, Func
            except ImportError:
                raise ImportError("Please `pip install zict` for spill-to-disk workers")
            path = os.path.join(self.local_dir, 'storage')
            storage = Func(dumps_to_disk, loads_from_disk, File(path))
            self.data = Buffer({}, storage, int(float(memory_limit)), weight)
        else:
            self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self.executor = ThreadPoolExecutor(self.ncores)
        self.scheduler = rpc(ip=scheduler_ip, port=scheduler_port)
        self.active = set()
        self.name = name
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_active = False
        self.execution_state = {'scheduler': self.scheduler.address,
                                'ioloop': self.loop,
                                'worker': self}
        self._last_disk_io = None
        self._last_net_io = None
        self._ipython_kernel = None

        if self.local_dir not in sys.path:
            sys.path.insert(0, self.local_dir)

        self.services = {}
        self.service_ports = service_ports or {}
        for k, v in (services or {}).items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            self.services[k] = v(self, io_loop=self.loop)
            self.services[k].listen(port)
            self.service_ports[k] = self.services[k].port

        handlers = {'compute': self.compute,
                    'gather': self.gather,
                    'compute-stream': self.compute_stream,
                    'run': self.run,
                    'get_data': self.get_data,
                    'update_data': self.update_data,
                    'delete_data': self.delete_data,
                    'terminate': self.terminate,
                    'ping': pingpong,
                    'health': self.host_health,
                    'upload_file': self.upload_file,
                    'start_ipython': self.start_ipython,
                    'keys': self.keys,
                }

        super(Worker, self).__init__(handlers, io_loop=self.loop, **kwargs)

        self.heartbeat_callback = PeriodicCallback(self.heartbeat,
                                                   self.heartbeat_interval,
                                                   io_loop=self.loop)
        self.loop.add_callback(self.heartbeat_callback.start)

    @property
    def worker_address(self):
        """ For API compatibility with Nanny """
        return self.address

    @gen.coroutine
    def heartbeat(self):
        if not self.heartbeat_active:
            self.heartbeat_active = True
            logger.debug("Heartbeat: %s" % self.address)
            try:
                yield self.scheduler.register(address=self.address, name=self.name,
                                        ncores=self.ncores,
                                        now=time(),
                                        host_info=self.host_health(),
                                        services=self.service_ports,
                                        memory_limit=self.memory_limit,
                                        **self.process_health())
            finally:
                self.heartbeat_active = False
        else:
            logger.debug("Heartbeat skipped: channel busy")

    @gen.coroutine
    def _start(self, port=0):
        self.listen(port)
        self.name = self.name or self.address
        for k, v in self.services.items():
            v.listen(0)
            self.service_ports[k] = v.port

        logger.info('      Start worker at: %20s:%d', self.ip, self.port)
        for k, v in self.service_ports.items():
            logger.info('  %16s at: %20s:%d' % (k, self.ip, v))
        logger.info('Waiting to connect to: %20s:%d',
                    self.scheduler.ip, self.scheduler.port)
        while True:
            try:
                resp = yield self.scheduler.register(
                        ncores=self.ncores, address=(self.ip, self.port),
                        keys=list(self.data),
                        name=self.name, nbytes=valmap(sizeof, self.data),
                        now=time(),
                        host_info=self.host_health(),
                        services=self.service_ports,
                        memory_limit=self.memory_limit,
                        **self.process_health())
                break
            except (OSError, StreamClosedError):
                logger.debug("Unable to register with scheduler.  Waiting")
                yield gen.sleep(0.5)
        if resp != 'OK':
            raise ValueError(resp)
        logger.info('        Registered to: %20s:%d',
                    self.scheduler.ip, self.scheduler.port)
        self.status = 'running'

    def start(self, port=0):
        self.loop.add_callback(self._start, port)

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id,
                'scheduler': (self.scheduler.ip, self.scheduler.port),
                'ncores': self.ncores,
                'memory_limit': self.memory_limit}

    @gen.coroutine
    def _close(self, report=True, timeout=10):
        self.heartbeat_callback.stop()
        with ignoring(RPCClosed, StreamClosedError):
            if report:
                yield gen.with_timeout(timedelta(seconds=timeout),
                        self.scheduler.unregister(address=(self.ip, self.port)),
                        io_loop=self.loop)
        self.scheduler.close_rpc()
        self.stop()
        self.executor.shutdown()
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)

        for k, v in self.services.items():
            v.stop()
        self.rpc.close()
        self.status = 'closed'
        self.stop()

    @gen.coroutine
    def terminate(self, stream, report=True):
        yield self._close(report=report)
        raise Return('OK')

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

    @property
    def address_tuple(self):
        return (self.ip, self.port)

    @gen.coroutine
    def gather(self, stream=None, who_has=None):
        who_has = {k: [coerce_to_address(addr) for addr in v]
                    for k, v in who_has.items()
                    if k not in self.data}
        try:
            result = yield gather_from_workers(who_has)
        except KeyError as e:
            logger.warn("Could not find data", e)
            raise Return({'status': 'missing-data',
                          'keys': e.args})
        else:
            self.data.update(result)
            raise Return({'status': 'OK'})

    def deserialize(self, function=None, args=None, kwargs=None, task=None):
        """ Deserialize task inputs and regularize to func, args, kwargs """
        if task is not None:
            task = loads(task)
        if function is not None:
            function = loads(function)
        if args:
            args = loads(args)
        if kwargs:
            kwargs = loads(kwargs)

        if task is not None:
            assert not function and not args and not kwargs
            function = execute_task
            args = (task,)

        return function, args or (), kwargs or {}

    @gen.coroutine
    def gather_many(self, msgs):
        """ Gather the data for many compute messages at once

        Returns
        -------
        good: the input messages for which we have data
        bad: a dict of task keys for which we could not find data
        data: The scope in which to run tasks
        len(remote): the number of new keys we've gathered
        """
        diagnostics = {}
        who_has = merge(msg['who_has'] for msg in msgs if 'who_has' in msg)

        start = time()
        local = {k: self.data[k] for k in who_has if k in self.data}
        stop = time()
        if stop - start > 0.005:
            diagnostics['disk_load_start'] = start
            diagnostics['disk_load_stop'] = stop

        who_has = {k: v for k, v in who_has.items() if k not in local}
        start = time()
        remote, bad_data = yield gather_from_workers(who_has,
                permissive=True)
        if remote:
            self.data.update(remote)
            yield self.scheduler.add_keys(address=self.address, keys=list(remote))
        stop = time()

        if remote:
            diagnostics['transfer_start'] = start
            diagnostics['transfer_stop'] = stop

        data = merge(local, remote)

        if bad_data:
            missing = {msg['key']: {k for k in msg['who_has'] if k in bad_data}
                        for msg in msgs if 'who_has' in msg}
            bad = {k: v for k, v in missing.items() if v}
            good = [msg for msg in msgs if not missing.get(msg['key'])]
        else:
            good, bad = msgs, {}
        raise Return([good, bad, data, len(remote), diagnostics])

    @gen.coroutine
    def _ready_task(self, function=None, key=None, args=(), kwargs={},
                    task=None, who_has=None):
        who_has = who_has or {}
        diagnostics = {}
        start = time()
        data = {k: self.data[k] for k in who_has if k in self.data}
        stop = time()

        if stop - start > 0.005:
            diagnostics['disk_load_start'] = start
            diagnostics['disk_load_stop'] = stop

        who_has = {k: set(map(coerce_to_address, v))
                   for k, v in who_has.items()
                   if k not in self.data}
        if who_has:
            try:
                logger.info("gather %d keys from peers", len(who_has))
                diagnostics['transfer_start'] = time()
                other = yield gather_from_workers(who_has)
                diagnostics['transfer_stop'] = time()
                self.data.update(other)
                yield self.scheduler.add_keys(address=self.address,
                                              keys=list(other))
                data.update(other)
            except KeyError as e:
                logger.warn("Could not find data for %s", key)
                raise Return({'status': 'missing-data',
                              'keys': e.args,
                              'key': key})

        try:
            start = default_timer()
            function, args, kwargs = self.deserialize(function, args, kwargs,
                    task)
            diagnostics['deserialization'] = default_timer() - start
        except Exception as e:
            logger.warn("Could not deserialize task", exc_info=True)
            emsg = error_message(e)
            emsg['key'] = key
            raise Return(emsg)

        # Fill args with data
        args2 = pack_data(args, data)
        kwargs2 = pack_data(kwargs, data)

        raise Return({'status': 'OK',
                      'function': function,
                      'args': args2,
                      'kwargs': kwargs2,
                      'diagnostics': diagnostics,
                      'key': key})

    @gen.coroutine
    def executor_submit(self, key, function, *args, **kwargs):
        """ Safely run function in thread pool executor

        We've run into issues running concurrent.future futures within
        tornado.  Apparently it's advantageous to use timeouts and periodic
        callbacks to ensure things run smoothly.  This can get tricky, so we
        pull it off into an separate method.
        """
        job_counter[0] += 1
        # logger.info("%s:%d Starts job %d, %s", self.ip, self.port, i, key)
        future = self.executor.submit(function, *args, **kwargs)
        pc = PeriodicCallback(lambda: logger.debug("future state: %s - %s",
            key, future._state), 1000, io_loop=self.loop); pc.start()
        try:
            yield future
        finally:
            pc.stop()
            pass

        result = future.result()

        # logger.info("Finish job %d, %s", i, key)
        raise gen.Return(result)

    @gen.coroutine
    def compute_stream(self, stream):
        with log_errors():
            logger.debug("Open compute stream")
            bstream = BatchedSend(interval=2, loop=self.loop)
            bstream.start(stream)

            closed = False
            last = gen.sleep(0)
            while not closed:
                try:
                    msgs = yield read(stream)
                except StreamClosedError:
                    break
                if not isinstance(msgs, list):
                    msgs = [msgs]

                batch = []
                for msg in msgs:
                    op = msg.pop('op', None)
                    if op == 'close':
                        closed = True
                        break
                    elif op == 'compute-task':
                        batch.append(msg)
                        logger.debug("%s asked to compute %s", self.address,
                                     msg['key'])
                    else:
                        logger.warning("Unknown operation %s, %s", op, msg)
                # self.loop.add_callback(self.compute_many, bstream, msgs)
                last = self.compute_many(bstream, msgs)

            try:
                yield last  # TODO: there might be more than one lingering
            except (RPCClosed, RuntimeError):
                pass

            yield bstream.close()
            logger.info("Close compute stream")

    @gen.coroutine
    def compute_many(self, bstream, msgs, report=False):
        good, bad, data, num_transferred, diagnostics = yield self.gather_many(msgs)

        for msg in msgs:
            msg.pop('who_has', None)

        if bad:
            logger.warn("Could not find data for %s", sorted(bad))
        for k, v in bad.items():
            bstream.send({'status': 'missing-data',
                          'key': k,
                          'keys': list(v)})

        if good:
            futures = [self.compute_one(data, report=report, **msg)
                                     for msg in good]
            wait_iterator = gen.WaitIterator(*futures)
            result = yield wait_iterator.next()
            if diagnostics:
                result.update(diagnostics)
            bstream.send(result)
            while not wait_iterator.done():
                msg = yield wait_iterator.next()
                bstream.send(msg)

    @gen.coroutine
    def compute_one(self, data, key=None, function=None, args=None, kwargs=None,
                    report=False, task=None):
        logger.debug("Compute one on %s", key)
        self.active.add(key)
        diagnostics = dict()
        try:
            start = default_timer()
            function, args, kwargs = self.deserialize(function, args, kwargs,
                    task)
            diagnostics['deserialization'] = default_timer() - start
        except Exception as e:
            logger.warn("Could not deserialize task", exc_info=True)
            emsg = error_message(e)
            emsg['key'] = key
            raise Return(emsg)

        # Fill args with data
        args2 = pack_data(args, data)
        kwargs2 = pack_data(kwargs, data)

        # Log and compute in separate thread
        result = yield self.executor_submit(key, apply_function, function,
                                            args2, kwargs2,
                                            self.execution_state, key)

        result['key'] = key
        result.update(diagnostics)

        if result['status'] == 'OK':
            self.data[key] = result.pop('result')
            if report:
                response = yield self.scheduler.add_keys(keys=[key],
                                        address=(self.ip, self.port))
                if not response == 'OK':
                    logger.warn('Could not report results to scheduler: %s',
                                str(response))
        else:
            logger.warn(" Compute Failed\n"
                "Function: %s\n"
                "args:     %s\n"
                "kwargs:   %s\n",
                str(funcname(function))[:1000],
                convert_args_to_str(args, max_len=1000),
                convert_kwargs_to_str(kwargs, max_len=1000), exc_info=True)

        logger.debug("Send compute response to scheduler: %s, %s", key,
                     result)
        try:
            self.active.remove(key)
        except KeyError:
            pass
        raise Return(result)

    @gen.coroutine
    def compute(self, stream=None, function=None, key=None, args=(), kwargs={},
            task=None, who_has=None, report=True):
        """ Execute function """
        self.active.add(key)

        # Ready function for computation
        msg = yield self._ready_task(function=function, key=key, args=args,
            kwargs=kwargs, task=task, who_has=who_has)
        if msg['status'] != 'OK':
            try:
                self.active.remove(key)
            except KeyError:
                pass
            raise Return(msg)
        else:
            function = msg['function']
            args = msg['args']
            kwargs = msg['kwargs']

        # Log and compute in separate thread
        result = yield self.executor_submit(key, apply_function, function,
                                            args, kwargs, self.execution_state,
                                            key)

        result['key'] = key
        result.update(msg['diagnostics'])

        if result['status'] == 'OK':
            self.data[key] = result.pop('result')
            if report:
                response = yield self.scheduler.add_keys(address=(self.ip, self.port),
                                                         keys=[key])
                if not response == 'OK':
                    logger.warn('Could not report results to scheduler: %s',
                                str(response))
        else:
            logger.warn(" Compute Failed\n"
                "Function: %s\n"
                "args:     %s\n"
                "kwargs:   %s\n",
                str(funcname(function))[:1000],
                convert_args_to_str(args, max_len=1000),
                convert_kwargs_to_str(kwargs, max_len=1000), exc_info=True)

        logger.debug("Send compute response to scheduler: %s, %s", key,
            get_msg_safe_str(msg))
        try:
            self.active.remove(key)
        except KeyError:
            pass
        raise Return(result)

    def run(self, stream, function=None, args=(), kwargs={}):
        return run(self, stream, function=function, args=args, kwargs=kwargs)

    @gen.coroutine
    def update_data(self, stream=None, data=None, report=True, deserialize=True):
        if deserialize:
            data = valmap(loads, data)
        self.data.update(data)
        if report:
            response = yield self.scheduler.add_keys(
                                address=(self.ip, self.port),
                                keys=list(data))
            assert response == 'OK'
        info = {'nbytes': {k: sizeof(v) for k, v in data.items()},
                'status': 'OK'}
        raise Return(info)

    @gen.coroutine
    def delete_data(self, stream, keys=None, report=True):
        if keys:
            for key in keys:
                if key in self.data:
                    del self.data[key]
            logger.info("Deleted %d keys", len(keys))
            if report:
                logger.debug("Reporting loss of keys to scheduler")
                yield self.scheduler.remove_keys(address=self.address,
                                              keys=list(keys))
        raise Return('OK')

    def get_data(self, stream, keys=None):
        return {k: dumps(self.data[k]) for k in keys if k in self.data}

    def start_ipython(self, stream):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython
        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip,
                ns={'worker': self},
                log=logger,
            )
        return self._ipython_kernel.get_connection_info()

    def upload_file(self, stream, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_dir, filename)
        if isinstance(data, unicode):
            data = data.encode()
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
                return {'status': 'error', 'exception': dumps(e)}
        return {'status': 'OK', 'nbytes': len(data)}

    def process_health(self, stream=None):
        d = {'active': len(self.active),
             'stored': len(self.data)}
        return d

    def host_health(self, stream=None):
        """ Information about worker """
        d = {'time': time()}
        try:
            import psutil
            mem = psutil.virtual_memory()
            d.update({'cpu': psutil.cpu_percent(),
                      'memory': mem.total,
                      'memory_percent': mem.percent})

            net_io = psutil.net_io_counters()
            if self._last_net_io:
                d['network-send'] = net_io.bytes_sent - self._last_net_io.bytes_sent
                d['network-recv'] = net_io.bytes_recv - self._last_net_io.bytes_recv
            else:
                d['network-send'] = 0
                d['network-recv'] = 0
            self._last_net_io = net_io

            try:
                disk_io = psutil.disk_io_counters()
            except RuntimeError:
                # This happens when there is no physical disk in worker
                pass
            else:
                if self._last_disk_io:
                    d['disk-read'] = disk_io.read_bytes - self._last_disk_io.read_bytes
                    d['disk-write'] = disk_io.write_bytes - self._last_disk_io.write_bytes
                else:
                    d['disk-read'] = 0
                    d['disk-write'] = 0
                self._last_disk_io = disk_io

        except ImportError:
            pass
        return d

    def keys(self, stream=None):
        return list(self.data)


job_counter = [0]


def execute_task(task):
    """ Evaluate a nested task

    >>> inc = lambda x: x + 1
    >>> execute_task((inc, 1))
    2
    >>> execute_task((sum, [1, 2, (inc, 3)]))
    7
    """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    elif isinstance(task, list):
        return list(map(execute_task, task))
    else:
        return task


cache = dict()


def dumps_function(func):
    """ Dump a function to bytes, cache functions """
    if func not in cache:
        b = dumps(func)
        cache[func] = b
    return cache[func]


def dumps_task(task):
    """ Serialize a dask task

    Returns a dict of bytestrings that can each be loaded with ``loads``

    Examples
    --------
    Either returns a task as a function, args, kwargs dict

    >>> from operator import add
    >>> dumps_task((add, 1))  # doctest: +SKIP
    {'function': b'\x80\x04\x95\x00\x8c\t_operator\x94\x8c\x03add\x94\x93\x94.'
     'args': b'\x80\x04\x95\x07\x00\x00\x00K\x01K\x02\x86\x94.'}

    Or as a single task blob if it can't easily decompose the result.  This
    happens either if the task is highly nested, or if it isn't a task at all

    >>> dumps_task(1)  # doctest: +SKIP
    {'task': b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'}
    """
    if istask(task):
        if task[0] is apply and not any(map(_maybe_complex, task[2:])):
            d = {'function': dumps_function(task[1]),
                 'args': dumps(task[2])}
            if len(task) == 4:
                d['kwargs'] = dumps(task[3])
            return d
        elif not any(map(_maybe_complex, task[1:])):
            return {'function': dumps_function(task[0]),
                        'args': dumps(task[1:])}
    return {'task': dumps(task)}


def apply_function(function, args, kwargs, execution_state, key):
    """ Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    thread_state.execution_state = execution_state
    thread_state.key = key
    start = time()
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        msg = error_message(e)
    else:
        msg = {'status': 'OK',
               'result': result,
               'nbytes': sizeof(result),
               'type': dumps_function(type(result)) if result is not None else None}
    finally:
        end = time()
    msg['compute_start'] = start
    msg['compute_stop'] = end
    msg['thread'] = current_thread().ident
    return msg


def get_msg_safe_str(msg):
    """ Make a worker msg, which contains args and kwargs, safe to cast to str:
    allowing for some arguments to raise exceptions during conversion and
    ignoring them.
    """
    class Repr(object):
        def __init__(self, f, val):
            self._f = f
            self._val = val
        def __repr__(self):
            return self._f(self._val)
    msg = msg.copy()
    if "args" in msg:
        msg["args"] = Repr(convert_args_to_str, msg["args"])
    if "kwargs" in msg:
        msg["kwargs"] = Repr(convert_kwargs_to_str, msg["kwargs"])
    return msg


def convert_args_to_str(args, max_len=None):
    """ Convert args to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(args))]
    for i, arg in enumerate(args):
        try:
            sarg = repr(arg)
        except:
            sarg = "< could not convert arg to str >"
        strs[i] = sarg
        length += len(sarg) + 2
        if max_len is not None and length > max_len:
            return "({}".format(", ".join(strs[:i+1]))[:max_len]
    else:
        return "({})".format(", ".join(strs))


def convert_kwargs_to_str(kwargs, max_len=None):
    """ Convert kwargs to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(kwargs))]
    for i, (argname, arg) in enumerate(kwargs.items()):
        try:
            sarg = repr(arg)
        except:
            sarg = "< could not convert arg to str >"
        skwarg = repr(argname) + ": " + sarg
        strs[i] = skwarg
        length += len(skwarg) + 2
        if max_len is not None and length > max_len:
            return "{{{}".format(", ".join(strs[:i+1]))[:max_len]
    else:
        return "{{{}}}".format(", ".join(strs))


from .protocol import compressions, default_compression

# TODO: use protocol.maybe_compress and proper file/memoryview objects

def dumps_to_disk(x):
    b = dumps(x)
    c = compressions[default_compression]['compress'](b)
    return c

def loads_from_disk(c):
    b = compressions[default_compression]['decompress'](c)
    x = loads(b)
    return x

def weight(k, v):
    return sizeof(v)


@gen.coroutine
def run(worker, stream, function=None, args=(), kwargs={}):
    function = loads(function)
    if args:
        args = loads(args)
    if kwargs:
        kwargs = loads(kwargs)
    try:
        import inspect
        if 'dask_worker' in inspect.getargspec(function).args:
            kwargs['dask_worker'] = worker
    except:
        pass
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        logger.warn(" Run Failed\n"
            "Function: %s\n"
            "args:     %s\n"
            "kwargs:   %s\n",
            str(funcname(function))[:1000],
            convert_args_to_str(args, max_len=1000),
            convert_kwargs_to_str(kwargs, max_len=1000), exc_info=True)

        response = error_message(e)
    else:
        response = {
            'status': 'OK',
            'result': dumps(result),
        }
    raise Return(response)
