from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from datetime import timedelta
from importlib import import_module
import heapq
import logging
import os
import random
import tempfile
from threading import current_thread, Lock, local
from timeit import default_timer
import shutil
import sys

from .core import read, write, connect, close, send_recv, error_message


from dask.core import istask
from dask.compatibility import apply
try:
    from cytoolz import valmap, merge, pluck, concat
except ImportError:
    from toolz import valmap, merge, pluck, concat
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError

from .batched import BatchedSend
from .config import config
from .utils_comm import pack_data, gather_from_workers
from .compatibility import reload, unicode
from .core import (read, write, connect, close, send_recv, error_message,
                   rpc, Server, pingpong, coerce_to_address, RPCClosed)
from .metrics import time
from .protocol.pickle import dumps, loads
from .sizeof import sizeof
from .threadpoolexecutor import ThreadPoolExecutor
from .utils import (funcname, get_ip, has_arg, _maybe_complex, log_errors,
                    All, ignoring, validate_key, mp_context)

_ncores = mp_context.cpu_count()

thread_state = local()

logger = logging.getLogger(__name__)

LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)

try:
    import psutil
    TOTAL_MEMORY = psutil.virtual_memory().total
except ImportError:
    logger.warn("Please install psutil to estimate worker memory use")
    TOTAL_MEMORY = 8e9


IN_PLAY = ('waiting', 'ready', 'executing', 'long-running')
PENDING = ('waiting', 'ready', 'constrained')


class WorkerBase(Server):
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

    Parameters
    ----------
    scheduler_ip: str
    scheduler_port: int
    ip: str, optional
    ncores: int, optional
    loop: tornado.ioloop.IOLoop
    local_dir: str, optional
        Directory where we place local resources
    name: str, optional
    heartbeat_interval: int
        Milliseconds between heartbeats to scheduler
    memory_limit: int
        Number of bytes of data to keep in memory before using disk
    executor: concurrent.futures.Executor

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
                 name=None, heartbeat_interval=5000, reconnect=True,
                 memory_limit='auto', executor=None, resources=None,
                 silence_logs=None, **kwargs):
        self.ip = ip or get_ip()
        self._port = 0
        self.ncores = ncores or _ncores
        self.local_dir = local_dir or tempfile.mkdtemp(prefix='worker-')
        self.total_resources = resources or {}
        self.available_resources = (resources or {}).copy()
        if silence_logs:
            logger.setLevel(silence_logs)
        if not os.path.exists(self.local_dir):
            os.mkdir(self.local_dir)

        if memory_limit == 'auto':
            memory_limit = int(TOTAL_MEMORY * 0.6 * min(1, self.ncores / _ncores))
        with ignoring(TypeError):
            memory_limit = float(memory_limit)
        if isinstance(memory_limit, float) and memory_limit <= 1:
            memory_limit = memory_limit * TOTAL_MEMORY
        self.memory_limit = memory_limit

        if self.memory_limit:
            try:
                from zict import Buffer, File, Func
            except ImportError:
                raise ImportError("Please `pip install zict` for spill-to-disk workers")
            path = os.path.join(self.local_dir, 'storage')
            storage = Func(dumps_to_disk, loads_from_disk, File(path))
            self.data = Buffer({}, storage, int(float(self.memory_limit)), weight)
        else:
            self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self.reconnect = reconnect
        self.executor = executor or ThreadPoolExecutor(self.ncores)
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

        handlers = {
          'gather': self.gather,
          'compute-stream': self.compute_stream,
          'run': self.run,
          'run_coroutine': self.run_coroutine,
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

        super(WorkerBase, self).__init__(handlers, io_loop=self.loop, **kwargs)

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
        logger.info('-' * 49)
        logger.info('              Threads: %26d', self.ncores)
        if self.memory_limit:
            logger.info('               Memory: %23.2f GB', self.memory_limit / 1e9)
        logger.info('      Local Directory: %26s', self.local_dir)
        logger.info('-' * 49)
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
                        resources=self.total_resources,
                        **self.process_health())
                break
            except EnvironmentError:
                logger.debug("Unable to register with scheduler.  Waiting")
                yield gen.sleep(0.5)
        if resp != 'OK':
            raise ValueError(resp)
        logger.info('        Registered to: %20s:%d',
                    self.scheduler.ip, self.scheduler.port)
        logger.info('-' * 49)
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
        if self.status in ('closed', 'closing'):
            return
        logger.info("Stopping worker at %s:%d", self.ip, self.port)
        self.status = 'closing'
        self.stop()
        self.heartbeat_callback.stop()
        with ignoring(EnvironmentError):
            if report:
                yield gen.with_timeout(timedelta(seconds=timeout),
                        self.scheduler.unregister(address=(self.ip, self.port)),
                        io_loop=self.loop)
        self.scheduler.close_rpc()
        self.executor.shutdown()
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)

        for k, v in self.services.items():
            v.stop()
        self.rpc.close()
        self.status = 'closed'

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

    def _deserialize(self, function=None, args=None, kwargs=None, task=None):
        """ Deserialize task inputs and regularize to func, args, kwargs """
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


    def run(self, stream, function, args=(), kwargs={}):
        return run(self, stream, function=function, args=args, kwargs=kwargs)

    def run_coroutine(self, stream, function, args=(), kwargs={}, wait=True):
        return run(self, stream, function=function, args=args, kwargs=kwargs,
                   is_coro=True, wait=wait)

    def update_data(self, stream=None, data=None, report=True):
        self.data.update(data)
        self.nbytes.update(valmap(sizeof, data))
        if report:
            self.batched_stream.send({'op': 'add-keys',
                                      'keys': list(data)})
        info = {'nbytes': {k: sizeof(v) for k, v in data.items()},
                'status': 'OK'}
        return info

    @gen.coroutine
    def delete_data(self, stream=None, keys=None, report=True):
        if keys:
            for key in list(keys):
                deps = self.dependents.get(key, ())
                if deps and any(self.task_state[dep] in IN_PLAY
                                for dep in deps):
                    logger.info("Tried to delete necessary key: %s", key)
                    self.log.append((key, 'tried-to-delete-unneccesary-key'))
                    keys.remove(key)
                    continue
                else:
                    state = self.task_state.get(key)
                    if state == 'memory':
                        del self.data[key]
                        self.forget_key(key)
                        self.log.append((key, 'delete-memory'))
                    elif state == 'error':
                        self.forget_key(key)
                        self.log.append((key, 'delete-error'))
                    elif key in self.data:
                        del self.data[key]
                        self.log.append((key, 'delete-data'))
            logger.debug("Deleted %d keys", len(keys))
            if report:
                logger.debug("Reporting loss of keys to scheduler")
                yield self.scheduler.remove_keys(address=self.address,
                                              keys=list(keys))
        raise Return('OK')

    @gen.coroutine
    def get_data(self, stream, keys=None, who=None):
        start = time()

        msg = {k: to_serialize(self.data[k]) for k in keys if k in self.data}
        nbytes = {k: self.nbytes.get(k) for k in keys if k in self.data}
        stop = time()
        if self.digests is not None:
            self.digests['get-data-load-duration'].add(stop - start)
        start = time()
        try:
            compressed = yield write(stream, msg)
            yield close(stream)
        except EnvironmentError:
            logger.exception('failed during get data', exc_info=True)
            stream.close()
            raise
        stop = time()
        if self.digests is not None:
            self.digests['get-data-send-duration'].add(stop - start)

        total_bytes = sum(filter(None, nbytes.values()))

        self.outgoing_count += 1
        duration = (stop - start) or 0.5  # windows
        self.outgoing_transfer_log.append({
            'start': start,
            'stop': stop,
            'middle': (start + stop) / 2,
            'duration': duration,
            'who': who,
            'keys': nbytes,
            'total': total_bytes,
            'compressed': compressed,
            'bandwidth': total_bytes / duration
        })

        raise gen.Return('dont-reply')

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
                    import pkg_resources
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
            self.nbytes.update(valmap(sizeof, result))
            raise Return({'status': 'OK'})


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
    return to_serialize(task)


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
        msg['op'] = 'task-erred'
    else:
        msg = {'op': 'task-finished',
               'status': 'OK',
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


from .protocol import compressions, default_compression, to_serialize

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
def run(worker, stream, function, args=(), kwargs={}, is_coro=False, wait=True):
    assert wait or is_coro, "Combination not supported"
    function = loads(function)
    if args:
        args = loads(args)
    if kwargs:
        kwargs = loads(kwargs)
    if has_arg(function, 'dask_worker'):
        kwargs['dask_worker'] = worker
    logger.info("Run out-of-band function %r", funcname(function))
    try:
        result = function(*args, **kwargs)
        if is_coro:
            result = (yield result) if wait else None
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
            'result': to_serialize(result),
        }
    raise Return(response)


class Worker(WorkerBase):
    def __init__(self, *args, **kwargs):
        self.tasks = dict()
        self.raw_tasks = dict()
        self.task_state = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.waiting_for_data = dict()
        self.who_has = dict()
        self.has_what = defaultdict(set)
        self.pending_data_per_worker = defaultdict(deque)

        self.data_needed = deque()  # TODO: replace with heap?

        self.in_flight = dict()
        self.total_connections = 50
        self.connections = {}

        self.nbytes = dict()
        self.types = dict()
        self.priorities = dict()
        self.priority_counter = 0
        self.durations = dict()
        self.response = defaultdict(dict)
        self.host_restrictions = dict()
        self.worker_restrictions = dict()
        self.resource_restrictions = dict()

        self.ready = list()
        self.constrained = deque()
        self.executing = set()
        self.executed_count = 0
        self.long_running = set()

        self.batched_stream = None
        self.target_message_size = 200e6  # 200 MB

        self.log = deque(maxlen=100000)
        self.validate = kwargs.pop('validate', False)

        self._transitions = {
                ('waiting', 'ready'): self.transition_waiting_ready,
                ('waiting', 'memory'): self.transition_waiting_memory,
                ('ready', 'executing'): self.transition_ready_executing,
                ('ready', 'memory'): self.transition_ready_memory,
                ('constrained', 'executing'): self.transition_constrained_executing,
                ('executing', 'memory'): self.transition_executing_done,
                ('executing', 'error'): self.transition_executing_done,
                ('executing', 'long-running'): self.transition_executing_long_running,
                ('long-running', 'error'): self.transition_executing_done,
                ('long-running', 'memory'): self.transition_executing_done,
        }

        self.incoming_transfer_log = deque(maxlen=(100000))
        self.incoming_count = 0
        self.outgoing_transfer_log = deque(maxlen=(100000))
        self.outgoing_count = 0

        WorkerBase.__init__(self, *args, **kwargs)

    def __str__(self):
        return "<%s: %s, %s, stored: %d, running: %d/%d, ready: %d, comm: %d, waiting: %d>" % (
                self.__class__.__name__, self.address, self.status,
                len(self.data), len(self.executing), self.ncores,
                len(self.ready), len(self.in_flight),
                len(self.waiting_for_data))

    __repr__ = __str__

    ################
    # Update Graph #
    ################

    @gen.coroutine
    def compute_stream(self, stream):
        try:
            self.batched_stream = BatchedSend(interval=2, loop=self.loop)
            self.batched_stream.start(stream)

            closed = False

            while not closed:
                self.priority_counter += 1
                try:
                    msgs = yield read(stream)
                except EnvironmentError:
                    if self.reconnect:
                        break
                    else:
                        yield self._close(report=False)
                    break

                start = time()

                for msg in msgs:
                    op = msg.pop('op', None)
                    if 'key' in msg:
                        validate_key(msg['key'])
                    if op == 'close':
                        closed = True
                        break
                    elif op == 'compute-task':
                        priority = msg.pop('priority')
                        priority = [self.priority_counter] + priority
                        priority = tuple(-x for x in priority)
                        self.add_task(priority=priority, **msg)
                    elif op == 'release-task':
                        self.release_task(**msg)
                    elif op == 'delete-data':
                        self.delete_data(**msg)
                    else:
                        logger.warning("Unknown operation %s, %s", op, msg)

                self.ensure_communicating()
                self.ensure_computing()

                end = time()
                if self.digests is not None:
                    self.digests['handle-messages-duration'].add(end - start)

            yield self.batched_stream.close()
            logger.info('Close compute stream')
        except Exception as e:
            logger.exception(e)
            raise

    def add_task(self, key, function=None, args=None, kwargs=None, task=None,
            who_has=None, nbytes=None, priority=None, duration=None,
            host_restrictions=None, worker_restrictions=None,
            resource_restrictions=None, **kwargs2):
        try:
            if key in self.tasks:
                state = self.task_state[key]
                if state in ('memory', 'error'):
                    if state == 'memory':
                        assert key in self.data
                    logger.debug("Asked to compute prexisting result: %s: %s" ,
                                 key, state)
                    self.batched_stream.send(self.response[key])
                    return
                if state in IN_PLAY:
                    return

            if key in self.data:
                self.response[key] = {'op': 'task-finished',
                                      'status': 'OK',
                                      'key': key,
                                      'nbytes': self.nbytes[key],
                                      'type': dumps_function(type(self.data[key]))}
                self.batched_stream.send(self.response[key])
                self.task_state[key] = 'memory'
                self.tasks[key] = None
                self.raw_tasks[key] = None
                self.log.append((key, 'new-task-already-in-memory'))
                return

            self.log.append((key, 'new'))
            try:
                self.tasks[key] = self._deserialize(function, args, kwargs, task)
                raw = {'function': function, 'args': args, 'kwargs': kwargs,
                        'task': task}
                self.raw_tasks[key] = {k: v for k, v in raw.items() if v is not None}
            except Exception as e:
                logger.warn("Could not deserialize task", exc_info=True)
                emsg = error_message(e)
                emsg['key'] = key
                emsg['op'] = 'task-erred'
                self.batched_stream.send(emsg)
                self.log.append((key, 'deserialize-error'))
                return

            self.priorities[key] = priority
            self.durations[key] = duration
            if host_restrictions:
                self.host_restrictions[key] = set(host_restrictions)
            if worker_restrictions:
                self.worker_restrictions[key] = set(worker_restrictions)
            if resource_restrictions:
                self.resource_restrictions[key] = resource_restrictions
            self.task_state[key] = 'waiting'

            if nbytes is not None:
                self.nbytes.update(nbytes)

            if who_has:
                self.dependencies[key] = set(who_has)
                for dep in who_has:
                    if dep not in self.dependents:
                        self.dependents[dep] = set()
                    self.dependents[dep].add(key)
                who_has = {dep: v for dep, v in who_has.items() if dep not in self.data}
                self.waiting_for_data[key] = set(who_has)
            else:
                self.waiting_for_data[key] = set()
                self.dependencies[key] = set()

            if who_has:
                for dep, workers in who_has.items():
                    if dep not in self.who_has:
                        self.who_has[dep] = set(workers)
                    self.who_has[dep].update(workers)

                    for worker in workers:
                        self.has_what[worker].add(dep)
                        self.pending_data_per_worker[worker].append(dep)

                self.data_needed.append(key)
            else:
                self.transition(key, 'ready')
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def release_task(self, key=None):
        if self.task_state.get(key) in PENDING:
            self.rescind_key(key)

    ###############
    # Transitions #
    ###############

    def transition(self, key, finish, **kwargs):
        start = self.task_state[key]
        if start == finish:
            return
        func = self._transitions[start, finish]
        state = func(key, **kwargs)
        self.log.append((key, start, state or finish))
        self.task_state[key] = state or finish
        if self.validate:
            self.validate_key(key)

    def transition_waiting_ready(self, key):
        try:
            if self.validate:
                assert self.task_state[key] == 'waiting'
                assert key in self.waiting_for_data
                assert not self.waiting_for_data[key]
                assert all(dep in self.data for dep in self.dependencies[key])
                assert key not in self.executing
                assert key not in self.ready

            del self.waiting_for_data[key]
            if key in self.resource_restrictions:
                self.constrained.append(key)
                return 'constrained'
            else:
                heapq.heappush(self.ready, (self.priorities[key], key))
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_waiting_memory(self, key):
        try:
            if self.validate:
                assert self.task_state[key] == 'waiting'
                assert key in self.waiting_for_data
                assert key not in self.executing
                assert key not in self.ready

            del self.waiting_for_data[key]
            self.response[key] = {'op': 'task-finished',
                                  'status': 'OK',
                                  'key': key,
                                  'nbytes': self.nbytes[key],
                                  'type': dumps_function(type(self.data[key]))}
            self.batched_stream.send(self.response[key])
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_ready_executing(self, key):
        try:
            if self.validate:
                assert key not in self.waiting_for_data
                # assert key not in self.data
                assert self.task_state[key] in ('ready', 'constrained')
                assert key not in self.ready
                assert all(dep in self.data for dep in self.dependencies[key])

            self.executing.add(key)
            self.loop.add_callback(self.execute, key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_ready_memory(self, key):
        self.response[key] = {'op': 'task-finished',
                              'status': 'OK',
                              'key': key,
                              'nbytes': self.nbytes[key],
                              'type': dumps_function(type(self.data[key]))}
        self.batched_stream.send(self.response[key])

    def transition_constrained_executing(self, key):
        self.transition_ready_executing(key)
        for resource, quantity in self.resource_restrictions[key].items():
            self.available_resources[resource] -= quantity

        if self.validate:
            assert all(v >= 0 for v in self.available_resources.values())

    def transition_executing_done(self, key):
        try:
            if self.validate:
                assert key in self.executing or key in self.long_running
                assert key not in self.waiting_for_data
                assert key not in self.ready

            if key in self.resource_restrictions:
                for resource, quantity in self.resource_restrictions[key].items():
                    self.available_resources[resource] += quantity

            if self.task_state[key] == 'executing':
                self.executing.remove(key)
                self.executed_count += 1
            elif self.task_state[key] == 'long-running':
                self.long_running.remove(key)
            if self.batched_stream:
                self.batched_stream.send(self.response[key])
            else:
                raise StreamClosedError()

        except EnvironmentError:
            logger.info("Stream closed")
            self._close(report=False)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_executing_long_running(self, key):
        try:
            if self.validate:
                assert key in self.executing

            self.executing.remove(key)
            self.long_running.add(key)

            self.ensure_computing()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    ##########################
    # Gather Data from Peers #
    ##########################

    def ensure_communicating(self):
        try:
            while self.data_needed and len(self.connections) < self.total_connections:
                logger.debug("Ensure communicating.  Pending: %d.  Connections: %d/%d",
                             len(self.data_needed),
                             len(self.connections),
                             self.total_connections)

                key = self.data_needed[0]

                if key not in self.tasks:
                    self.data_needed.popleft()
                    continue

                if self.task_state.get(key) != 'waiting':
                    self.log.append((key, 'communication pass'))
                    self.data_needed.popleft()
                    continue

                deps = self.dependencies[key]
                deps = [d for d in deps
                          if d not in self.data
                          and d not in self.executing
                          and d not in self.in_flight]

                for dep in deps:
                    if not self.who_has.get(dep):
                        logger.info("Can't find dependencies for key %s", key)
                        self.cancel_key(key)
                        continue

                self.log.append(('gather-dependencies', key, deps))

                while deps and len(self.connections) < self.total_connections:
                    token = object()
                    self.connections[token] = None
                    self.loop.add_callback(self.gather_dep, deps.pop(), token, cause=key)

                if not deps:
                    self.data_needed.popleft()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def put_key_in_memory(self, key, value):
        if key in self.data:
            return

        self.data[key] = value

        if key not in self.nbytes:
            self.nbytes[key] = sizeof(value)

        self.types[key] = type(value)

        for dep in self.dependents.get(key, ()):
            if dep in self.waiting_for_data:
                if key in self.waiting_for_data[dep]:
                    self.waiting_for_data[dep].remove(key)
                if not self.waiting_for_data[dep]:
                    self.transition(dep, 'ready')

        if key in self.task_state:
            self.transition(key, 'memory')

    @gen.coroutine
    def gather_dep(self, dep, slot, cause=None):
        failures = 5
        del self.connections[slot]
        try:
            if self.validate:
                self.validate_state()

            while True:
                if not self.who_has.get(dep):
                    if dep not in self.dependents:
                        return
                    failures += 1
                    result = yield self.query_who_has(dep)
                    if not result or failures > 5:
                        for key in list(self.dependents[dep]):
                            if dep in self.executing:
                                continue
                            if dep in self.waiting_for_data.get(key, ()):
                                self.cancel_key(key)
                        return
                    else:
                        assert self.who_has.get(dep)
                worker = random.choice(list(self.who_has[dep]))
                ip, port = worker.split(':')
                try:
                    start = time()
                    future = connect(ip, int(port), timeout=10)
                    self.connections[future] = True
                    stream = yield future
                    end = time()
                    if self.digests is not None:
                        self.digests['gather-connect-duration'].add(end - start)
                except (gen.TimeoutError, EnvironmentError):
                    logger.info("Failed to connect to %s", worker)
                    with ignoring(KeyError):  # other coroutine may have removed
                        for d in self.has_what.pop(worker):
                            self.who_has[d].remove(worker)
                else:
                    break
                finally:
                    del self.connections[future]

            if dep in self.data or dep in self.in_flight:  # someone beat us
                stream.close() # close newly opened stream
                return

            deps = {dep}

            total_bytes = self.nbytes[dep]
            L = self.pending_data_per_worker[worker]

            while L:
                d = L.popleft()
                if (d in self.data or
                    d in self.in_flight or
                    d in self.executing or
                    d not in self.nbytes):  # no longer tracking
                    continue
                if total_bytes + self.nbytes[d] > self.target_message_size:
                    break
                deps.add(d)
                total_bytes += self.nbytes[d]

            for d in deps:
                assert d not in self.in_flight
                self.in_flight[d] = stream
            self.log.append(('request-dep', dep, worker, deps))
            self.connections[stream] = deps

            try:
                start = time()
                logger.debug("Request %d keys and %d bytes", len(deps),
                             total_bytes)
                response = yield send_recv(stream, op='get_data', keys=list(deps),
                                           close=True, who=self.address)
                stop = time()
                deps2 = list(response)

                if cause:
                    self.response[cause].update({'transfer_start': start,
                                                 'transfer_stop': stop})

                total_bytes = sum(self.nbytes.get(dep, 0) for dep in deps2)
                duration = (stop - start) or 0.5
                self.incoming_transfer_log.append({
                    'start': start,
                    'stop': stop,
                    'middle': (start + stop) / 2.0,
                    'duration': duration,
                    'keys': {dep: self.nbytes.get(dep, None) for dep in deps2},
                    'total': total_bytes,
                    'bandwidth': total_bytes / duration,
                    'who': worker
                })
                if self.digests is not None:
                    self.digests['transfer-bandwidth'].add(total_bytes / duration)
                    self.digests['transfer-duration'].add(duration)
                self.counters['transfer-count'].add(len(deps2))
                self.incoming_count += 1
            except EnvironmentError as e:
                logger.error("Worker stream died during communication: %s",
                             worker)
                response = {}
                self.log.append(('receive-dep-failed', worker))
            finally:
                del self.connections[stream]
                stream.close()

            self.log.append(('receive-dep', worker, list(response)))

            assert len(self.connections) < self.total_connections

            for d in deps:
                del self.in_flight[d]

            for d, v in response.items():
                self.put_key_in_memory(d, v)

            if response:
                self.batched_stream.send({'op': 'add-keys',
                                          'keys': list(response)})

            for d in deps:
                if d not in response and d in self.dependents:
                    self.log.append(('missing-dep', d))
                    try:
                        self.who_has[d].remove(worker)
                    except KeyError:
                        pass
                    try:
                        self.has_what[worker].remove(d)
                    except KeyError:
                        pass
                    for key in self.dependents.get(d, ()):
                        if key in self.waiting_for_data:
                            self.data_needed.appendleft(key)

            if self.validate:
                self.validate_state()

            self.ensure_computing()
            self.ensure_communicating()
        except Exception as e:
            logger.exception(e)
            if self.batched_stream and LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    @gen.coroutine
    def query_who_has(self, *deps):
        with log_errors():
            response = yield self.scheduler.who_has(keys=deps)
            self.update_who_has(response)
            raise gen.Return(response)

    def update_who_has(self, who_has):
        try:
            for dep, workers in who_has.items():
                if dep in self.who_has:
                    self.who_has[dep].update(workers)
                else:
                    self.who_has[dep] = set(workers)

                for worker in workers:
                    self.has_what[worker].add(dep)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def cancel_key(self, key):
        try:
            self.log.append(('cancel', key))
            if key in self.waiting_for_data:
                missing = [dep for dep in self.dependencies[key]
                           if dep not in self.data
                           and not self.who_has.get(dep)]
                self.log.append(('report-missing-data', key, missing))
                self.batched_stream.send({'op': 'missing-data',
                                          'key': key,
                                          'keys': missing})
            self.forget_key(key)
        except Exception as e:
            logger.exception(e)
            raise

    def forget_key(self, key):
        try:
            self.log.append(('forget', key))
            if key in self.tasks:
                del self.tasks[key]
                del self.raw_tasks[key]
                del self.task_state[key]
            if key in self.waiting_for_data:
                del self.waiting_for_data[key]

            for dep in self.dependencies.pop(key, ()):
                self.dependents[dep].remove(key)
                if not self.dependents[dep]:
                    del self.dependents[dep]

            if key in self.who_has:
                for worker in self.who_has.pop(key):
                    self.has_what[worker].remove(key)
                    if not self.has_what[worker]:
                        del self.has_what[worker]

            if key not in self.dependents:
                if key in self.nbytes:
                    del self.nbytes[key]
                if key in self.types:
                    del self.types[key]
                if key in self.priorities:
                    del self.priorities[key]
                if key in self.durations:
                    del self.durations[key]
                if key in self.response:
                    del self.response[key]

            if key in self.host_restrictions:
                del self.host_restrictions[key]
            if key in self.worker_restrictions:
                del self.worker_restrictions[key]
            if key in self.resource_restrictions:
                del self.resource_restrictions[key]
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def rescind_key(self, key):
        try:
            if self.task_state.get(key) not in PENDING:
                return
            del self.task_state[key]
            del self.tasks[key]
            del self.raw_tasks[key]
            if key in self.waiting_for_data:
                del self.waiting_for_data[key]

            for dep in self.dependencies.pop(key, ()):
                self.dependents[dep].remove(key)
                if not self.dependents[dep]:
                    del self.dependents[dep]

            if key not in self.dependents:
                # if key in self.nbytes:
                #     del self.nbytes[key]
                if key in self.priorities:
                    del self.priorities[key]
                if key in self.durations:
                    del self.durations[key]
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    ################
    # Execute Task #
    ################

    def meets_resource_constraints(self, key):
        if key not in self.resource_restrictions:
            return True
        for resource, needed in self.resource_restrictions[key].items():
            if self.available_resources[resource] < needed:
                return False

        return True

    def ensure_computing(self):
        try:
            while self.constrained and len(self.executing) < self.ncores:
                key = self.constrained[0]
                if self.task_state.get(key) != 'constrained':
                    self.constrained.popleft()
                    continue
                if self.meets_resource_constraints(key):
                    self.constrained.popleft()
                    self.transition(key, 'executing')
                else:
                    break
            while self.ready and len(self.executing) < self.ncores:
                _, key = heapq.heappop(self.ready)
                if key not in self.task_state:
                    continue
                if self.task_state[key] in ('memory', 'error', 'executing'):
                    continue
                self.transition(key, 'executing')
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    @gen.coroutine
    def execute(self, key, report=False):
        try:
            if self.validate:
                assert key in self.executing
                assert key not in self.waiting_for_data
                assert self.task_state[key] == 'executing'

            function, args, kwargs = self.tasks[key]

            start = time()
            args2 = pack_data(args, self.data, key_types=str)
            kwargs2 = pack_data(kwargs, self.data, key_types=str)
            stop = time()
            if stop - start > 0.005:
                self.response[key]['disk_load_start'] = start
                self.response[key]['disk_load_stop'] = stop
                if self.digests is not None:
                    self.digests['disk-load-duration'].add(stop - start)

            logger.debug("Execute key: %s", key)  # TODO: comment out?
            result = yield self.executor_submit(key, apply_function, function,
                                                args2, kwargs2,
                                                self.execution_state, key)

            result['key'] = key
            value = result.pop('result', None)
            self.response[key].update(result)

            if result['op'] == 'task-finished':
                self.put_key_in_memory(key, value)
                self.transition(key, 'memory')
                if self.digests is not None:
                    self.digests['task-duration'].add(result['compute_stop'] -
                                                      result['compute_start'])
            else:
                logger.warn(" Compute Failed\n"
                    "Function: %s\n"
                    "args:     %s\n"
                    "kwargs:   %s\n",
                    str(funcname(function))[:1000],
                    convert_args_to_str(args2, max_len=1000),
                    convert_kwargs_to_str(kwargs2, max_len=1000), exc_info=True)
                self.transition(key, 'error')

            logger.debug("Send compute response to scheduler: %s, %s", key,
                         self.response[key])

            if self.validate:
                assert key not in self.executing
                assert key not in self.waiting_for_data

            self.ensure_computing()
            self.ensure_communicating()
        except RuntimeError as e:
            logger.error("Thread Pool Executor error: %s", e)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    ##################
    # Administrative #
    ##################

    def validate_key_memory(self, key):
        assert key in self.data
        assert key in self.nbytes
        assert key not in self.waiting_for_data
        assert key not in self.executing
        assert key not in self.ready

    def validate_key_executing(self, key):
        assert key in self.executing
        assert key not in self.data
        assert key not in self.waiting_for_data
        assert all(dep in self.data for dep in self.dependencies[key])

    def validate_key_ready(self, key):
        assert key in pluck(1, self.ready)
        assert key not in self.data
        assert key not in self.executing
        assert key not in self.waiting_for_data
        assert all(dep in self.data for dep in self.dependencies[key])

    def validate_key_waiting(self, key):
        assert key not in self.data
        assert not all(dep in self.data for dep in self.dependencies[key])

    def validate_key(self, key):
        try:
            state = self.task_state[key]
            if state == 'memory':
                self.validate_key_memory(key)
            elif state == 'waiting':
                self.validate_key_waiting(key)
            elif state == 'ready':
                self.validate_key_ready(key)
            elif state == 'executing':
                self.validate_key_executing(key)
        except Exception as e:
            logger.exception(e)
            import pdb; pdb.set_trace()
            raise

    def validate_state(self):
        try:
            for key, workers in self.who_has.items():
                for w in workers:
                    assert key in self.has_what[w]

            for worker, keys in self.has_what.items():
                for k in keys:
                    assert worker in self.who_has[k]

            for key, state in self.task_state.items():
                if state == 'memory':
                    assert key in self.data
                    assert isinstance(self.nbytes[key], int)
                if state == 'error':
                    assert key not in self.data
                if state == 'waiting':
                    assert key in self.waiting_for_data
                    s = self.waiting_for_data[key]
                    for dep in self.dependencies[key]:
                        assert (dep in s or
                                dep in self.in_flight or
                                dep in self.executing or
                                dep in self.data)
                if state == 'ready':
                    assert key in pluck(1, self.ready)
                if state == 'executing':
                    assert key in self.executing
                if state == 'long-running':
                    assert key not in self.executing
                    assert key in self.long_running

            for key in self.tasks:
                if self.task_state[key] == 'memory':
                    assert isinstance(self.nbytes[key], int)
                    assert key not in self.waiting_for_data
                    assert key in self.data

        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def stateof(self, key):
        return {'executing': key in self.executing,
                'waiting_for_data': key in self.waiting_for_data,
                'heap': key in pluck(1, self.ready),
                'data': key in self.data}

    def story(self, *keys):
        return [msg for msg in self.log
                    if any(key in msg for key in keys)
                    or any(key in c
                           for key in keys
                           for c in msg
                           if isinstance(c, (tuple, list, set)))]


def is_valid_worker(worker_restrictions=None, host_restrictions=None,
        resource_restrictions=None, resources=None, worker=None):
    """
    Can this worker run on this machine given known scheduling restrictions?

    Examples
    --------
    >>> is_valid_worker(worker_restrictions={'alice:8000', 'bob:8000'},
    ...                 worker='alice:8000')
    True

    >>> is_valid_worker(host_restrictions={'alice', 'bob'},
    ...                 worker='alice:8000')
    True

    >>> is_valid_worker(resource_restrictions={'GPU': 1, 'MEM': 8e9},
    ...                 resources={'GPU': 2, 'MEM':4e9})
    False

    >>> is_valid_worker(host_restrictions={'alice', 'bob'},
    ...                 resource_restrictions={'GPU': 1, 'MEM': 8e9},
    ...                 resources={'GPU': 2, 'MEM': 10e9},
    ...                 worker='charlie:8000')
    False
    """
    if worker_restrictions is not None:
        if worker not in worker_restrictions:
            return False

    if host_restrictions is not None:
        host = worker.split(':')[0]
        if host not in host_restrictions:
            return False

    if resource_restrictions is not None:
        for resource, quantity in resource_restrictions.items():
            if resources.get(resource, 0) < quantity:
                return False

    return True
