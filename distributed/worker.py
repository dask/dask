from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from datetime import timedelta
import heapq
import logging
import os
from pickle import PicklingError
import random
import tempfile
from threading import current_thread, local
import shutil
import sys

from dask.core import istask
from dask.compatibility import apply
try:
    from cytoolz import pluck
except ImportError:
    from toolz import pluck
from tornado.gen import Return
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.locks import Event

from .batched import BatchedSend
from .comm import get_address_host, get_local_address_for
from .config import config
from .compatibility import unicode
from .core import (error_message, CommClosedError,
                   rpc, pingpong, coerce_to_address)
from .metrics import time
from .node import ServerNode
from .preloading import preload_modules
from .protocol import (pickle, to_serialize, deserialize_bytes,
                       serialize_bytelist)
from .security import Security
from .sizeof import safe_sizeof as sizeof
from .threadpoolexecutor import ThreadPoolExecutor
from .utils import (funcname, get_ip, has_arg, _maybe_complex, log_errors,
                    ignoring, validate_key, mp_context, import_file,
                    silence_logging)
from .utils_comm import pack_data, gather_from_workers

_ncores = mp_context.cpu_count()

thread_state = local()

logger = logging.getLogger(__name__)

LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)

no_value = '--no-value-sentinel--'

try:
    import psutil
    TOTAL_MEMORY = psutil.virtual_memory().total
except ImportError:
    logger.warning("Please install psutil to estimate worker memory use")
    TOTAL_MEMORY = 8e9
    psutil = None


IN_PLAY = ('waiting', 'ready', 'executing', 'long-running')
PENDING = ('waiting', 'ready', 'constrained')
PROCESSING = ('waiting', 'ready', 'constrained', 'executing', 'long-running')
READY = ('ready', 'constrained')


class WorkerBase(ServerNode):
    def __init__(self, scheduler_ip, scheduler_port=None, ncores=None,
                 loop=None, local_dir=None, services=None, service_ports=None,
                 name=None, heartbeat_interval=5000, reconnect=True,
                 memory_limit='auto', executor=None, resources=None,
                 silence_logs=None, death_timeout=None, preload=(),
                 security=None, **kwargs):
        if scheduler_port is None:
            scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
        self._port = 0
        self.ncores = ncores or _ncores
        self.total_resources = resources or {}
        self.available_resources = (resources or {}).copy()
        self.death_timeout = death_timeout
        self.preload = preload
        if silence_logs:
            silence_logging(level=silence_logs)

        if local_dir:
            local_dir = os.path.abspath(local_dir)
            if not os.path.exists(local_dir):
                os.mkdir(local_dir)
        self.local_dir = tempfile.mkdtemp(prefix='worker-', dir=local_dir)

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args('worker')
        self.listen_args = self.security.get_listen_args('worker')

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
            storage = Func(serialize_bytelist, deserialize_bytes, File(path))
            self.data = Buffer({}, storage, int(float(self.memory_limit)), weight)
        else:
            self.data = dict()
        self.loop = loop or IOLoop.current()
        self.status = None
        self._closed = Event()
        self.reconnect = reconnect
        self.executor = executor or ThreadPoolExecutor(self.ncores)
        self.scheduler = rpc(scheduler_addr, connection_args=self.connection_args)
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
        self.service_specs = services or {}

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

        super(WorkerBase, self).__init__(handlers, io_loop=self.loop,
                                         connection_args=self.connection_args,
                                         **kwargs)

        self.heartbeat_callback = PeriodicCallback(self.heartbeat,
                                                   self.heartbeat_interval,
                                                   io_loop=self.loop)

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
                if psutil:
                    memory_info = psutil.Process().memory_info()
                    kwargs = {'memory': memory_info.vms,
                              'memory-vms': memory_info.vms,
                              'memory-rss': memory_info.rss}
                else:
                    kwargs = {}

                yield self.scheduler.register(
                        address=self.address,
                        name=self.name,
                        ncores=self.ncores,
                        now=time(),
                        host_info=self.host_health(),
                        services=self.service_ports,
                        memory_limit=self.memory_limit,
                        executing=len(self.executing),
                        in_memory=len(self.data),
                        ready=len(self.ready),
                        in_flight=len(self.in_flight_tasks),
                        **kwargs)
            finally:
                self.heartbeat_active = False
        else:
            logger.debug("Heartbeat skipped: channel busy")

    @gen.coroutine
    def _register_with_scheduler(self):
        self.heartbeat_callback.stop()
        start = time()
        while True:
            if self.death_timeout and time() > start + self.death_timeout:
                yield self._close(timeout=1)
                return
            if self.status in ('closed', 'closing'):
                raise gen.Return
            try:
                future = self.scheduler.register(
                        ncores=self.ncores, address=self.address,
                        keys=list(self.data),
                        name=self.name,
                        nbytes=self.nbytes,
                        now=time(),
                        host_info=self.host_health(),
                        services=self.service_ports,
                        memory_limit=self.memory_limit,
                        local_directory=self.local_dir,
                        resources=self.total_resources)
                if self.death_timeout:
                    diff = self.death_timeout - (time() - start)
                    future = gen.with_timeout(timedelta(seconds=diff), future,
                                              io_loop=self.loop)
                resp = yield future
                self.status = 'running'
                break
            except EnvironmentError:
                logger.info("Trying to connect to scheduler: %s" %
                            str(self.scheduler.address))
                yield gen.sleep(0.1)
            except gen.TimeoutError:
                pass
        if resp != 'OK':
            raise ValueError(resp)
        self.heartbeat_callback.start()

    def start_services(self, listen_ip=''):
        for k, v in self.service_specs.items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            if isinstance(v, tuple):
                v, kwargs = v
            else:
                v, kwargs = v, {}

            self.services[k] = v(self, io_loop=self.loop, **kwargs)
            self.services[k].listen((listen_ip, port))
            self.service_ports[k] = self.services[k].port

    @gen.coroutine
    def _start(self, addr_or_port=0):
        assert self.status is None

        # XXX Factor this out
        if not addr_or_port:
            # Default address is the required one to reach the scheduler
            self.listen(get_local_address_for(self.scheduler.address),
                        listen_args=self.listen_args)
            self.ip = get_address_host(self.address)
        elif isinstance(addr_or_port, int):
            # addr_or_port is an integer => assume TCP
            self.ip = get_ip(
                get_address_host(self.scheduler.address)
            )
            self.listen((self.ip, addr_or_port),
                        listen_args=self.listen_args)
        else:
            self.listen(addr_or_port, listen_args=self.listen_args)
            self.ip = get_address_host(self.address)

        self.name = self.name or self.address
        preload_modules(self.preload, parameter=self, file_dir=self.local_dir)
        # Services listen on all addresses
        # Note Nanny is not a "real" service, just some metadata
        # passed in service_ports...
        self.start_services()

        logger.info('      Start worker at: %26s', self.address)
        for k, v in self.service_ports.items():
            logger.info('  %16s at: %20s:%d' % (k, self.ip, v))
        logger.info('Waiting to connect to: %26s',
                    self.scheduler.address)
        logger.info('-' * 49)
        logger.info('              Threads: %26d', self.ncores)
        if self.memory_limit:
            logger.info('               Memory: %23.2f GB', self.memory_limit / 1e9)
        logger.info('      Local Directory: %26s', self.local_dir)
        logger.info('-' * 49)

        yield self._register_with_scheduler()

        if self.status == 'running':
            logger.info('        Registered to: %32s', self.scheduler.address)
            logger.info('-' * 49)

    def start(self, port=0):
        self.loop.add_callback(self._start, port)

    def identity(self, comm):
        return {'type': type(self).__name__,
                'id': self.id,
                'scheduler': self.scheduler.address,
                'ncores': self.ncores,
                'memory_limit': self.memory_limit}

    @gen.coroutine
    def _close(self, report=True, timeout=10, nanny=True):
        if self.status in ('closed', 'closing'):
            return
        logger.info("Stopping worker at %s", self.address)
        self.status = 'closing'
        self.stop()
        self.heartbeat_callback.stop()
        with ignoring(EnvironmentError, gen.TimeoutError):
            if report:
                yield gen.with_timeout(timedelta(seconds=timeout),
                        self.scheduler.unregister(address=self.address),
                        io_loop=self.loop)
        self.scheduler.close_rpc()
        self.executor.shutdown()
        if os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)

        for k, v in self.services.items():
            v.stop()

        self.status = 'closed'

        if nanny and 'nanny' in self.services:
            with self.rpc(self.services['nanny']) as r:
                yield r.terminate()

        self.rpc.close()
        self._closed.set()

    @gen.coroutine
    def terminate(self, comm, report=True):
        yield self._close(report=report)
        raise Return('OK')

    @gen.coroutine
    def wait_until_closed(self):
        yield self._closed.wait()
        assert self.status == 'closed'

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

        result = future.result()

        # logger.info("Finish job %d, %s", i, key)
        raise gen.Return(result)

    def run(self, comm, function, args=(), kwargs={}):
        return run(self, comm, function=function, args=args, kwargs=kwargs)

    def run_coroutine(self, comm, function, args=(), kwargs={}, wait=True):
        return run(self, comm, function=function, args=args, kwargs=kwargs,
                   is_coro=True, wait=wait)

    def update_data(self, comm=None, data=None, report=True):
        for key, value in data.items():
            if key in self.task_state:
                self.transition(key, 'memory', value=value)
            else:
                self.put_key_in_memory(key, value)
                self.task_state[key] = 'memory'
                self.tasks[key] = None
                self.priorities[key] = None
                self.durations[key] = None
                self.dependencies[key] = set()

            if key in self.dep_state:
                self.transition_dep(key, 'memory', value=value)

            self.log.append((key, 'receive-from-scatter'))

        if report:
            self.batched_stream.send({'op': 'add-keys',
                                      'keys': list(data)})
        info = {'nbytes': {k: sizeof(v) for k, v in data.items()},
                'status': 'OK'}
        return info

    @gen.coroutine
    def delete_data(self, comm=None, keys=None, report=True):
        if keys:
            for key in list(keys):
                self.log.append((key, 'delete'))
                if key in self.task_state:
                    self.release_key(key)

                if key in self.dep_state:
                    self.release_dep(key)

            logger.debug("Deleted %d keys", len(keys))
            if report:
                logger.debug("Reporting loss of keys to scheduler")
                # TODO: this route seems to not exist?
                yield self.scheduler.remove_keys(address=self.address,
                                                 keys=list(keys))
        raise Return('OK')

    @gen.coroutine
    def get_data(self, comm, keys=None, who=None):
        start = time()

        msg = {k: to_serialize(self.data[k]) for k in keys if k in self.data}
        nbytes = {k: self.nbytes.get(k) for k in keys if k in self.data}
        stop = time()
        if self.digests is not None:
            self.digests['get-data-load-duration'].add(stop - start)
        start = time()
        try:
            compressed = yield comm.write(msg)
        except EnvironmentError:
            logger.exception('failed during get data', exc_info=True)
            comm.abort()
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

    @gen.coroutine
    def set_resources(self, **resources):
        for r, quantity in resources.items():
            if r in self.total_resources:
                self.available_resources[r] += quantity - self.total_resources[r]
            else:
                self.available_resources[r] = quantity
            self.total_resources[r] = quantity

        yield self.scheduler.set_resources(resources=self.total_resources,
                                           worker=self.address)

    def start_ipython(self, comm):
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

    def upload_file(self, comm, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_dir, filename)
        if isinstance(data, unicode):
            data = data.encode()
        with open(out_filename, 'wb') as f:
            f.write(data)
            f.flush()

        if load:
            try:
                import_file(out_filename)
            except Exception as e:
                logger.exception(e)
                return {'status': 'error', 'exception': pickle.dumps(e)}
        return {'status': 'OK', 'nbytes': len(data)}

    def host_health(self, comm=None):
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

    def keys(self, comm=None):
        return list(self.data)

    @gen.coroutine
    def gather(self, comm=None, who_has=None):
        who_has = {k: [coerce_to_address(addr) for addr in v]
                    for k, v in who_has.items()
                    if k not in self.data}
        result, missing_keys, missing_workers = yield gather_from_workers(
                who_has, rpc=self.rpc)
        if missing_keys:
            logger.warning("Could not find data: %s on workers: %s (who_has: %s)",
                           missing_keys, missing_workers, who_has)
            raise Return({'status': 'missing-data',
                          'keys': missing_keys})
        else:
            self.update_data(data=result, report=False)
            raise Return({'status': 'OK'})


job_counter = [0]


def _deserialize(function=None, args=None, kwargs=None, task=None):
    """ Deserialize task inputs and regularize to func, args, kwargs """
    if function is not None:
        function = pickle.loads(function)
    if args:
        args = pickle.loads(args)
    if kwargs:
        kwargs = pickle.loads(kwargs)

    if task is not None:
        assert not function and not args and not kwargs
        function = execute_task
        args = (task,)

    return function, args or (), kwargs or {}


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
        b = pickle.dumps(func)
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
                 'args': pickle.dumps(task[2])}
            if len(task) == 4:
                d['kwargs'] = pickle.dumps(task[3])
            return d
        elif not any(map(_maybe_complex, task[1:])):
            return {'function': dumps_function(task[0]),
                        'args': pickle.dumps(task[1:])}
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
               'type': type(result) if result is not None else None}
    finally:
        end = time()
    msg['start'] = start
    msg['stop'] = end
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
            return "({}".format(", ".join(strs[:i + 1]))[:max_len]
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
            return "{{{}".format(", ".join(strs[:i + 1]))[:max_len]
    else:
        return "{{{}}}".format(", ".join(strs))


def weight(k, v):
    return sizeof(v)


@gen.coroutine
def run(server, comm, function, args=(), kwargs={}, is_coro=False, wait=True):
    assert wait or is_coro, "Combination not supported"
    function = pickle.loads(function)
    if args:
        args = pickle.loads(args)
    if kwargs:
        kwargs = pickle.loads(kwargs)
    if has_arg(function, 'dask_worker'):
        kwargs['dask_worker'] = server
    if has_arg(function, 'dask_scheduler'):
        kwargs['dask_scheduler'] = server
    logger.info("Run out-of-band function %r", funcname(function))
    try:
        result = function(*args, **kwargs)
        if is_coro:
            result = (yield result) if wait else None
    except Exception as e:
        logger.warning(" Run Failed\n"
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
    """ Worker node in a Dask distributed cluster

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Workers keep the scheduler informed of their data and use that scheduler to
    gather data from other workers when necessary to perform a computation.

    You can start a worker with the ``dask-worker`` command line application::

        $ dask-worker scheduler-ip:port

    Use the ``--help`` flag to see more options

        $ dask-worker --help

    The rest of this docstring is about the internal state the the worker uses
    to manage and track internal computations.

    **State**

    **Informational State**

    These attributes don't change significantly during execution.

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
    * **total_connections**: ``int``
        The maximum number of concurrent connections we want to see
    * **total_comm_nbytes**: ``int``
    * **batched_stream**: ``BatchedSend``
        A batched stream along which we communicate to the scheduler
    * **log**: ``[(message)]``
        A structured and queryable log.  See ``Worker.story``

    **Volatile State**

    This attributes track the progress of tasks that this worker is trying to
    complete.  In the descriptions below a ``key`` is the name of a task that
    we want to compute and ``dep`` is the name of a piece of dependent data
    that we want to collect from others.

    * **data:** ``{key: object}``:
        Dictionary mapping keys to actual values
    * **task_state**: ``{key: string}``:
        The state of all tasks that the scheduler has asked us to compute.
        Valid states include waiting, constrained, exeucuting, memory, erred
    * **tasks**: ``{key: dict}``
        The function, args, kwargs of a task.  We run this when appropriate
    * **dependencies**: ``{key: {deps}}``
        The data needed by this key to run
    * **dependents**: ``{dep: {keys}}``
        The keys that use this dependency
    * **data_needed**: deque(keys)
        The keys whose data we still lack, arranged in a deque
    * **waiting_for_data**: ``{kep: {deps}}``
        A dynamic verion of dependencies.  All dependencies that we still don't
        have for a particular key.
    * **ready**: [keys]
        Keys that are ready to run.  Stored in a LIFO stack
    * **constrained**: [keys]
        Keys for which we have the data to run, but are waiting on abstract
        resources like GPUs.  Stored in a FIFO deque
    * **executing**: {keys}
        Keys that are currently executing
    * **executed_count**: int
        A number of tasks that this worker has run in its lifetime
    * **long_running**: {keys}
        A set of keys of tasks that are running and have started their own
        long-running clients.

    * **dep_state**: ``{dep: string}``:
        The state of all dependencies required by our tasks
        Valid states include waiting, flight, and memory
    * **who_has**: ``{dep: {worker}}``
        Workers that we believe have this data
    * **has_what**: ``{worker: {deps}}``
        The data that we care about that we think a worker has
    * **pending_data_per_worker**: ``{worker: [dep]}``
        The data on each worker that we still want, prioritized as a deque
    * **in_flight_tasks**: ``{task: worker}``
        All dependencies that are coming to us in current peer-to-peer
        connections and the workers from which they are coming.
    * **in_flight_workers**: ``{worker: {task}}``
        The workers from which we are currently gathering data and the
        dependencies we expect from those connections
    * **comm_bytes**: ``int``
        The total number of bytes in flight
    * **suspicious_deps**: ``{dep: int}``
        The number of times a dependency has not been where we expected it

    * **nbytes**: ``{key: int}``
        The size of a particular piece of data
    * **types**: ``{key: type}``
        The type of a particular piece of data
    * **threads**: ``{key: int}``
        The ID of the thread on which the task ran
    * **exceptions**: ``{key: exception}``
        The exception caused by running a task if it erred
    * **tracebacks**: ``{key: traceback}``
        The exception caused by running a task if it erred
    * **startstops**: ``{key: [(str, float, float)]}``
        Log of transfer, load, and compute times for a task

    * **priorities**: ``{key: tuple}``
        The priority of a key given by the scheduler.  Determines run order.
    * **durations**: ``{key: float}``
        Expected duration of a task
    * **resource_restrictions**: ``{key: {str: number}}``
        Abstract resources required to run a task

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
    resources: dict
        Resources that thiw worker has like ``{'GPU': 2}``

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
    def __init__(self, *args, **kwargs):
        self.tasks = dict()
        self.task_state = dict()
        self.dep_state = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.waiting_for_data = dict()
        self.who_has = dict()
        self.has_what = defaultdict(set)
        self.pending_data_per_worker = defaultdict(deque)
        self.extensions = {}

        self.data_needed = deque()  # TODO: replace with heap?

        self.in_flight_tasks = dict()
        self.in_flight_workers = dict()
        self.total_connections = 50
        self.total_comm_nbytes = 10e6
        self.comm_nbytes = 0
        self.suspicious_deps = defaultdict(lambda: 0)
        self._missing_dep_flight = set()

        self.nbytes = dict()
        self.types = dict()
        self.threads = dict()
        self.exceptions = dict()
        self.tracebacks = dict()

        self.priorities = dict()
        self.priority_counter = 0
        self.durations = dict()
        self.startstops = defaultdict(list)
        self.resource_restrictions = dict()

        self.ready = list()
        self.constrained = deque()
        self.executing = set()
        self.executed_count = 0
        self.long_running = set()

        self.batched_stream = None
        self.recent_messages_log = deque(maxlen=10000)
        self.target_message_size = 50e6  # 50 MB

        self.log = deque(maxlen=100000)
        self.validate = kwargs.pop('validate', False)

        self._transitions = {
                ('waiting', 'ready'): self.transition_waiting_ready,
                ('waiting', 'memory'): self.transition_waiting_done,
                ('waiting', 'error'): self.transition_waiting_done,
                ('ready', 'executing'): self.transition_ready_executing,
                ('ready', 'memory'): self.transition_ready_memory,
                ('constrained', 'executing'): self.transition_constrained_executing,
                ('executing', 'memory'): self.transition_executing_done,
                ('executing', 'error'): self.transition_executing_done,
                ('executing', 'long-running'): self.transition_executing_long_running,
                ('long-running', 'error'): self.transition_executing_done,
                ('long-running', 'memory'): self.transition_executing_done,
        }

        self._dep_transitions = {
                ('waiting', 'flight'): self.transition_dep_waiting_flight,
                ('waiting', 'memory'): self.transition_dep_waiting_memory,
                ('flight', 'waiting'): self.transition_dep_flight_waiting,
                ('flight', 'memory'): self.transition_dep_flight_memory,
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
                len(self.ready), len(self.in_flight_tasks),
                len(self.waiting_for_data))

    __repr__ = __str__

    ################
    # Update Graph #
    ################

    @gen.coroutine
    def compute_stream(self, comm):
        try:
            self.batched_stream = BatchedSend(interval=2, loop=self.loop)
            self.batched_stream.start(comm)

            def on_closed():
                if self.reconnect and self.status not in ('closed', 'closing'):
                    logger.info("Connection to scheduler broken. Reregistering")
                    self._register_with_scheduler()
                else:
                    self._close(report=False)

            closed = False

            while not closed:
                try:
                    msgs = yield comm.read()
                except CommClosedError:
                    on_closed()
                    break
                except EnvironmentError as e:
                    break

                start = time()

                for msg in msgs:
                    self.recent_messages_log.append(msg)
                    op = msg.pop('op', None)
                    if 'key' in msg:
                        validate_key(msg['key'])
                    if op == 'close':
                        closed = True
                        self._close()
                        break
                    elif op == 'compute-task':
                        self.add_task(**msg)
                    elif op == 'release-task':
                        self.log.append((msg['key'], 'release-task'))
                        self.release_key(report=False, **msg)
                    elif op == 'delete-data':
                        self.delete_data(**msg)
                    else:
                        logger.warning("Unknown operation %s, %s", op, msg)

                self.priority_counter -= 1

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
            resource_restrictions=None, **kwargs2):
        if isinstance(priority, list):
            priority.insert(1, self.priority_counter)
        try:
            if key in self.tasks:
                state = self.task_state[key]
                if state in ('memory', 'error'):
                    if state == 'memory':
                        assert key in self.data
                    logger.debug("Asked to compute pre-existing result: %s: %s" ,
                                 key, state)
                    self.send_task_state_to_scheduler(key)
                    return
                if state in IN_PLAY:
                    return

            if self.dep_state.get(key) == 'memory':
                self.task_state[key] = 'memory'
                self.send_task_state_to_scheduler(key)
                self.tasks[key] = None
                self.log.append((key, 'new-task-already-in-memory'))
                self.priorities[key] = priority
                self.durations[key] = duration
                return

            self.log.append((key, 'new'))
            try:
                start = time()
                self.tasks[key] = _deserialize(function, args, kwargs, task)
                stop = time()

                if stop - start > 0.010:
                    self.startstops[key].append(('deserialize', start, stop))
            except Exception as e:
                logger.warning("Could not deserialize task", exc_info=True)
                emsg = error_message(e)
                emsg['key'] = key
                emsg['op'] = 'task-erred'
                self.batched_stream.send(emsg)
                self.log.append((key, 'deserialize-error'))
                return

            self.priorities[key] = priority
            self.durations[key] = duration
            if resource_restrictions:
                self.resource_restrictions[key] = resource_restrictions
            self.task_state[key] = 'waiting'

            if nbytes is not None:
                self.nbytes.update(nbytes)

            who_has = who_has or {}
            self.dependencies[key] = set(who_has)
            self.waiting_for_data[key] = set()

            for dep in who_has:
                if dep not in self.dependents:
                    self.dependents[dep] = set()
                self.dependents[dep].add(key)

                if dep not in self.dep_state:
                    if self.task_state.get(dep) == 'memory':
                        self.dep_state[dep] = 'memory'
                    else:
                        self.dep_state[dep] = 'waiting'

                if self.dep_state[dep] != 'memory':
                    self.waiting_for_data[key].add(dep)

            for dep, workers in who_has.items():
                assert workers
                if dep not in self.who_has:
                    self.who_has[dep] = set(workers)
                self.who_has[dep].update(workers)

                for worker in workers:
                    self.has_what[worker].add(dep)
                    if self.dep_state[dep] != 'memory':
                        self.pending_data_per_worker[worker].append(dep)

            if self.waiting_for_data[key]:
                self.data_needed.append(key)
            else:
                self.transition(key, 'ready')
            if self.validate:
                if who_has:
                    assert all(dep in self.dep_state for dep in who_has)
                    assert all(dep in self.nbytes for dep in who_has)
                    for dep in who_has:
                        self.validate_dep(dep)
                    self.validate_key(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    ###############
    # Transitions #
    ###############

    def transition_dep(self, dep, finish, **kwargs):
        try:
            start = self.dep_state[dep]
        except KeyError:
            return
        if start == finish:
            return
        func = self._dep_transitions[start, finish]
        state = func(dep, **kwargs)
        self.log.append(('dep', dep, start, state or finish))
        if dep in self.dep_state:
            self.dep_state[dep] = state or finish
            if self.validate:
                self.validate_dep(dep)

    def transition_dep_waiting_flight(self, dep, worker=None):
        try:
            if self.validate:
                assert dep not in self.in_flight_tasks
                assert self.dependents[dep]

            self.in_flight_tasks[dep] = worker
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_dep_flight_waiting(self, dep, worker=None):
        try:
            if self.validate:
                assert dep in self.in_flight_tasks

            del self.in_flight_tasks[dep]
            try:
                self.who_has[dep].remove(worker)
            except KeyError:
                pass
            try:
                self.has_what[worker].remove(dep)
            except KeyError:
                pass

            if not self.who_has.get(dep):
                if dep not in self._missing_dep_flight:
                    self._missing_dep_flight.add(dep)
                    self.loop.add_callback(self.handle_missing_dep, dep)
            for key in self.dependents.get(dep, ()):
                if self.task_state[key] == 'waiting':
                    self.data_needed.appendleft(key)

            if not self.dependents[dep]:
                self.release_dep(dep)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_dep_flight_memory(self, dep, value=None):
        try:
            if self.validate:
                assert dep in self.in_flight_tasks

            del self.in_flight_tasks[dep]
            self.dep_state[dep] = 'memory'
            self.put_key_in_memory(dep, value)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_dep_waiting_memory(self, dep, value=None):
        try:
            if self.validate:
                try:
                    assert dep in self.data
                    assert dep in self.nbytes
                    assert dep in self.types
                    assert self.task_state[dep] == 'memory'
                except Exception as e:
                    logger.exception(e)
                    import pdb; pdb.set_trace()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

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

    def transition_waiting_done(self, key, value=None):
        try:
            if self.validate:
                assert self.task_state[key] == 'waiting'
                assert key in self.waiting_for_data
                assert key not in self.executing
                assert key not in self.ready

            del self.waiting_for_data[key]
            self.send_task_state_to_scheduler(key)
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
                assert self.task_state[key] in READY
                assert key not in self.ready
                assert all(dep in self.data for dep in self.dependencies[key])

            self.executing.add(key)
            self.loop.add_callback(self.execute, key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_ready_memory(self, key, value=None):
        self.send_task_state_to_scheduler(key)

    def transition_constrained_executing(self, key):
        self.transition_ready_executing(key)
        for resource, quantity in self.resource_restrictions[key].items():
            self.available_resources[resource] -= quantity

        if self.validate:
            assert all(v >= 0 for v in self.available_resources.values())

    def transition_executing_done(self, key, value=no_value):
        try:
            if self.validate:
                assert key in self.executing or key in self.long_running
                assert key not in self.waiting_for_data
                assert key not in self.ready

            out = None
            if key in self.resource_restrictions:
                for resource, quantity in self.resource_restrictions[key].items():
                    self.available_resources[resource] += quantity

            if self.task_state[key] == 'executing':
                self.executing.remove(key)
                self.executed_count += 1
            elif self.task_state[key] == 'long-running':
                self.long_running.remove(key)

            if value is not no_value:
                try:
                    self.task_state[key] = 'memory'
                    self.put_key_in_memory(key, value, transition=False)
                except Exception as e:
                    logger.info("Failed to put key in memory", exc_info=True)
                    msg = error_message(e)
                    self.exceptions[key] = msg['exception']
                    self.tracebacks[key] = msg['traceback']
                    self.task_state[key] = 'error'
                    out = 'error'

                if key in self.dep_state:
                    self.transition_dep(key, 'memory')

            if self.batched_stream:
                self.send_task_state_to_scheduler(key)
            else:
                raise CommClosedError

            return out

        except EnvironmentError:
            logger.info("Comm closed")
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
            self.batched_stream.send({'op': 'long-running', 'key': key})

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
        changed = True
        try:
            while changed and self.data_needed and len(self.in_flight_workers) < self.total_connections:
                changed = False
                logger.debug("Ensure communicating.  Pending: %d.  Connections: %d/%d",
                             len(self.data_needed),
                             len(self.in_flight_workers),
                             self.total_connections)

                key = self.data_needed[0]

                if key not in self.tasks:
                    self.data_needed.popleft()
                    changed = True
                    continue

                if self.task_state.get(key) != 'waiting':
                    self.log.append((key, 'communication pass'))
                    self.data_needed.popleft()
                    changed = True
                    continue

                deps = self.dependencies[key]
                if self.validate:
                    assert all(dep in self.dep_state for dep in deps)

                deps = [dep for dep in deps if self.dep_state[dep] == 'waiting']

                missing_deps = {dep for dep in deps if not self.who_has.get(dep)}
                if missing_deps:
                    logger.info("Can't find dependencies for key %s", key)
                    missing_deps2 = {dep for dep in missing_deps
                                         if dep not in self._missing_dep_flight}
                    for dep in missing_deps2:
                        self._missing_dep_flight.add(dep)
                    self.loop.add_callback(self.handle_missing_dep,
                                           *missing_deps2)

                    deps = [dep for dep in deps if dep not in missing_deps]

                self.log.append(('gather-dependencies', key, deps))

                in_flight = False

                while deps and (len(self.in_flight_workers) < self.total_connections
                                or self.comm_nbytes < self.total_comm_nbytes):
                    dep = deps.pop()
                    if self.dep_state[dep] != 'waiting':
                        continue
                    if dep not in self.who_has:
                        continue
                    workers = [w for w in self.who_has[dep]
                                  if w not in self.in_flight_workers]
                    if not workers:
                        in_flight = True
                        continue
                    worker = random.choice(list(workers))
                    to_gather, total_nbytes = self.select_keys_for_gather(worker, dep)
                    self.comm_nbytes += total_nbytes
                    self.in_flight_workers[worker] = to_gather
                    for d in to_gather:
                        self.transition_dep(d, 'flight', worker=worker)
                    self.loop.add_callback(self.gather_dep, worker, dep,
                            to_gather, total_nbytes, cause=key)
                    changed = True

                if not deps and not in_flight:
                    self.data_needed.popleft()
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def send_task_state_to_scheduler(self, key):
        if key in self.data:
            nbytes = self.nbytes[key] or sizeof(self.data[key])
            typ = self.types.get(key) or type(self.data[key])
            try:
                typ = dumps_function(typ)
            except PicklingError:
                # Some types fail pickling (example: _thread.lock objects),
                # send their name as a best effort.
                typ = pickle.dumps(typ.__name__)
            d = {'op': 'task-finished',
                 'status': 'OK',
                 'key': key,
                 'nbytes': nbytes,
                 'thread': self.threads.get(key),
                 'type': typ}
        elif key in self.exceptions:
            d = {'op': 'task-erred',
                 'status': 'error',
                 'key': key,
                 'thread': self.threads.get(key),
                 'exception': self.exceptions[key],
                 'traceback': self.tracebacks[key]}
        else:
            logger.error("Key not ready to send to worker, %s: %s",
                         key, self.task_state[key])
            return

        if key in self.startstops:
            d['startstops'] = self.startstops[key]
        self.batched_stream.send(d)

    def put_key_in_memory(self, key, value, transition=True):
        if key in self.data:
            return

        start = time()
        self.data[key] = value
        stop = time()
        if stop - start > 0.020:
            self.startstops[key].append(('disk-write', start, stop))

        if key not in self.nbytes:
            self.nbytes[key] = sizeof(value)

        if key not in self.types:
            self.types[key] = type(value)

        self.types[key] = type(value)

        for dep in self.dependents.get(key, ()):
            if dep in self.waiting_for_data:
                if key in self.waiting_for_data[dep]:
                    self.waiting_for_data[dep].remove(key)
                if not self.waiting_for_data[dep]:
                    self.transition(dep, 'ready')

        if transition and key in self.task_state:
            self.transition(key, 'memory')

        self.log.append((key, 'put-in-memory'))

    def select_keys_for_gather(self, worker, dep):
        deps = {dep}

        total_bytes = self.nbytes[dep]
        L = self.pending_data_per_worker[worker]

        while L:
            d = L.popleft()
            if self.dep_state.get(d) != 'waiting':
                continue
            if total_bytes + self.nbytes[d] > self.target_message_size:
                break
            deps.add(d)
            total_bytes += self.nbytes[d]

        return deps, total_bytes

    @gen.coroutine
    def gather_dep(self, worker, dep, deps, total_nbytes, cause=None):
        if self.status != 'running':
            return
        with log_errors():
            response = {}
            try:
                if self.validate:
                    self.validate_state()

                self.log.append(('request-dep', dep, worker, deps))
                logger.debug("Request %d keys", len(deps))
                start = time()
                response = yield self.rpc(worker).get_data(keys=list(deps),
                                                           who=self.address)
                stop = time()

                if cause:
                    self.startstops[cause].append(('transfer', start, stop))

                total_bytes = sum(self.nbytes.get(dep, 0) for dep in response)
                duration = (stop - start) or 0.5
                self.incoming_transfer_log.append({
                    'start': start,
                    'stop': stop,
                    'middle': (start + stop) / 2.0,
                    'duration': duration,
                    'keys': {dep: self.nbytes.get(dep, None) for dep in response},
                    'total': total_bytes,
                    'bandwidth': total_bytes / duration,
                    'who': worker
                })
                if self.digests is not None:
                    self.digests['transfer-bandwidth'].add(total_bytes / duration)
                    self.digests['transfer-duration'].add(duration)
                self.counters['transfer-count'].add(len(response))
                self.incoming_count += 1

                self.log.append(('receive-dep', worker, list(response)))

                if response:
                    self.batched_stream.send({'op': 'add-keys',
                                              'keys': list(response)})
            except EnvironmentError as e:
                logger.exception("Worker stream died during communication: %s",
                                 worker)
                self.log.append(('receive-dep-failed', worker))
                for d in self.has_what.pop(worker):
                    self.who_has[d].remove(worker)
                    if not self.who_has[d]:
                        del self.who_has[d]

            except Exception as e:
                logger.exception(e)
                if self.batched_stream and LOG_PDB:
                    import pdb; pdb.set_trace()
                raise
            finally:
                self.comm_nbytes -= total_nbytes

                for d in self.in_flight_workers.pop(worker):
                    if d in response:
                        self.transition_dep(d, 'memory', value=response[d])
                    elif self.dep_state.get(d) != 'memory':
                        self.transition_dep(d, 'waiting', worker=worker)

                    if d not in response and d in self.dependents:
                        self.log.append(('missing-dep', d))

                if self.validate:
                    self.validate_state()

                self.ensure_computing()
                self.ensure_communicating()

    def bad_dep(self, dep):
        exc = ValueError("Could not find dependent %s.  Check worker logs" % str(dep))
        for key in self.dependents[dep]:
            msg = error_message(exc)
            self.exceptions[key] = msg['exception']
            self.tracebacks[key] = msg['traceback']
            self.transition(key, 'error')
        self.release_dep(dep)

    @gen.coroutine
    def handle_missing_dep(self, *deps):
        original_deps = list(deps)
        self.log.append(('handle-missing', deps))
        try:
            deps = {dep for dep in deps if dep in self.dependents}
            if not deps:
                return

            for dep in list(deps):
                suspicious = self.suspicious_deps[dep]
                if suspicious > 5:
                    deps.remove(dep)
                    self.bad_dep(dep)
            if not deps:
                return

            for dep in deps:
                logger.info("Dependent not found: %s %s .  Asking scheduler",
                            dep, self.suspicious_deps[dep])

            who_has = yield self.scheduler.who_has(keys=list(deps))
            who_has = {k: v for k, v in who_has.items() if v}
            self.update_who_has(who_has)
            for dep in deps:
                self.suspicious_deps[dep] += 1

                if not who_has.get(dep):
                    self.log.append((dep, 'no workers found',
                                     self.dependents.get(dep)))
                    self.release_dep(dep)
                else:
                    self.log.append((dep, 'new workers found'))
                    for key in self.dependents.get(dep, ()):
                        if key in self.waiting_for_data:
                            self.data_needed.append(key)

        except Exception as e:
            logger.exception(e)
        finally:
            for dep in original_deps:
                self._missing_dep_flight.remove(dep)

            self.ensure_communicating()

    @gen.coroutine
    def query_who_has(self, *deps):
        with log_errors():
            response = yield self.scheduler.who_has(keys=deps)
            self.update_who_has(response)
            raise gen.Return(response)

    def update_who_has(self, who_has):
        try:
            for dep, workers in who_has.items():
                if not workers:
                    continue
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

    def release_key(self, key, cause=None, reason=None, report=True):
        try:
            if key not in self.task_state:
                return
            state = self.task_state.pop(key)
            if reason == 'stolen' and state in ('executing', 'long-running', 'memory'):
                self.task_state[key] = state
                return
            if cause:
                self.log.append((key, 'release-key', cause))
            else:
                self.log.append((key, 'release-key'))
            del self.tasks[key]
            if key in self.data and key not in self.dep_state:
                del self.data[key]
                del self.nbytes[key]
                del self.types[key]

            if key in self.waiting_for_data:
                del self.waiting_for_data[key]

            for dep in self.dependencies.pop(key, ()):
                self.dependents[dep].remove(key)
                if not self.dependents[dep] and self.dep_state[dep] == 'waiting':
                    self.release_dep(dep)

            if key in self.threads:
                del self.threads[key]
            del self.priorities[key]
            del self.durations[key]

            if key in self.exceptions:
                del self.exceptions[key]
            if key in self.tracebacks:
                del self.tracebacks[key]

            if key in self.startstops:
                del self.startstops[key]

            if key in self.executing:
                self.executing.remove(key)

            if key in self.resource_restrictions:
                del self.resource_restrictions[key]

            if report and state in PROCESSING:  # not finished
                self.batched_stream.send({'op': 'release',
                                          'key': key,
                                          'cause': cause})
        except CommClosedError:
            pass
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def release_dep(self, dep, report=False):
        try:
            if dep not in self.dep_state:
                return
            self.log.append((dep, 'release-dep'))
            state = self.dep_state.pop(dep)

            if dep in self.suspicious_deps:
                del self.suspicious_deps[dep]

            if dep in self.who_has:
                for worker in self.who_has.pop(dep):
                    self.has_what[worker].remove(dep)

            if dep not in self.task_state:
                if dep in self.data:
                    del self.data[dep]
                    del self.types[dep]
                del self.nbytes[dep]

            if dep in self.in_flight_tasks:
                del self.in_flight_tasks[dep]

            for key in self.dependents.pop(dep, ()):
                self.dependencies[key].remove(dep)
                if self.task_state[key] != 'memory':
                    self.release_key(key, cause=dep)

            if report and state == 'memory':
                self.batched_stream.send({'op': 'release-worker-data',
                                          'keys': [dep]})
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
                if self.task_state.get(key) in READY:
                    self.transition(key, 'executing')
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    @gen.coroutine
    def execute(self, key, report=False):
        executor_error = None
        try:
            if key not in self.executing or key not in self.task_state:
                return
            if self.validate:
                assert key not in self.waiting_for_data
                assert self.task_state[key] == 'executing'

            function, args, kwargs = self.tasks[key]

            start = time()
            args2 = pack_data(args, self.data, key_types=str)
            kwargs2 = pack_data(kwargs, self.data, key_types=str)
            stop = time()
            if stop - start > 0.005:
                self.startstops[key].append(('disk-read', start, stop))
                if self.digests is not None:
                    self.digests['disk-load-duration'].add(stop - start)

            logger.debug("Execute key: %s worker: %s", key, self.address)  # TODO: comment out?
            try:
                result = yield self.executor_submit(key, apply_function, function,
                                                    args2, kwargs2,
                                                    self.execution_state, key)
            except RuntimeError as e:
                executor_error = e
                raise

            if self.task_state.get(key) not in ('executing', 'long-running'):
                return

            result['key'] = key
            value = result.pop('result', None)
            self.startstops[key].append(('compute', result['start'],
                                                    result['stop']))
            self.threads[key] = result['thread']

            if result['op'] == 'task-finished':
                self.nbytes[key] = result['nbytes']
                self.types[key] = result['type']
                self.transition(key, 'memory', value=value)
                if self.digests is not None:
                    self.digests['task-duration'].add(result['stop'] -
                                                      result['start'])
            else:
                self.exceptions[key] = result['exception']
                self.tracebacks[key] = result['traceback']
                logger.warning(" Compute Failed\n"
                    "Function:  %s\n"
                    "args:      %s\n"
                    "kwargs:    %s\n"
                    "Exception: %s\n",
                    str(funcname(function))[:1000],
                    convert_args_to_str(args2, max_len=1000),
                    convert_kwargs_to_str(kwargs2, max_len=1000),
                    repr(pickle.loads(result['exception'])))
                self.transition(key, 'error')

            logger.debug("Send compute response to scheduler: %s, %s", key,
                         result)

            if self.validate:
                assert key not in self.executing
                assert key not in self.waiting_for_data

            self.ensure_computing()
            self.ensure_communicating()
        except Exception as e:
            if executor_error is e:
                logger.error("Thread Pool Executor error: %s", e)
            else:
                logger.exception(e)
                if LOG_PDB:
                    import pdb; pdb.set_trace()
                raise
        finally:
            if key in self.executing:
                self.executing.remove(key)

    ##################
    # Administrative #
    ##################

    def validate_key_memory(self, key):
        assert key in self.data
        assert key in self.nbytes
        assert key not in self.waiting_for_data
        assert key not in self.executing
        assert key not in self.ready
        if key in self.dep_state:
            assert self.dep_state[key] == 'memory'

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

    def validate_dep_waiting(self, dep):
        assert dep not in self.data
        assert dep in self.nbytes
        assert self.dependents[dep]
        assert not any(key in self.ready for key in self.dependents[dep])

    def validate_dep_flight(self, dep):
        assert dep not in self.data
        assert dep in self.nbytes
        assert not any(key in self.ready for key in self.dependents[dep])
        peer = self.in_flight_tasks[dep]
        assert dep in self.in_flight_workers[peer]

    def validate_dep_memory(self, dep):
        assert dep in self.data
        assert dep in self.nbytes
        assert dep in self.types
        if dep in self.task_state:
            assert self.task_state[dep] == 'memory'

    def validate_dep(self, dep):
        try:
            state = self.dep_state[dep]
            if state == 'waiting':
                self.validate_dep_waiting(dep)
            elif state == 'flight':
                self.validate_dep_flight(dep)
            elif state == 'memory':
                self.validate_dep_memory(dep)
            else:
                raise ValueError("Unknown dependent state", state)
        except Exception as e:
            logger.exception(e)
            import pdb; pdb.set_trace()
            raise

    def validate_state(self):
        if self.status != 'running':
            return
        try:
            for key, workers in self.who_has.items():
                for w in workers:
                    assert key in self.has_what[w]

            for worker, keys in self.has_what.items():
                for k in keys:
                    assert worker in self.who_has[k]

            for key in self.task_state:
                self.validate_key(key)

            for dep in self.dep_state:
                self.validate_dep(dep)

            for key, deps in self.waiting_for_data.items():
                if key not in self.data_needed:
                    for dep in deps:
                        assert (dep in self.in_flight_tasks or
                                dep in self._missing_dep_flight or
                                self.who_has[dep].issubset(self.in_flight_workers))

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


def get_worker():
    """ Get the worker currently running this task

    Examples
    --------
    >>> def f():
    ...     worker = get_worker()  # The worker on which this task is running
    ...     return worker.address

    >>> future = client.submit(f)  # doctest: +SKIP
    >>> future.result()  # doctest: +SKIP
    'tcp://127.0.0.1:47373'

    See Also
    --------
    worker_client
    """
    return thread_state.execution_state['worker']
