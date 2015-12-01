from __future__ import print_function, division, absolute_import

from collections import defaultdict
from concurrent.futures._base import DoneAndNotDoneFutures, CancelledError
from concurrent import futures
from functools import wraps, partial
import itertools
import logging
import os
import time
import uuid

from dask.base import tokenize, normalize_token
from dask.core import flatten, quote
from dask.compatibility import apply
from toolz import first, groupby, valmap
from tornado import gen
from tornado.gen import Return
from tornado.locks import Event
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.queues import Queue

from .client import (WrappedKey, _gather, unpack_remotedata, pack_data,
        scatter_to_workers)
from .core import read, write, connect, rpc, coerce_to_rpc
from .scheduler import Scheduler
from .sizeof import sizeof
from .utils import All, sync, funcname

logger = logging.getLogger(__name__)


tokens = (str(uuid.uuid4()) for i in itertools.count(1))


_global_executors = set()


class Future(WrappedKey):
    """ The result of a remotely running computation """
    def __init__(self, key, executor):
        self.key = key
        self.executor = executor
        self.executor._inc_ref(key)

        if key not in executor.futures:
            executor.futures[key] = {'event': Event(), 'status': 'pending'}

    @property
    def status(self):
        try:
            return self.executor.futures[self.key]['status']
        except KeyError:
            return 'cancelled'

    @property
    def event(self):
        return self.executor.futures[self.key]['event']

    def done(self):
        """ Is the computation complete? """
        return self.event.is_set()

    def result(self):
        """ Wait until computation completes. Gather result to local process """
        result = sync(self.executor.loop, self._result, raiseit=False)
        if self.status in ('error', 'cancelled'):
            raise result
        else:
            return result

    @gen.coroutine
    def _result(self, raiseit=True):
        try:
            d = self.executor.futures[self.key]
        except KeyError:
            exception = CancelledError(self.key)
            if raiseit:
                raise exception
            else:
                raise gen.Return(exception)

        yield d['event'].wait()
        if self.status == 'error':
            exception = d['exception']
            traceback = d['traceback']  # TODO: use me
            if raiseit:
                raise exception
            else:
                raise Return(exception)
        else:
            result = yield self.executor._gather([self])
            raise gen.Return(result[0])

    @gen.coroutine
    def _exception(self):
        yield self.event.wait()
        if self.status == 'error':
            exception = self.executor.futures[self.key]['exception']
            raise Return(exception)
        else:
            raise Return(None)

    def exception(self):
        """ Return the exception of a failed task """
        return sync(self.executor.loop, self._exception)

    def cancelled(self):
        """ Returns True if the future has been cancelled """
        return self.key not in self.executor.futures

    @gen.coroutine
    def _traceback(self):
        yield self.event.wait()
        if self.status == 'error':
            raise Return(self.executor.futures[self.key]['traceback'])
        else:
            raise Return(None)

    def traceback(self):
        """ Return the exception of a failed task """
        return sync(self.executor.loop, self._traceback)

    def __del__(self):
        self.executor._dec_ref(self.key)

    def __str__(self):
        return '<Future: status: %s, key: %s>' % (self.status, self.key)

    __repr__ = __str__


@partial(normalize_token.register, Future)
def normalize_future(f):
    return [f.key, type(f)]


class Executor(object):
    """ Distributed executor with data dependencies

    This executor resembles executors in concurrent.futures but also allows
    Futures within submit/map calls.

    Provide center address on initialization

    >>> executor = Executor(('127.0.0.1', 8787))  # doctest: +SKIP

    Use ``submit`` method like normal

    >>> a = executor.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = executor.submit(add, 10, 20)  # doctest: +SKIP

    Additionally, provide results of submit calls (futures) to further submit
    calls:

    >>> c = executor.submit(add, a, b)  # doctest: +SKIP

    This allows for the dynamic creation of complex dependencies.
    """
    def __init__(self, center, start=True, delete_batch_time=1, loop=None):
        self.center = coerce_to_rpc(center)
        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self.loop = loop or IOLoop()
        self.scheduler = Scheduler(center, delete_batch_time=delete_batch_time)

        if start:
            self.start()

    def start(self):
        """ Start scheduler running in separate thread """
        if hasattr(self, '_loop_thread'):
            return
        from threading import Thread
        self._loop_thread = Thread(target=self.loop.start)
        self._loop_thread.daemon = True
        _global_executors.add(self)
        self._loop_thread.start()
        sync(self.loop, self._start)

    @gen.coroutine
    def _start(self):
        yield self.scheduler._sync_center()
        self._scheduler_start_event = Event()
        self.coroutines = [self.scheduler.start(), self.report()]
        _global_executors.add(self)
        yield self._scheduler_start_event.wait()
        logger.debug("Started scheduling coroutines. Synchronized")

    @property
    def scheduler_queue(self):
        return self.scheduler.scheduler_queue

    @property
    def report_queue(self):
        return self.scheduler.report_queue

    def __enter__(self):
        if not self.loop._running:
            self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown()

    def _inc_ref(self, key):
        self.refcount[key] += 1

    def _dec_ref(self, key):
        self.refcount[key] -= 1
        if self.refcount[key] == 0:
            del self.refcount[key]
            self._release_key(key)

    def _release_key(self, key):
        """ Release key from distributed memory """
        logger.debug("Release key %s", key)
        if key in self.futures:
            self.futures[key]['event'].clear()
            del self.futures[key]
        self.loop.add_callback(self.scheduler_queue.put_nowait,
                {'op': 'release-held-data', 'key': key})

    @gen.coroutine
    def report(self):
        """ Listen to scheduler """
        while True:
            msg = yield self.report_queue.get()
            if msg['op'] == 'start':
                self._scheduler_start_event.set()
            if msg['op'] == 'close':
                break
            if msg['op'] == 'key-in-memory':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]['status'] = 'finished'
                    self.futures[msg['key']]['event'].set()
            if msg['op'] == 'lost-data':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]['status'] = 'lost'
                    self.futures[msg['key']]['event'].clear()
            if msg['op'] == 'task-erred':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]['status'] = 'error'
                    self.futures[msg['key']]['exception'] = msg['exception']
                    self.futures[msg['key']]['traceback'] = msg['traceback']
                    self.futures[msg['key']]['event'].set()

    @gen.coroutine
    def _shutdown(self, fast=False):
        """ Send shutdown signal and wait until scheduler completes """
        self.loop.add_callback(self.report_queue.put_nowait,
                               {'op': 'close'})
        self.loop.add_callback(self.scheduler_queue.put_nowait,
                               {'op': 'close'})
        if self in _global_executors:
            _global_executors.remove(self)
        if not fast:
            yield self.coroutines

    def shutdown(self):
        """ Send shutdown signal and wait until scheduler terminates """
        self.loop.add_callback(self.report_queue.put_nowait, {'op': 'close'})
        self.loop.add_callback(self.scheduler_queue.put_nowait, {'op': 'close'})
        self.loop.stop()
        self._loop_thread.join()
        if self in _global_executors:
            _global_executors.remove(self)

    def submit(self, func, *args, **kwargs):
        """ Submit a function application to the scheduler

        Parameters
        ----------
        func: callable
        *args:
        **kwargs:
        pure: bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
        workers: set, iterable of sets
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)

        Examples
        --------
        >>> c = executor.submit(add, a, b)  # doctest: +SKIP

        Returns
        -------
        Future

        See Also
        --------
        distributed.executor.Executor.submit:
        """
        if not callable(func):
            raise TypeError("First input to submit must be a callable function")

        key = kwargs.pop('key', None)
        pure = kwargs.pop('pure', True)
        workers = kwargs.pop('workers', None)

        if key is None:
            if pure:
                key = funcname(func) + '-' + tokenize(func, kwargs, *args)
            else:
                key = funcname(func) + '-' + next(tokens)

        if key in self.futures:
            return Future(key, self)

        if kwargs:
            task = (apply, func, args, kwargs)
        else:
            task = (func,) + args

        if workers is not None:
            restrictions = {key: workers}
        else:
            restrictions = {}

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self.loop.add_callback(self.scheduler_queue.put_nowait,
                                        {'op': 'update-graph',
                                         'dsk': {key: task},
                                         'keys': [key],
                                         'restrictions': restrictions})

        return Future(key, self)

    def map(self, func, *iterables, **kwargs):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

        Parameters
        ----------
        func: callable
        iterables: Iterables
        pure: bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
        workers: set, iterable of sets
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)

        Examples
        --------
        >>> L = executor.map(func, sequence)  # doctest: +SKIP

        Returns
        -------
        list of futures

        See also
        --------
        distributed.executor.Executor.submit
        """
        pure = kwargs.pop('pure', True)
        workers = kwargs.pop('workers', None)
        if not callable(func):
            raise TypeError("First input to map must be a callable function")
        iterables = [list(it) for it in iterables]
        if pure:
            keys = [funcname(func) + '-' + tokenize(func, kwargs, *args)
                    for args in zip(*iterables)]
        else:
            uid = str(uuid.uuid4())
            keys = [funcname(func) + '-' + uid + '-' + next(tokens)
                    for i in range(min(map(len, iterables)))]

        if not kwargs:
            dsk = {key: (func,) + args
                   for key, args in zip(keys, zip(*iterables))}
        else:
            dsk = {key: (apply, func, args, kwargs)
                   for key, args in zip(keys, zip(*iterables))}

        if isinstance(workers, (list, set)):
            if workers and isinstance(first(workers), (list, set)):
                if len(workers) != len(keys):
                    raise ValueError("You only provided %d worker restrictions"
                    " for a sequence of length %d" % (len(workers), len(keys)))
                restrictions = dict(zip(keys, workers))
            else:
                restrictions = {key: workers for key in keys}
        elif workers is None:
            restrictions = {}
        else:
            raise TypeError("Workers must be a list or set of workers or None")

        logger.debug("map(%s, ...)", funcname(func))
        self.loop.add_callback(self.scheduler_queue.put_nowait,
                                        {'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': keys,
                                         'restrictions': restrictions})

        return [Future(key, self) for key in keys]

    @gen.coroutine
    def _gather(self, futures):
        futures2, keys = unpack_remotedata(futures)
        keys = list(keys)

        while True:
            logger.debug("Waiting on futures to clear before gather")
            yield All([self.futures[key]['event'].wait() for key in keys
                                                    if key in self.futures])
            exceptions = [self.futures[key]['exception'] for key in keys
                          if self.futures[key]['status'] == 'error']
            if exceptions:
                raise exceptions[0]
            try:
                data = yield _gather(self.center, keys)
            except KeyError as e:
                logger.debug("Couldn't gather keys %s", e)
                self.loop.add_callback(self.scheduler_queue.put_nowait,
                                                {'op': 'missing-data',
                                                 'missing': e.args})
                for key in e.args:
                    self.futures[key]['event'].clear()
            else:
                break

        data = dict(zip(keys, data))

        result = pack_data(futures2, data)
        raise gen.Return(result)

    def gather(self, futures):
        """ Gather futures from distributed memory

        Accepts a future or any nested core container of futures

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> x = e.submit(add, 1, 2)  # doctest: +SKIP
        >>> e.gather(x)  # doctest: +SKIP
        3
        >>> e.gather([x, [x], x])  # doctest: +SKIP
        [3, [3], 3]
        """
        return sync(self.loop, self._gather, futures)

    @gen.coroutine
    def _scatter(self, data, workers=None):
        if not self.scheduler.ncores:
            raise ValueError("No workers yet found.  "
                             "Try syncing with center.\n"
                             "  e.sync_center()")
        ncores = workers if workers is not None else self.scheduler.ncores
        remotes, who_has, nbytes = yield scatter_to_workers(
                                            self.center, ncores, data)
        if isinstance(remotes, list):
            remotes = [Future(r.key, self) for r in remotes]
        elif isinstance(remotes, dict):
            remotes = {k: Future(v.key, self) for k, v in remotes.items()}
        self.loop.add_callback(self.scheduler_queue.put_nowait,
                                        {'op': 'update-data',
                                         'who-has': who_has,
                                         'nbytes': nbytes})
        while not all(k in self.scheduler.who_has for k in who_has):
            yield gen.sleep(0.001)
        raise gen.Return(remotes)

    def scatter(self, data, workers=None):
        """ Scatter data into distributed memory

        Accepts a list of data elements or dict of key-value pairs

        Optionally provide a set of workers to constrain the scatter.  Specify
        workers as hostname/port pairs, i.e.  ('127.0.0.1', 8787).
        Default port is 8788.

        Examples
        --------
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.scatter([1, 2, 3])  # doctest: +SKIP
        [RemoteData<center=127.0.0.1:8787, key=d1d26ff2-8...>,
         RemoteData<center=127.0.0.1:8787, key=d1d26ff2-8...>,
         RemoteData<center=127.0.0.1:8787, key=d1d26ff2-8...>]
        >>> e.scatter({'x': 1, 'y': 2, 'z': 3})  # doctest: +SKIP
        {'x': RemoteData<center=127.0.0.1:8787, key=x>,
         'y': RemoteData<center=127.0.0.1:8787, key=y>,
         'z': RemoteData<center=127.0.0.1:8787, key=z>}

        >>> e.scatter([1, 2, 3], workers=[('hostname', 8788)])  # doctest: +SKIP
        """
        return sync(self.loop, self._scatter, data, workers=workers)

    @gen.coroutine
    def _get(self, dsk, keys, restrictions=None, raise_on_error=True):
        flatkeys = list(flatten([keys]))
        futures = {key: Future(key, self) for key in flatkeys}

        self.loop.add_callback(self.scheduler_queue.put_nowait,
                                        {'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': flatkeys,
                                         'restrictions': restrictions or {}})

        packed = pack_data(keys, futures)
        if raise_on_error:
            result = yield self._gather(packed)
        else:
            try:
                result = yield self._gather(packed)
                result = 'OK', result
            except Exception as e:
                result = 'error', e
        raise gen.Return(result)

    def get(self, dsk, keys, **kwargs):
        """ Gather futures from distributed memory

        Parameters
        ----------
        dsk: dict
        keys: object, or nested lists of objects
        restrictions: dict (optional)
            A mapping of {key: {set of worker hostnames}} that restricts where
            jobs can take place

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3
        """
        status, result = sync(self.loop, self._get, dsk, keys,
                              raise_on_error=False, **kwargs)

        if status == 'error':
            raise result
        else:
            return result

    @gen.coroutine
    def _restart(self):
        logger.debug("Sending shutdown signal to workers")
        nannies = yield self.center.nannies()
        for addr in nannies:
            self.loop.add_callback(self.scheduler_queue.put_nowait,
                    {'op': 'worker-failed', 'worker': addr, 'heal': False})

        logger.debug("Sending kill signal to nannies")
        nannies = [rpc(ip=ip, port=n_port)
                   for (ip, w_port), n_port in nannies.items()]
        yield All([nanny.kill() for nanny in nannies])

        while self.scheduler.ncores:
            yield gen.sleep(0.01)

        yield self._shutdown(fast=True)

        events = [d['event'] for d in self.futures.values()]
        self.futures.clear()
        for e in events:
            e.set()

        yield All([nanny.instantiate(close=True) for nanny in nannies])

        logger.info("Restarting executor")
        self.scheduler.report_queue = Queue()
        self.scheduler.scheduler_queue = Queue()
        self.scheduler.delete_queue = Queue()
        yield self._start()

    def restart(self):
        """ Restart the distributed network

        This kills all active work, deletes all data on the network, and
        restarts the worker processes.
        """
        return sync(self.loop, self._restart)

    @gen.coroutine
    def _upload_file(self, filename):
        with open(filename, 'rb') as f:
            data = f.read()
        _, fn = os.path.split(filename)
        d = yield self.center.broadcast(msg={'op': 'upload_file',
                                             'filename': fn,
                                             'data': data})

        assert all(len(data) == v for v in d.values())

    def upload_file(self, filename):
        """ Upload local package to workers

        Parameters
        ----------
        filename: string
            Filename of .py file to send to workers
        """
        return sync(self.loop, self._upload_file, filename)


@gen.coroutine
def _wait(fs, timeout=None, return_when='ALL_COMPLETED'):
    if timeout is not None:
        raise NotImplementedError("Timeouts not yet supported")
    if return_when == 'ALL_COMPLETED':
        yield All({f.event.wait() for f in fs})
        done, not_done = set(fs), set()
    else:
        raise NotImplementedError("Only return_when='ALL_COMPLETED' supported")

    raise gen.Return(DoneAndNotDoneFutures(done, not_done))


ALL_COMPLETED = 'ALL_COMPLETED'


@wraps(futures.wait)
def wait(fs, timeout=None, return_when='ALL_COMPLETED'):
    if len(set(f.executor for f in fs)) == 1:
        loop = first(fs).executor.loop
    else:
        # TODO: Groupby executor, spawn many _as_completed coroutines
        raise NotImplementedError("wait on many event loops not yet supported")

    return sync(loop, _wait, fs, timeout, return_when)


@gen.coroutine
def _as_completed(fs, queue):
    groups = groupby(lambda f: f.key, fs)
    firsts = [v[0] for v in groups.values()]
    wait_iterator = gen.WaitIterator(*[f.event.wait() for f in firsts])

    while not wait_iterator.done():
        result = yield wait_iterator.next()
        # TODO: handle case of restarted futures
        future = firsts[wait_iterator.current_index]
        for f in groups[future.key]:
            queue.put_nowait(f)


@wraps(futures.as_completed)
def as_completed(fs):
    if len(set(f.executor for f in fs)) == 1:
        loop = first(fs).executor.loop
    else:
        # TODO: Groupby executor, spawn many _as_completed coroutines
        raise NotImplementedError(
        "as_completed on many event loops not yet supported")

    from .compatibility import Queue
    queue = Queue()

    coroutine = lambda: _as_completed(fs, queue)
    loop.add_callback(coroutine)

    for i in range(len(fs)):
        yield queue.get()


def default_executor(e=None):
    """ Return an executor if exactly one has started """
    if e:
        return e
    if len(_global_executors) == 1:
        return first(_global_executors)
    if len(_global_executors) == 0:
        raise ValueError("No executors found\n"
                "Start an executor and point it to the center address\n"
                "  from distributed import Executor\n"
                "  executor = Executor('ip-addr-of-center:8787')\n")
    if len(_global_executors) > 1:
        raise ValueError("There are %d executors running.\n"
            "Please specify which executor you want with the executor= \n"
            "keyword argument." % len(_global_executors))
