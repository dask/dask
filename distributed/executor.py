from __future__ import print_function, division, absolute_import

from collections import defaultdict
from concurrent.futures._base import DoneAndNotDoneFutures, CancelledError
from concurrent import futures
from functools import wraps, partial
import itertools
import logging
import os
import uuid

import dask
from dask.base import tokenize, normalize_token, Base
from dask.core import flatten
from dask.compatibility import apply
from toolz import first, groupby, merge
from tornado import gen
from tornado.gen import Return
from tornado.locks import Event
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError, IOStream
from tornado.queues import Queue

from .client import (WrappedKey, unpack_remotedata, pack_data)
from .core import read, write, connect, rpc, coerce_to_rpc
from .scheduler import Scheduler
from .utils import All, sync, funcname, ignoring

logger = logging.getLogger(__name__)


_global_executor = [None]


class Future(WrappedKey):
    """ A remotely running computation

    A Future is a local proxy to a result running on a remote worker.  A user
    manages future objects in the local Python process to determine what
    happens in the larger cluster.

    Examples
    --------

    Futures typically emerge from Executor computations

    >>> my_future = executor.submit(add, 1, 2)  # doctest: +SKIP

    We can track the progress and results of a future

    >>> my_future  # doctest: +SKIP
    <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>

    We can get the result or the exception and traceback from the future

    >>> my_future.result()  # doctest: +SKIP
    3

    See Also
    --------
    Executor:  Creates futures
    """
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
        """ Return the exception of a failed task

        See Also
        --------
        Future.traceback
        """
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
        """ Return the exception of a failed task

        See Also
        --------
        Future.exception
        """
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
    """ Drive computations on a distributed cluster

    The Executor connects users to a distributed compute cluster.  It provides
    an asynchronous user interface around functions and futures.  This class
    resembles executors in ``concurrent.futures`` but also allows ``Future``
    objects within ``submit/map`` calls.

    Parameters
    ----------
    address: string, tuple, or ``Scheduler``
        This can be the address of a ``Center`` or ``Scheduler`` servers, either
        as a string ``'127.0.0.1:8787'`` or tuple ``('127.0.0.1', 8787)``
        or it can be a local ``Scheduler`` object.

    Examples
    --------
    Provide cluster's head node address on initialization:

    >>> executor = Executor('127.0.0.1:8787')  # doctest: +SKIP

    Use ``submit`` method to send individual computations to the cluster

    >>> a = executor.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = executor.submit(add, 10, 20)  # doctest: +SKIP

    Continue using submit or map on results to build up larger computations

    >>> c = executor.submit(add, a, b)  # doctest: +SKIP

    Gather results with the ``gather`` method.

    >>> executor.gather([c])  # doctest: +SKIP
    33

    See Also
    --------
    distributed.scheduler.Scheduler: Internal scheduler
    """
    def __init__(self, address, start=True, delete_batch_time=1, loop=None):
        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self.loop = loop or IOLoop()
        self.coroutines = []
        self._start_arg = address

        if start:
            self.start(delete_batch_time=delete_batch_time)

    def start(self, **kwargs):
        """ Start scheduler running in separate thread """
        if hasattr(self, '_loop_thread'):
            return
        from threading import Thread
        self._loop_thread = Thread(target=self.loop.start)
        self._loop_thread.daemon = True
        _global_executor[0] = self
        self._loop_thread.start()
        sync(self.loop, self._start, **kwargs)

    def _send_to_scheduler(self, msg):
        if isinstance(self.scheduler, Scheduler):
            self.loop.add_callback(self.scheduler_queue.put_nowait, msg)
        elif isinstance(self.scheduler_stream, IOStream):
            write(self.scheduler_stream, msg)
        else:
            raise NotImplementedError()

    @gen.coroutine
    def _start(self, **kwargs):
        if isinstance(self._start_arg, Scheduler):
            self.scheduler = self._start_arg
            self.center = self._start_arg.center
        if isinstance(self._start_arg, str):
            ip, port = tuple(self._start_arg.split(':'))
            self._start_arg = (ip, int(port))
        if isinstance(self._start_arg, tuple):
            r = coerce_to_rpc(self._start_arg)
            ident = yield r.identity()
            if ident['type'] == 'Center':
                self.center = r
                self.scheduler = Scheduler(self.center, loop=self.loop,
                        **kwargs)
            elif ident['type'] == 'Scheduler':
                self.scheduler = r
                self.scheduler_stream = yield connect(*self._start_arg)
                yield write(self.scheduler_stream, {'op': 'start-control'})
                cip, cport = ident['center']
                self.center = rpc(ip=cip, port=cport)
            else:
                raise ValueError("Unknown Type")

        if isinstance(self.scheduler, Scheduler):
            if self.scheduler.status != 'running':
                yield self.scheduler.sync_center()
                self.scheduler.start()
            self.scheduler_queue = Queue()
            self.report_queue = Queue()
            self.coroutines.append(self.scheduler.handle_queues(
                self.scheduler_queue, self.report_queue))

        start_event = Event()
        self.coroutines.append(self._handle_report(start_event))

        _global_executor[0] = self
        yield start_event.wait()
        logger.debug("Started scheduling coroutines. Synchronized")

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
        self._send_to_scheduler({'op': 'release-held-data', 'key': key})

    @gen.coroutine
    def _handle_report(self, start_event):
        """ Listen to scheduler """
        if isinstance(self.scheduler, Scheduler):
            next_message = self.report_queue.get
        elif isinstance(self.scheduler_stream, IOStream):
            next_message = lambda: read(self.scheduler_stream)
        else:
            raise NotImplemented()

        while True:
            try:
                msg = yield next_message()
            except StreamClosedError:
                break

            if msg['op'] == 'stream-start':
                start_event.set()
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
            if msg['op'] == 'restart':
                logger.info("Receive restart signal from scheduler")
                events = [d['event'] for d in self.futures.values()]
                self.futures.clear()
                for e in events:
                    e.set()
                with ignoring(AttributeError):
                    self._restart_event.set()

    @gen.coroutine
    def _shutdown(self, fast=False):
        """ Send shutdown signal and wait until scheduler completes """
        self._send_to_scheduler({'op': 'close'})
        if _global_executor[0] is self:
            _global_executor[0] = None
        if not fast:
            yield self.coroutines

    def shutdown(self, timeout=10):
        """ Send shutdown signal and wait until scheduler terminates """
        self._send_to_scheduler({'op': 'close'})
        self.loop.stop()
        self._loop_thread.join(timeout=timeout)
        if _global_executor[0] is self:
            _global_executor[0] = None

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
        Executor.map: Submit on many arguments at once
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
                key = funcname(func) + '-' + str(uuid.uuid4())

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

        task2, _ = unpack_remotedata(task)

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self._send_to_scheduler({'op': 'update-graph',
                                'dsk': {key: task2},
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
        Executor.submit: Submit a single function
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
            keys = [funcname(func) + '-' + uid + '-' + str(uuid.uuid4())
                    for i in range(min(map(len, iterables)))]

        if not kwargs:
            dsk = {key: (func,) + args
                   for key, args in zip(keys, zip(*iterables))}
        else:
            dsk = {key: (apply, func, args, kwargs)
                   for key, args in zip(keys, zip(*iterables))}

        dsk = {key: unpack_remotedata(task)[0] for key, task in dsk.items()}

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
        self._send_to_scheduler({'op': 'update-graph',
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

            response, data = yield self.scheduler.gather(keys=keys)

            if response == b'error':
                logger.debug("Couldn't gather keys %s", data)
                self._send_to_scheduler({'op': 'missing-data',
                                        'missing': data.args})
                for key in data.args:
                    self.futures[key]['event'].clear()
            else:
                break

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
        >>> e.gather([x, [x], x])  # support lists and dicts # doctest: +SKIP
        [3, [3], 3]

        See Also
        --------
        Executor.scatter: Send data out to cluster
        """
        return sync(self.loop, self._gather, futures)

    @gen.coroutine
    def _scatter(self, data, workers=None):
        remotes = yield self.scheduler.scatter(data=data, workers=workers)
        if isinstance(remotes, list):
            remotes = [Future(r.key, self) for r in remotes]
            keys = {r.key for r in remotes}
        elif isinstance(remotes, dict):
            remotes = {k: Future(v.key, self) for k, v in remotes.items()}
            keys = set(remotes)

        for key in keys:
            self.futures[key]['status'] = 'finished'
            self.futures[key]['event'].set()

        raise gen.Return(remotes)

    def scatter(self, data, workers=None):
        """ Scatter data into distributed memory

        Accepts a list of data elements or dict of key-value pairs

        Optionally provide a set of workers to constrain the scatter.  Specify
        workers as hostname/port pairs, e.g. ``('127.0.0.1', 8787)``.

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

        See Also
        --------
        Executor.gather: Gather data back to local process
        """
        return sync(self.loop, self._scatter, data, workers=workers)

    @gen.coroutine
    def _get(self, dsk, keys, restrictions=None, raise_on_error=True):
        flatkeys = list(flatten([keys]))
        futures = {key: Future(key, self) for key in flatkeys}
        dsk2 = {k: unpack_remotedata(v)[0] for k, v in dsk.items()}
        dsk3 = {k: v for k, v in dsk2.items() if (k == v) is not True}

        self._send_to_scheduler({'op': 'update-graph',
                                'dsk': dsk3,
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
        """ Compute dask graph

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

        See Also
        --------
        Executor.compute: Compute asynchronous collections
        """
        status, result = sync(self.loop, self._get, dsk, keys,
                              raise_on_error=False, **kwargs)

        if status == 'error':
            raise result
        else:
            return result

    def compute(self, *args, **kwargs):
        """ Compute dask collections on cluster

        Parameters
        ----------
        args: iterable of dask objects
            Collections like dask.array or dataframe or dask.value objects
        sync: bool (optional)
            Returns Futures if False (default) or concrete values if True

        Returns
        -------
        List of Futures

        Examples
        --------

        >>> from dask import do, value
        >>> from operator import add
        >>> x = dask.do(add)(1, 2)
        >>> y = dask.do(add)(x, x)
        >>> xx, yy = executor.compute(x, y)  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6

        See Also
        --------
        Executor.get: Normal synchronous dask.get function
        """
        sync = kwargs.pop('sync', False)
        assert not kwargs
        if sync:
            return dask.compute(*args, get=self.get)

        variables = [a for a in args if isinstance(a, Base)]

        groups = groupby(lambda x: x._optimize, variables)
        dsk = merge([opt(merge([v.dask for v in val]),
                         [v._keys() for v in val])
                    for opt, val in groups.items()])
        names = ['finalize-%s' % tokenize(v) for v in variables]
        dsk2 = {name: (v._finalize, v, v._keys()) for name, v in zip(names, variables)}

        dsk3 = {k: unpack_remotedata(v)[0] for k, v in merge(dsk, dsk2).items()}

        self._send_to_scheduler({'op': 'update-graph',
                                'dsk': dsk3,
                                'keys': names})

        i = 0
        futures = []
        for arg in args:
            if isinstance(arg, Base):
                futures.append(Future(names[i], self))
                i += 1
            else:
                futures.append(arg)

        return futures

    @gen.coroutine
    def _restart(self):
        self._send_to_scheduler({'op': 'restart'})
        self._restart_event = Event()
        yield self._restart_event.wait()

        raise gen.Return(self)

    def restart(self):
        """ Restart the distributed network

        This kills all active work, deletes all data on the network, and
        restarts the worker processes.
        """
        return sync(self.loop, self._restart)

    @gen.coroutine
    def _upload_file(self, filename, raise_on_error=True):
        with open(filename, 'rb') as f:
            data = f.read()
        _, fn = os.path.split(filename)
        d = yield self.center.broadcast(msg={'op': 'upload_file',
                                             'filename': fn,
                                             'data': data})

        if any(isinstance(v, Exception) for v in d.values()):
            exception = next(v for v in d.values() if isinstance(v, Exception))
            if raise_on_error:
                raise exception
            else:
                raise gen.Return(exception)

        assert all(len(data) == v for v in d.values())

    def upload_file(self, filename):
        """ Upload local package to workers

        This sends a local file up to all worker nodes.  This file is placed
        into a temporary directory on Python's system path so any .py or .egg
        files will be importable.

        Parameters
        ----------
        filename: string
            Filename of .py or .egg file to send to workers

        Examples
        --------
        >>> executor.upload_file('mylibrary.egg')  # doctest: +SKIP
        >>> from mylibrary import myfunc  # doctest: +SKIP
        >>> L = e.map(myfunc, seq)  # doctest: +SKIP
        """
        result = sync(self.loop, self._upload_file, filename,
                        raise_on_error=False)
        if isinstance(result, Exception):
            raise result


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


def wait(fs, timeout=None, return_when='ALL_COMPLETED'):
    """ Wait until all futures are complete

    Parameters
    ----------
    fs: list of futures

    Returns
    -------
    Named tuple of completed, not completed
    """
    executor = default_executor()
    result = sync(executor.loop, _wait, fs, timeout, return_when)
    return result


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


def as_completed(fs):
    """ Return futures in the order in which they complete

    This returns an iterator that yields the input future objects in the order
    in which they complete.  Calling ``next`` on the iterator will block until
    the next future completes, irrespective of order.

    This function does not return futures in the order in which they are input.
    """
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
    if _global_executor[0]:
        return _global_executor[0]
    else:
        raise ValueError("No executors found\n"
                "Start an executor and point it to the center address\n"
                "  from distributed import Executor\n"
                "  executor = Executor('ip-addr-of-center:8787')\n")
