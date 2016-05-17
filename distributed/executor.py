from __future__ import print_function, division, absolute_import

from collections import defaultdict, Iterator, Iterable
from concurrent.futures._base import DoneAndNotDoneFutures, CancelledError
import copy
from datetime import timedelta
from functools import partial
import logging
import os
from time import sleep
import uuid
from threading import Thread
import six

import dask
from dask.base import tokenize, normalize_token, Base
from dask.core import flatten, _deps
from dask.compatibility import apply
from dask.context import _globals
from toolz import first, groupby, merge, valmap, keymap
from tornado import gen
from tornado.gen import Return, TimeoutError
from tornado.locks import Event
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.queues import Queue

from .batched import BatchedSend
from .client import WrappedKey, unpack_remotedata, pack_data
from .compatibility import Queue as pyQueue, Empty, isqueue
from .core import (read, write, connect, coerce_to_rpc, dumps,
        clean_exception, loads)
from .worker import dumps_function, dumps_task
from .utils import (All, sync, funcname, ignoring, queue_to_iterator,
        tokey, log_errors, str_graph)

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
        if self.status == 'error':
            six.reraise(*result)
        if self.status == 'cancelled':
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
            if raiseit:
                six.reraise(*clean_exception(**d))
            else:
                raise Return(clean_exception(**d))
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

    def cancel(self):
        """ Returns True if the future has been cancelled """
        return self.executor.cancel([self])

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
        """ Return the traceback of a failed task

        This returns a traceback object.  You can inspect this object using the
        ``traceback`` module.  Alternatively if you call ``future.result()``
        this traceback will accompany the raised exception.

        Examples
        --------
        >>> import traceback  # doctest: +SKIP
        >>> tb = future.traceback()  # doctest: +SKIP
        >>> traceback.export_tb(tb)  # doctest: +SKIP
        [...]

        See Also
        --------
        Future.exception
        """
        return sync(self.executor.loop, self._traceback)

    @property
    def type(self):
        try:
            return self.executor.futures[self.key]['type']
        except KeyError:
            return None

    def __del__(self):
        self.executor._dec_ref(self.key)

    def __str__(self):
        if self.type:
            try:
                typ = self.type.__name__
            except AttributeError:
                typ = str(self.type)
            return '<Future: status: %s, type: %s, key: %s>' % (self.status,
                    typ, self.key)
        else:
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
        This can be the address of a ``Scheduler`` server, either
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
    def __init__(self, address, start=True, loop=None, timeout=3):
        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self._should_close_loop = loop is None and start
        self.loop = loop or IOLoop() if start else IOLoop.current()
        self.coroutines = []
        self.id = str(uuid.uuid1())
        self._start_arg = address

        if start:
            self.start(timeout=timeout)

    def __str__(self):
        if hasattr(self, '_loop_thread'):
            n = sync(self.loop, self.scheduler.ncores)
            return '<Executor: scheduler=%s:%d processes=%d cores=%d>' % (
                    self.scheduler.ip, self.scheduler.port, len(n),
                    sum(n.values()))
        else:
            return '<Executor: scheduler=%s:%d>' % (
                    self.scheduler.ip, self.scheduler.port)

    __repr__ = __str__

    def start(self, **kwargs):
        """ Start scheduler running in separate thread """
        if hasattr(self, '_loop_thread'):
            return
        if not self.loop._running:
            from threading import Thread
            self._loop_thread = Thread(target=self.loop.start)
            self._loop_thread.daemon = True
            self._loop_thread.start()
            while not self.loop._running:
                sleep(0.001)
        pc = PeriodicCallback(lambda: None, 1000, io_loop=self.loop)
        self.loop.add_callback(pc.start)
        _global_executor[0] = self
        sync(self.loop, self._start, **kwargs)

    def _send_to_scheduler(self, msg):
        self.loop.add_callback(self.scheduler_stream.send, msg)

    @gen.coroutine
    def _start(self, timeout=3, **kwargs):
        r = coerce_to_rpc(self._start_arg, timeout=timeout)
        try:
            ident = yield r.identity()
        except (StreamClosedError, OSError):
            raise IOError("Could not connect to %s:%d" % (r.ip, r.port))
        if ident['type'] == 'Scheduler':
            self.scheduler = r
            stream = yield connect(r.ip, r.port)
            yield write(stream, {'op': 'register-client',
                                 'client': self.id})
            bstream = BatchedSend(interval=10)
            bstream.start(stream)
            self.scheduler_stream = bstream
        else:
            raise ValueError("Unknown Type")

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
        self._send_to_scheduler({'op': 'client-releases-keys',
                                 'keys': [key],
                                 'client': self.id})

    @gen.coroutine
    def _handle_report(self, start_event):
        """ Listen to scheduler """
        with log_errors():
            while True:
                try:
                    msgs = yield read(self.scheduler_stream.stream)
                except StreamClosedError:
                    logger.debug("Stream closed to scheduler", exc_info=True)
                    break
                if not isinstance(msgs, list):
                    msgs = [msgs]

                breakout = False
                for msg in msgs:
                    logger.debug("Executor receives message %s", msg)

                    if msg['op'] == 'stream-start':
                        start_event.set()
                    elif msg['op'] == 'close':
                        breakout = True
                        break
                    elif msg['op'] == 'key-in-memory':
                        if msg['key'] in self.futures:
                            self.futures[msg['key']]['status'] = 'finished'
                            self.futures[msg['key']]['event'].set()
                            if (msg.get('type') and
                                not self.futures[msg['key']].get('type')):
                                self.futures[msg['key']]['type'] = loads(msg['type'])
                    elif msg['op'] == 'lost-data':
                        if msg['key'] in self.futures:
                            self.futures[msg['key']]['status'] = 'lost'
                            self.futures[msg['key']]['event'].clear()
                    elif msg['op'] == 'cancelled-key':
                        if msg['key'] in self.futures:
                            self.futures[msg['key']]['event'].set()
                            del self.futures[msg['key']]
                    elif msg['op'] == 'task-erred':
                        if msg['key'] in self.futures:
                            self.futures[msg['key']]['status'] = 'error'
                            try:
                                self.futures[msg['key']]['exception'] = loads(msg['exception'])
                            except TypeError:
                                self.futures[msg['key']]['exception'] = \
                                    Exception('Undeserializable exception', msg['exception'])
                            self.futures[msg['key']]['traceback'] = (loads(msg['traceback'])
                                                                     if msg['traceback'] else None)
                            self.futures[msg['key']]['event'].set()
                    elif msg['op'] == 'restart':
                        logger.info("Receive restart signal from scheduler")
                        events = [d['event'] for d in self.futures.values()]
                        self.futures.clear()
                        for e in events:
                            e.set()
                        with ignoring(AttributeError):
                            self._restart_event.set()
                    elif 'error' in msg['op']:
                        logger.warn("Scheduler exception:")
                        logger.exception(msg['exception'])
                if breakout:
                    break

    @gen.coroutine
    def _shutdown(self, fast=False):
        """ Send shutdown signal and wait until scheduler completes """
        self._send_to_scheduler({'op': 'close-stream'})
        if _global_executor[0] is self:
            _global_executor[0] = None
        if not fast:
            with ignoring(TimeoutError):
                yield [gen.with_timeout(timedelta(seconds=2), f)
                        for f in self.coroutines]
        with ignoring(AttributeError):
            yield self.scheduler_stream.close(ignore_closed=True)
        with ignoring(AttributeError):
            self.scheduler.close_streams()

    def shutdown(self, timeout=10):
        """ Send shutdown signal and wait until scheduler terminates """
        with ignoring(AttributeError):
            self.loop.add_callback(self.scheduler_stream.send,
                                   {'op': 'close-stream'})
            sync(self.loop, self.scheduler_stream.close)
        with ignoring(AttributeError):
            self.scheduler.close_streams()
        if self._should_close_loop:
            sync(self.loop, self.loop.stop)
            self.loop.close()
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
        allow_other_workers: bool (defaults to False)
            Used with `workers`. Inidicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).

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
        allow_other_workers = kwargs.pop('allow_other_workers', False)

        if allow_other_workers not in (True, False, None):
            raise TypeError("allow_other_workers= must be True or False")

        if key is None:
            if pure:
                key = funcname(func) + '-' + tokenize(func, kwargs, *args)
            else:
                key = funcname(func) + '-' + str(uuid.uuid4())

        key = tokey(key)

        if key in self.futures:
            return Future(key, self)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        if isinstance(workers, str):
            workers = [workers]
        if workers is not None:
            restrictions = {key: workers}
            loose_restrictions = [key] if allow_other_workers else []
        else:
            restrictions = {}
            loose_restrictions = []

        args2, arg_dependencies = unpack_remotedata(args, byte_keys=True)
        kwargs2, kwarg_dependencies = unpack_remotedata(kwargs, byte_keys=True)
        dependencies = arg_dependencies | kwarg_dependencies

        task = {'function': dumps_function(func)}
        if args2:
            task['args'] = dumps(args2)
        if kwargs2:
            task['kwargs'] = dumps(kwargs2)

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': {key: task},
                                 'keys': [key],
                                 'dependencies': {key: list(dependencies)},
                                 'restrictions': valmap(list, restrictions),
                                 'loose_restrictions': loose_restrictions,
                                 'client': self.id})

        return Future(key, self)

    def _threaded_map(self, q_out, func, qs_in, **kwargs):
        """ Internal function for mapping Queue """
        if isqueue(qs_in[0]):
            get = pyQueue.get
        elif isinstance(qs_in[0], Iterator):
            get = next
        else:
            raise NotImplementedError()

        while True:
            try:
                args = [get(q) for q in qs_in]
            except StopIteration as e:
                q_out.put(e)
                break
            f = self.submit(func, *args, **kwargs)
            q_out.put(f)


    def map(self, func, *iterables, **kwargs):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

        Parameters
        ----------
        func: callable
        iterables: Iterables, Iterators, or Queues
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
        List, iterator, or Queue of futures, depending on the type of the
        inputs.

        See also
        --------
        Executor.submit: Submit a single function
        """
        if not callable(func):
            raise TypeError("First input to map must be a callable function")

        if (all(map(isqueue, iterables)) or
            all(isinstance(i, Iterator) for i in iterables)):
            q_out = pyQueue()
            t = Thread(target=self._threaded_map, args=(q_out, func, iterables),
                                                  kwargs=kwargs)
            t.daemon = True
            t.start()
            if isqueue(iterables[0]):
                return q_out
            else:
                return queue_to_iterator(q_out)

        pure = kwargs.pop('pure', True)
        workers = kwargs.pop('workers', None)
        allow_other_workers = kwargs.pop('allow_other_workers', False)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        iterables = list(zip(*zip(*iterables)))
        if pure:
            keys = [funcname(func) + '-' + tokenize(func, kwargs, *args)
                    for args in zip(*iterables)]
        else:
            uid = str(uuid.uuid4())
            keys = [funcname(func) + '-' + uid + '-' + str(i)
                    for i in range(min(map(len, iterables)))]

        keys = [tokey(key) for key in keys]

        if not kwargs:
            dsk = {key: (func,) + args
                   for key, args in zip(keys, zip(*iterables))}
        else:
            dsk = {key: (apply, func, (tuple, list(args)), kwargs)
                   for key, args in zip(keys, zip(*iterables))}

        d = {key: unpack_remotedata(task, byte_keys=True) for key, task in dsk.items()}
        dsk2 = str_graph({k: v[0] for k, v in d.items()})
        dependencies = {k: set(map(tokey, v[1])) for k, v in d.items()}

        if isinstance(workers, str):
            workers = [workers]
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
        if allow_other_workers not in (True, False, None):
            raise TypeError("allow_other_workers= must be True or False")
        if allow_other_workers is True:
            loose_restrictions = set(keys)
        else:
            loose_restrictions = set()

        logger.debug("map(%s, ...)", funcname(func))
        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': valmap(dumps_task, dsk2),
                                 'dependencies': valmap(list, dependencies),
                                 'keys': keys,
                                 'restrictions': valmap(list, restrictions),
                                 'loose_restrictions': list(loose_restrictions),
                                 'client': self.id})

        return [Future(key, self) for key in keys]

    @gen.coroutine
    def _gather(self, futures, errors='raise'):
        futures2, keys = unpack_remotedata(futures, byte_keys=True)
        keys = [tokey(key) for key in keys]
        bad_data = dict()

        while True:
            logger.debug("Waiting on futures to clear before gather")
            yield All([self.futures[key]['event'].wait() for key in keys
                                                    if key in self.futures])
            exceptions = set()
            bad_keys = set()
            for key in keys:
                if self.futures[key]['status'] == 'error':
                    exceptions.add(key)
                    if errors == 'raise':
                        d = self.futures[key]
                        six.reraise(type(d['exception']),
                                    d['exception'],
                                    d['traceback'])
                    if errors == 'skip':
                        bad_keys.add(key)
                        bad_data[key] = None
                    else:
                        raise ValueError("Bad value, `errors=%s`" % errors)
            keys = [k for k in keys if k not in bad_keys]

            response = yield self.scheduler.gather(keys=keys)

            if response['status'] == 'error':
                logger.debug("Couldn't gather keys %s", response['keys'])
                self._send_to_scheduler({'op': 'missing-data',
                                         'keys': response['keys']})
                for key in response['keys']:
                    self.futures[key]['event'].clear()
            else:
                break

        if bad_data and errors == 'skip' and isinstance(futures2, list):
            futures2 = [f for f in futures2 if f not in exceptions]

        data = valmap(loads, response['data'])
        result = pack_data(futures2, merge(data, bad_data))
        raise gen.Return(result)

    def _threaded_gather(self, qin, qout, **kwargs):
        """ Internal function for gathering Queue """
        while True:
            L = [qin.get()]
            while qin.empty():
                try:
                    L.append(qin.get_nowait())
                except Empty:
                    break
            results = self.gather(L, **kwargs)
            for item in results:
                qout.put(item)

    def gather(self, futures, errors='raise'):
        """ Gather futures from distributed memory

        Accepts a future, nested container of futures, iterator, or queue.
        The return type will match the input type.

        Returns
        -------
        Future results

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> x = e.submit(add, 1, 2)  # doctest: +SKIP
        >>> e.gather(x)  # doctest: +SKIP
        3
        >>> e.gather([x, [x], x])  # support lists and dicts # doctest: +SKIP
        [3, [3], 3]

        >>> seq = e.gather(iter([x, x]))  # support iterators # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        3

        See Also
        --------
        Executor.scatter: Send data out to cluster
        """
        if isqueue(futures):
            qout = pyQueue()
            t = Thread(target=self._threaded_gather, args=(futures, qout),
                        kwargs={'errors': errors})
            t.daemon = True
            t.start()
            return qout
        elif isinstance(futures, Iterator):
            return (self.gather(f, errors=errors) for f in futures)
        else:
            return sync(self.loop, self._gather, futures, errors=errors)

    @gen.coroutine
    def _scatter(self, data, workers=None, broadcast=False):
        if isinstance(workers, str):
            workers = [workers]
        if isinstance(data, dict) and not all(isinstance(k, (bytes, str))
                                               for k in data):
            d = yield self._scatter(keymap(tokey, data), workers, broadcast)
            raise gen.Return({k: d[tokey(k)] for k in data})

        if isinstance(data, dict):
            data2 = valmap(dumps, data)
        elif isinstance(data, (list, tuple, set, frozenset)):
            data2 = list(map(dumps, data))
        elif isinstance(data, (Iterable, Iterator)):
            data2 = list(map(dumps, data))
        else:
            raise TypeError("Don't know how to scatter %s" % type(data))
        keys = yield self.scheduler.scatter(data=data2, workers=workers,
                                            client=self.id, broadcast=broadcast)
        if isinstance(data, dict):
            out = {k: Future(k, self) for k in keys}
        elif isinstance(data, (tuple, list, set, frozenset)):
            out = type(data)([Future(k, self) for k in keys])
        elif isinstance(data, (Iterable, Iterator)):
            out = [Future(k, self) for k in keys]
        else:
            raise TypeError(
                    "Input to scatter must be a list, iterator, or queue")

        for key in keys:
            self.futures[key]['status'] = 'finished'
            self.futures[key]['event'].set()

        raise gen.Return(out)

    def _threaded_scatter(self, q_or_i, qout, **kwargs):
        """ Internal function for scattering Iterable/Queue data """
        while True:
            if isqueue(q_or_i):
                L = [q_or_i.get()]
                while not q_or_i.empty():
                    try:
                        L.append(q_or_i.get_nowait())
                    except Empty:
                        break
            else:
                try:
                    L = [next(q_or_i)]
                except StopIteration as e:
                    qout.put(e)
                    break

            futures = self.scatter(L, **kwargs)
            for future in futures:
                qout.put(future)

    def scatter(self, data, workers=None, broadcast=False):
        """ Scatter data into distributed memory

        Parameters
        ----------
        data: list, iterator, dict, or Queue
            Data to scatter out to workers.  Output type matches input type.
        workers: list of tuples (optional)
            Optionally constrain locations of data.
            Specify workers as hostname/port pairs, e.g. ``('127.0.0.1', 8787)``.
        broadcast: bool (defaults to False)
            Whether to send each data element to all workers.
            By default we round-robin based on number of cores.

        Returns
        -------
        List, dict, iterator, or queue of futures matching the type of input.

        Examples
        --------
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.scatter([1, 2, 3])  # doctest: +SKIP
        [<Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,
         <Future: status: finished, key: 58e78e1b34eb49a68c65b54815d1b158>,
         <Future: status: finished, key: d3395e15f605bc35ab1bac6341a285e2>]

        >>> e.scatter({'x': 1, 'y': 2, 'z': 3})  # doctest: +SKIP
        {'x': <Future: status: finished, key: x>,
         'y': <Future: status: finished, key: y>,
         'z': <Future: status: finished, key: z>}

        Constrain location of data to subset of workers
        >>> e.scatter([1, 2, 3], workers=[('hostname', 8788)])   # doctest: +SKIP

        Handle streaming sequences of data with iterators or queues
        >>> seq = e.scatter(iter([1, 2, 3]))  # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        <Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,

        Broadcast data to all workers
        >>> [future] = e.scatter([element], broadcast=True)  # doctest: +SKIP

        See Also
        --------
        Executor.gather: Gather data back to local process
        """
        if isqueue(data) or isinstance(data, Iterator):
            logger.debug("Starting thread for streaming data")
            qout = pyQueue()

            t = Thread(target=self._threaded_scatter,
                       args=(data, qout),
                       kwargs={'workers': workers, 'broadcast': broadcast})
            t.daemon = True
            t.start()

            if isqueue(data):
                return qout
            else:
                return queue_to_iterator(qout)
        else:
            return sync(self.loop, self._scatter, data, workers=workers,
                        broadcast=broadcast)
    @gen.coroutine
    def _cancel(self, futures):
        keys = {f.key for f in futures_of(futures)}
        yield self.scheduler.cancel(keys=list(keys), client=self.id)
        for k in keys:
            with ignoring(KeyError):
                del self.futures[k]

    def cancel(self, futures):
        """
        Cancel running futures

        This stops future tasks from being scheduled if they have not yet run
        and deletes them if they have already run.  After calling, this result
        and all dependent results will no longer be accessible

        Parameters
        ----------
        futures: list of Futures
        """
        return sync(self.loop, self._cancel, futures)

    @gen.coroutine
    def _run(self, function, *args, **kwargs):
        workers = kwargs.pop('workers', None)
        responses = yield self.scheduler.broadcast(msg=dict(op='run',
                                                function=dumps(function),
                                                args=dumps(args),
                                                kwargs=dumps(kwargs)),
                                                workers=workers)
        results = {}
        for key, resp in responses.items():
            if resp['status'] == 'OK':
                results[key] = loads(resp['result'])
            elif resp['status'] == 'error':
                raise loads(resp['exception'])
        raise Return(results)

    def run(self, function, *args, **kwargs):
        """
        Run a function on all workers outside of task scheduling system

        This calls a function on all currently known workers immediately,
        blocks until those results come back, and returns the results
        asynchronously as a dictionary keyed by worker address.  This method
        if generally used for side effects, such and collecting diagnostic
        information or installing libraries.

        Parameters
        ----------
        function: callable
        *args: arguments for remote function
        **kwargs: keyword arguments for remote function
        workers: list
            Workers on which to run the function. Defaults to all known workers.

        Examples
        --------
        >>> e.run(os.getpid)  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321,
         '192.168.0.102:9000': 5555}

        Restrict computation to particular workers with the ``workers=``
        keyword argument.

        >>> e.run(os.getpid, workers=['192.168.0.100:9000',
        ...                           '192.168.0.101:9000'])  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321}
        """
        return sync(self.loop, self._run, function, *args, **kwargs)

    @gen.coroutine
    def _get(self, dsk, keys, restrictions=None, raise_on_error=True):
        keyset = set(flatten([keys]))
        flatkeys = list(map(tokey, flatten([keys])))
        futures = {key: Future(key, self) for key in flatkeys}

        values = {k for k, v in dsk.items() if isinstance(v, Future)
                                            and k not in keyset}
        if values:
            dsk = dask.optimize.inline(dsk, keys=values)

        d = {k: unpack_remotedata(v, byte_keys=True) for k, v in dsk.items()}
        dsk2 = str_graph({k: v[0] for k, v in d.items()})
        dsk3 = {k: v for k, v in dsk2.items() if (k == v) is not True}

        if restrictions:
            restrictions = keymap(tokey, restrictions)
            restrictions = valmap(list, restrictions)

        dependencies = {tokey(k): set(map(tokey, v[1])) for k, v in d.items()}

        for k, v in dsk3.items():
            dependencies[k] |= set(_deps(dsk3, v))

        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': valmap(dumps_task, dsk3),
                                 'dependencies': valmap(list, dependencies),
                                 'keys': list(flatkeys),
                                 'restrictions': restrictions or {},
                                 'client': self.id})

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

    def compute(self, args, sync=False):
        """ Compute dask collections on cluster

        Parameters
        ----------
        args: iterable of dask objects or single dask object
            Collections like dask.array or dataframe or dask.value objects
        sync: bool (optional)
            Returns Futures if False (default) or concrete values if True

        Returns
        -------
        List of Futures if input is a sequence, or a single future otherwise

        Examples
        --------
        >>> from dask import do, value
        >>> from operator import add
        >>> x = dask.do(add)(1, 2)
        >>> y = dask.do(add)(x, x)
        >>> xx, yy = executor.compute([x, y])  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6

        Also support single arguments

        >>> xx = executor.compute(x)  # doctest: +SKIP

        See Also
        --------
        Executor.get: Normal synchronous dask.get function
        """
        if isinstance(args, (list, tuple, set, frozenset)):
            singleton = False
        else:
            args = [args]
            singleton = True

        variables = [a for a in args if isinstance(a, Base)]

        groups = groupby(lambda x: x._optimize, variables)
        dsk = merge([opt(merge([v.dask for v in val]),
                         [v._keys() for v in val])
                    for opt, val in groups.items()])
        names = [tokey('finalize-%s' % tokenize(v)) for v in variables]
        dsk2 = {name: (v._finalize, v._keys()) for name, v in zip(names, variables)}

        d = {k: unpack_remotedata(v, byte_keys=True) for k, v in merge(dsk, dsk2).items()}
        dsk3 = str_graph({k: v[0] for k, v in d.items()})
        dependencies = {tokey(k): set(map(tokey, v[1])) for k, v in d.items()}

        for k, v in dsk3.items():
            dependencies[k] |= set(_deps(dsk3, v))

        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': valmap(dumps_task, dsk3),
                                 'dependencies': valmap(list, dependencies),
                                 'keys': names,
                                 'client': self.id})

        i = 0
        futures = []
        for arg in args:
            if isinstance(arg, Base):
                futures.append(Future(names[i], self))
                i += 1
            else:
                futures.append(arg)

        if sync:
            result = self.gather(futures)
        else:
            result = futures

        if singleton:
            return first(result)
        else:
            return result

    def persist(self, collections):
        """ Persist dask collections on cluster

        Starts computation of the collection on the cluster in the background.
        Provides a new dask collection that is semantically identical to the
        previous one, but now based off of futures currently in execution.

        Parameters
        ----------
        collections: sequence or single dask object
            Collections like dask.array or dataframe or dask.value objects

        Returns
        -------
        List of collections, or single collection, depending on type of input.

        Examples
        --------
        >>> xx = executor.persist(x)  # doctest: +SKIP
        >>> xx, yy = executor.persist([x, y])  # doctest: +SKIP

        See Also
        --------
        Executor.compute
        """
        if isinstance(collections, (tuple, list, set, frozenset)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        assert all(isinstance(c, Base) for c in collections)

        groups = groupby(lambda x: x._optimize, collections)
        dsk = merge([opt(merge([v.dask for v in val]),
                         [v._keys() for v in val])
                    for opt, val in groups.items()])

        d = {k: unpack_remotedata(v, byte_keys=True) for k, v in dsk.items()}
        dsk2 = str_graph({k: v[0] for k, v in d.items()})
        dependencies = {tokey(k): set(map(tokey, v[1])) for k, v in d.items()}

        for k, v in dsk2.items():
            dependencies[k] |= set(_deps(dsk2, v))

        names = list({tokey(k) for c in collections for k in flatten(c._keys())})

        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': valmap(dumps_task, dsk2),
                                 'dependencies': valmap(list, dependencies),
                                 'keys': names,
                                 'client': self.id})

        result = [redict_collection(c, {k: Future(tokey(k), self)
                                        for k in flatten(c._keys())})
                for c in collections]
        if singleton:
            return first(result)
        else:
            return result

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
        d = yield self.scheduler.broadcast(msg={'op': 'upload_file',
                                                'filename': fn,
                                                'data': data})

        if any(v['status'] == 'error' for v in d.values()):
            exceptions = [loads(v['exception']) for v in d.values()
                          if v['status'] == 'error']
            if raise_on_error:
                raise exceptions[0]
            else:
                raise gen.Return(exceptions[0])

        assert all(len(data) == v['nbytes'] for v in d.values())

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
    def _rebalance(self, futures=None, workers=None):
        yield _wait(futures)
        keys = list({f.key for f in futures_of(futures)})
        result = yield self.scheduler.rebalance(keys=keys, workers=workers)
        assert result['status'] == 'OK'

    def rebalance(self, futures=None, workers=None):
        """ Rebalance data within network

        Move data between workers to roughly balance memory burden.  This
        either affects a subset of the keys/workers or the entire network,
        depending on keyword arguments.

        This operation is generally not well tested against normal operation of
        the scheduler.  It it not recommended to use it while waiting on
        computations.

        Parameters
        ----------
        futures: list, optional
            A list of futures to balance, defaults all data
        workers: list, optional
            A list of workers on which to balance, defaults to all workers
        """
        sync(self.loop, self._rebalance, futures, workers),

    @gen.coroutine
    def _replicate(self, futures, n=None, workers=None, branching_factor=2):
        futures = futures_of(futures)
        yield _wait(futures)
        keys = {f.key for f in futures}
        yield self.scheduler.replicate(keys=list(keys), n=n, workers=workers,
                branching_factor=branching_factor)

    def replicate(self, futures, n=None, workers=None, branching_factor=2):
        """ Set replication of futures within network

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        This operation blocks until complete.  It does not guarantee
        replication of data to future workers.

        Parameters
        ----------
        futures: list of futures
            Futures we wish to replicate
        n: int, optional
            Number of processes on the cluster on which to replicate the data.
            Defaults to all.
        workers: list of worker addresses
            Workers on which we want to restrict the replication.
            Defaults to all.
        branching_factor: int, optional
            The number of workers that can copy data in each generation

        Examples
        --------
        >>> x = e.submit(func, *args)  # doctest: +SKIP
        >>> e.replicate([x])  # send to all workers  # doctest: +SKIP
        >>> e.replicate([x], n=3)  # send to three workers  # doctest: +SKIP
        >>> e.replicate([x], workers=['alice', 'bob'])  # send to specific  # doctest: +SKIP
        >>> e.replicate([x], n=1, workers=['alice', 'bob'])  # send to one of specific workers  # doctest: +SKIP
        >>> e.replicate([x], n=1)  # reduce replications # doctest: +SKIP

        See also
        --------
        Executor.rebalance
        """
        sync(self.loop, self._replicate, futures, n=n, workers=workers,
                branching_factor=branching_factor)

    def ncores(self, workers=None):
        """ The number of threads/cores available on each worker node

        Parameters
        ----------
        workers: list (optional)
            A list of workers that we care about specifically.
            Leave empty to receive information about all workers.

        Examples
        --------
        >>> e.ncores()  # doctest: +SKIP
        {'192.168.1.141:46784': 8,
         '192.167.1.142:47548': 8,
         '192.167.1.143:47329': 8,
         '192.167.1.144:37297': 8}

        See Also
        --------
        Executor.who_has
        Executor.has_what
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return sync(self.loop, self.scheduler.ncores, addresses=workers)

    def who_has(self, futures=None):
        """ The workers storing each future's data

        Parameters
        ----------
        futures: list (optional)
            A list of futures, defaults to all data

        Examples
        --------
        >>> x, y, z = e.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> e.who_has()  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784'],
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': ['192.168.1.141:46784']}

        >>> e.who_has([x, y])  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784']}

        See Also
        --------
        Executor.has_what
        Executor.ncores
        """
        if futures is not None:
            futures = futures_of(futures)
            keys = list({f.key for f in futures})
        else:
            keys = None
        return sync(self.loop, self.scheduler.who_has, keys=keys)

    def has_what(self, workers=None):
        """ Which keys are held by which workers

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = e.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> e.has_what()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Executor.who_has
        Executor.ncores
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return sync(self.loop, self.scheduler.has_what, keys=workers)

    def nbytes(self, keys=None, summary=True):
        """ The bytes taken up by each key on the cluster

        This is as measured by ``sys.getsizeof`` which may not accurately
        reflect the true cost.

        Parameters
        ----------
        keys: list (optional)
            A list of keys, defaults to all keys
        summary: boolean, (optional)
            Summarize keys into key types

        Examples
        --------
        >>> x, y, z = e.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> e.nbytes(summary=False)  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': 28,
         'inc-1e297fc27658d7b67b3a758f16bcf47a': 28,
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': 28}

        >>> e.nbytes(summary=True)  # doctest: +SKIP
        {'inc': 84}

        See Also
        --------
        Executor.who_has
        """
        return sync(self.loop, self.scheduler.nbytes, keys=keys,
                    summary=summary)


class CompatibleExecutor(Executor):
    """ A concurrent.futures-compatible Executor

    A subclass of Executor that conforms to concurrent.futures API,
    allowing swapping in for other Executors.
    """

    def map(self, func, *iterables, **kwargs):
        """ Map a function on a sequence of arguments

        Returns
        -------
        iter_results: iterable
            Iterable yielding results of the map.

        See Also
        --------
        Executor.map: for more info
        """
        list_of_futures = super(CompatibleExecutor, self).map(
                                func, *iterables, **kwargs)
        for f in list_of_futures:
            yield f.result()


@gen.coroutine
def _wait(fs, timeout=None, return_when='ALL_COMPLETED'):
    fs = futures_of(fs)
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
    fs = futures_of(fs)
    groups = groupby(lambda f: f.key, fs)
    firsts = [v[0] for v in groups.values()]
    wait_iterator = gen.WaitIterator(*[f.event.wait() for f in firsts])

    while not wait_iterator.done():
        yield wait_iterator.next()
        # TODO: handle case of restarted futures
        future = firsts[wait_iterator.current_index]
        for f in groups[future.key]:
            queue.put_nowait(f)


@gen.coroutine
def _first_completed(futures):
    """ Return a single completed future

    See Also:
        _as_completed
    """
    q = Queue()
    yield _as_completed(futures, q)
    result = yield q.get()
    raise gen.Return(result)


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

    queue = pyQueue()

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
                "Start an executor and point it to the scheduler address\n"
                "  from distributed import Executor\n"
                "  executor = Executor('ip-addr-of-scheduler:8786')\n")


def ensure_default_get(executor):
    if _globals['get'] != executor.get:
        print("Setting global dask scheduler to use distributed")
        dask.set_options(get=executor.get)


def redict_collection(c, dsk):
    from dask.delayed import Delayed
    if isinstance(c, Delayed):
        assert len(dsk) == 1
        return Delayed(first(dsk), [dsk])
    else:
        cc = copy.copy(c)
        cc.dask = dsk
        return cc


def futures_of(o):
    if isinstance(o, WrappedKey):
        return [o]
    if isinstance(o, (tuple, set, list)):
        return [f for item in o for f in futures_of(item)]
    if isinstance(o, dict):
        return [f for v in o.values() for f in futures_of(v)]
    if hasattr(o, 'dask'):
        return futures_of(o.dask)
    return []
