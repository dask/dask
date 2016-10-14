from __future__ import print_function, division, absolute_import

from collections import defaultdict, Iterator, Iterable
from concurrent.futures._base import DoneAndNotDoneFutures, CancelledError
from contextlib import contextmanager
import copy
from datetime import timedelta
from functools import partial
from glob import glob
import logging
import os
import sys
import pickle
from time import sleep
import uuid
from threading import Thread
import six
import socket

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
from .utils_comm import WrappedKey, unpack_remotedata, pack_data
from .compatibility import Queue as pyQueue, Empty, isqueue
from .core import (read, write, connect, coerce_to_rpc, dumps,
        clean_exception, loads)
from .worker import dumps_function, dumps_task
from .utils import (All, sync, funcname, ignoring, queue_to_iterator,
        tokey, log_errors, str_graph)

logger = logging.getLogger(__name__)

_global_client = [None]


class Future(WrappedKey):
    """ A remotely running computation

    A Future is a local proxy to a result running on a remote worker.  A user
    manages future objects in the local Python process to determine what
    happens in the larger cluster.

    Examples
    --------

    Futures typically emerge from Client computations

    >>> my_future = client.submit(add, 1, 2)  # doctest: +SKIP

    We can track the progress and results of a future

    >>> my_future  # doctest: +SKIP
    <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>

    We can get the result or the exception and traceback from the future

    >>> my_future.result()  # doctest: +SKIP

    See Also
    --------
    Client:  Creates futures
    """
    def __init__(self, key, client):
        self.key = key
        self.client = client
        self.client._inc_ref(tokey(key))
        self._generation = self.client.generation
        self._cleared = False

        tkey = tokey(key)

        if tkey not in client.futures:
            client.futures[tkey] = {'event': Event(), 'status': 'pending'}

    @property
    def executor(self):
        return self.client

    @property
    def status(self):
        try:
            return self.client.futures[tokey(self.key)]['status']
        except KeyError:
            return 'cancelled'

    @property
    def event(self):
        return self.client.futures[tokey(self.key)]['event']

    def done(self):
        """ Is the computation complete? """
        return self.event.is_set()

    def result(self):
        """ Wait until computation completes. Gather result to local process """
        result = sync(self.client.loop, self._result, raiseit=False)
        if self.status == 'error':
            six.reraise(*result)
        if self.status == 'cancelled':
            raise result
        else:
            return result

    @gen.coroutine
    def _result(self, raiseit=True):
        try:
            d = self.client.futures[tokey(self.key)]
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
            result = yield self.client._gather([self])
            raise gen.Return(result[0])

    @gen.coroutine
    def _exception(self):
        yield self.event.wait()
        if self.status == 'error':
            exception = self.client.futures[self.key]['exception']
            raise Return(exception)
        else:
            raise Return(None)

    def exception(self):
        """ Return the exception of a failed task

        See Also
        --------
        Future.traceback
        """
        return sync(self.client.loop, self._exception)

    def add_done_callback(self, fn):
        """ Call callback on future when callback has finished

        The callback ``fn`` should take the future as its only argument.  This
        will be called regardless of if the future completes successfully,
        errs, or is cancelled
        """
        self.client.loop.add_callback(done_callback, self, fn)

    def cancel(self):
        """ Returns True if the future has been cancelled """
        return self.client.cancel([self])

    def cancelled(self):
        """ Returns True if the future has been cancelled """
        return tokey(self.key) not in self.client.futures

    @gen.coroutine
    def _traceback(self):
        yield self.event.wait()
        if self.status == 'error':
            raise Return(self.client.futures[tokey(self.key)]['traceback'])
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
        return sync(self.client.loop, self._traceback)

    @property
    def type(self):
        try:
            return self.client.futures[tokey(self.key)]['type']
        except KeyError:
            return None

    def release(self):
        if not self._cleared and self.client.generation == self._generation:
            self._cleared = True
            self.client._dec_ref(tokey(self.key))

    def __getstate__(self):
        return self.key

    def __setstate__(self, key):
        c = default_client()
        Future.__init__(self, key, c)
        c._send_to_scheduler({'op': 'update-graph', 'tasks': {},
                              'keys': [tokey(self.key)], 'client': c.id})

    def __del__(self):
        self.release()

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


@gen.coroutine
def done_callback(future, callback):
    """ Wait on future, then call callback """
    while future.status == 'pending':
        yield future.event.wait()
    callback(future)


@partial(normalize_token.register, Future)
def normalize_future(f):
    return [f.key, type(f)]


class Client(object):
    """ Drive computations on a distributed cluster

    The Client connects users to a distributed compute cluster.  It provides
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

    >>> client = Client('127.0.0.1:8787')  # doctest: +SKIP

    Use ``submit`` method to send individual computations to the cluster

    >>> a = client.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = client.submit(add, 10, 20)  # doctest: +SKIP

    Continue using submit or map on results to build up larger computations

    >>> c = client.submit(add, a, b)  # doctest: +SKIP

    Gather results with the ``gather`` method.

    >>> client.gather([c])  # doctest: +SKIP
    33

    See Also
    --------
    distributed.scheduler.Scheduler: Internal scheduler
    """
    def __init__(self, address=None, start=True, loop=None, timeout=3,
                 set_as_default=True):
        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self._should_close_loop = loop is None and start
        self.loop = loop or IOLoop() if start else IOLoop.current()
        self.coroutines = []
        self.id = str(uuid.uuid1())
        self.generation = 0
        self.status = None
        self._pending_msg_buffer = []
        if hasattr(address, 'scheduler_address'):
            self.cluster = address
            address = address.scheduler_address
        self._start_arg = address
        if set_as_default:
            self._previous_get = _globals.get('get')
            dask.set_options(get=self.get)
            self._previous_shuffle = _globals.get('shuffle')
            dask.set_options(shuffle='tasks')

        if start:
            self.start(timeout=timeout)

    def __str__(self):
        if hasattr(self, '_loop_thread'):
            n = sync(self.loop, self.scheduler.ncores)
            return '<Client: scheduler="%s:%d" processes=%d cores=%d>' % (
                    self.scheduler.ip, self.scheduler.port, len(n),
                    sum(n.values()))
        else:
            return '<Client: scheduler="%s:%d">' % (
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
        _global_client[0] = self
        sync(self.loop, self._start, **kwargs)
        self.status = 'running'

    def _send_to_scheduler(self, msg):
        if self.status is 'running':
            self.loop.add_callback(self.scheduler_stream.send, msg)
        elif self.status is 'connecting':
            self._pending_msg_buffer.append(msg)
        else:
            raise Exception("Client not running.  Status: %s" % self.status)

    @gen.coroutine
    def _start(self, timeout=3, **kwargs):
        if self._start_arg is None:
            from distributed.deploy import LocalCluster
            try:
                self.cluster = LocalCluster(loop=self.loop, start=False)
            except (OSError, socket.error):
                self.cluster = LocalCluster(scheduler_port=0, loop=self.loop,
                                            start=False)
            self._start_arg = self.cluster.scheduler_address
            while (not self.cluster.workers or
               len(self.cluster.scheduler.ncores) < len(self.cluster.workers)):
                yield gen.sleep(0.01)

        self.scheduler = coerce_to_rpc(self._start_arg, timeout=timeout)
        self.scheduler_stream = None

        yield self.ensure_connected()

        self.coroutines.append(self._handle_report())

    @gen.coroutine
    def reconnect(self, timeout=0.1):
        with log_errors():
            assert self.scheduler_stream.stream.closed()
            self.status = 'connecting'
            self.scheduler_stream = None

            events = [d['event'] for d in self.futures.values()]
            self.futures.clear()
            for e in events:
                e.set()

            while self.status == 'connecting':
                try:
                    yield self.ensure_connected()
                    break
                except IOError:
                    yield gen.sleep(timeout)

    @gen.coroutine
    def ensure_connected(self):
        if self.scheduler_stream and not self.scheduler_stream.closed():
            return

        try:
            stream = yield connect(self.scheduler.ip, self.scheduler.port)
        except:
            raise IOError("Could not connect to %s:%d" %
                          (self.scheduler.ip, self.scheduler.port))

        ident = yield self.scheduler.identity()

        yield write(stream, {'op': 'register-client',
                             'client': self.id})
        msg = yield read(stream)
        assert len(msg) == 1
        assert msg[0]['op'] == 'stream-start'

        bstream = BatchedSend(interval=10, loop=self.loop)
        bstream.start(stream)
        self.scheduler_stream = bstream

        _global_client[0] = self
        self.status = 'running'

        for msg in self._pending_msg_buffer:
            self._send_to_scheduler(msg)
        del self._pending_msg_buffer[:]

        logger.debug("Started scheduling coroutines. Synchronized")

    def __enter__(self):
        if not self.loop._running:
            self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown()

    def __del__(self):
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
        if self.status != 'closed':
            self._send_to_scheduler({'op': 'client-releases-keys',
                                     'keys': [key],
                                     'client': self.id})

    @gen.coroutine
    def _handle_report(self):
        """ Listen to scheduler """
        with log_errors():
            while True:
                try:
                    msgs = yield read(self.scheduler_stream.stream)
                except StreamClosedError:
                    logger.debug("Stream closed to scheduler", exc_info=True)
                    if self.status == 'running':
                        logger.info("Reconnecting...")
                        self.status = 'connecting'
                        yield self.reconnect()
                        continue
                    else:
                        break
                if not isinstance(msgs, list):
                    msgs = [msgs]

                breakout = False
                for msg in msgs:
                    logger.debug("Client receives message %s", msg)

                    if msg.get('status') == 'scheduler-error':
                        six.reraise(*clean_exception(**msg))
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
        with log_errors():
            if self.status == 'closed':
                raise Return()
            self._send_to_scheduler({'op': 'close-stream'})
            self.status = 'closed'
            if _global_client[0] is self:
                _global_client[0] = None
            if not fast:
                with ignoring(TimeoutError):
                    yield [gen.with_timeout(timedelta(seconds=2), f)
                            for f in self.coroutines]
            with ignoring(AttributeError):
                yield self.scheduler_stream.close(ignore_closed=True)
            with ignoring(AttributeError):
                self.scheduler.close_rpc()

    def shutdown(self, timeout=10):
        """ Send shutdown signal and wait until scheduler terminates

        This cancels all currently running tasks, clears the state of the
        scheduler, and shuts down all workers and scheduler.

        You do not need to call this when you finish your session.  You only
        need to call this if you want to take down the distributed cluster.

        See Also
        --------
        Client.restart
        """
        # XXX handling of self.status here is not thread-safe
        if self.status == 'closed':
            return
        sync(self.loop, self._shutdown, fast=True)
        assert self.status == 'closed'

        if self._should_close_loop:
            sync(self.loop, self.loop.stop)
            self.loop.close(all_fds=True)
            self._loop_thread.join(timeout=timeout)
        with ignoring(AttributeError):
            dask.set_options(get=self._previous_get)
        with ignoring(AttributeError):
            dask.set_options(shuffle=self._previous_shuffle)
        if self.get == _globals.get('get'):
            del _globals['get']
        with ignoring(AttributeError):
            self.cluster.close()

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
        >>> c = client.submit(add, a, b)  # doctest: +SKIP

        Returns
        -------
        Future

        See Also
        --------
        Client.map: Submit on many arguments at once
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

        skey = tokey(key)

        if skey in self.futures:
            return Future(key, self)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        if isinstance(workers, str):
            workers = [workers]
        if workers is not None:
            restrictions = {skey: workers}
            loose_restrictions = [skey] if allow_other_workers else []
        else:
            restrictions = {}
            loose_restrictions = []

        if kwargs:
            dsk = {skey: (apply, func, list(args), kwargs)}
        else:
            dsk = {skey: (func,) + tuple(args)}

        futures = self._graph_to_futures(dsk, [skey], restrictions,
                loose_restrictions, priority={skey: 0})

        logger.debug("Submit %s(...), %s", funcname(func), key)

        return futures[skey]

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
        >>> L = client.map(func, sequence)  # doctest: +SKIP

        Returns
        -------
        List, iterator, or Queue of futures, depending on the type of the
        inputs.

        See also
        --------
        Client.submit: Submit a single function
        """
        if not callable(func):
            raise TypeError("First input to map must be a callable function")

        if (all(map(isqueue, iterables)) or
            all(isinstance(i, Iterator) for i in iterables)):
            maxsize = kwargs.pop('maxsize', 0)
            q_out = pyQueue(maxsize=maxsize)
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
                    for i in range(min(map(len, iterables)))] if iterables else []

        if not kwargs:
            dsk = {key: (func,) + args
                   for key, args in zip(keys, zip(*iterables))}
        else:
            dsk = {key: (apply, func, (tuple, list(args)), kwargs)
                   for key, args in zip(keys, zip(*iterables))}

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

        priority = dict(zip(keys, range(len(keys))))

        futures = self._graph_to_futures(dsk, keys, restrictions,
                loose_restrictions, priority=priority)
        logger.debug("map(%s, ...)", funcname(func))

        return [futures[tokey(key)] for key in keys]

    @gen.coroutine
    def _gather(self, futures, errors='raise'):
        futures2, keys = unpack_remotedata(futures, byte_keys=True)
        keys = [tokey(key) for key in keys]
        bad_data = dict()

        @gen.coroutine
        def wait(k):
            """ Want to stop the All(...) early if we find an error """
            yield self.futures[k]['event'].wait()
            if self.futures[k]['status'] != 'finished':
                raise Exception()

        while True:
            logger.debug("Waiting on futures to clear before gather")

            with ignoring(Exception):
                yield All([wait(key) for key in keys if key in self.futures])

            exceptions = set()
            bad_keys = set()
            for key in keys:
                if (key not in self.futures or
                    self.futures[key]['status'] == 'error'):
                    exceptions.add(key)
                    if errors == 'raise':
                        try:
                            d = self.futures[key]
                            six.reraise(type(d['exception']),
                                        d['exception'],
                                        d['traceback'])
                        except KeyError:
                            six.reraise(CancelledError,
                                        CancelledError(key),
                                        None)
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
            futures2 = [f for f in futures2 if f not in bad_data]

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

    def gather(self, futures, errors='raise', maxsize=0):
        """ Gather futures from distributed memory

        Accepts a future, nested container of futures, iterator, or queue.
        The return type will match the input type.

        Returns
        -------
        Future results

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> x = c.submit(add, 1, 2)  # doctest: +SKIP
        >>> c.gather(x)  # doctest: +SKIP
        3
        >>> c.gather([x, [x], x])  # support lists and dicts # doctest: +SKIP
        [3, [3], 3]

        >>> seq = c.gather(iter([x, x]))  # support iterators # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        3

        See Also
        --------
        Client.scatter: Send data out to cluster
        """
        if isqueue(futures):
            qout = pyQueue(maxsize=maxsize)
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
            types = valmap(type, data)
        elif isinstance(data, (list, tuple, set, frozenset)):
            data2 = list(map(dumps, data))
            types = list(map(type, data))
        elif isinstance(data, (Iterable, Iterator)):
            data2 = list(map(dumps, data))
            types = list(map(type, data))
        else:
            raise TypeError("Don't know how to scatter %s" % type(data))
        keys = yield self.scheduler.scatter(data=data2, workers=workers,
                                            client=self.id,
                                            broadcast=broadcast)
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

        if isinstance(types, list):
            for key, typ in zip(keys, types):
                self.futures[key]['type'] = typ
        elif isinstance(types, dict):
            for key in keys:
                self.futures[key]['type'] = types[key]

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

    def scatter(self, data, workers=None, broadcast=False, maxsize=0):
        """ Scatter data into distributed memory

        This moves data from the local client process into the workers of the
        distributed scheduler.  Note that it is often better to submit jobs to
        your workers to have them load the data rather than loading data
        locally and then scattering it out to them.

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
        maxsize: int (optional)
            Maximum size of queue if using queues, 0 implies infinite

        Returns
        -------
        List, dict, iterator, or queue of futures matching the type of input.

        Examples
        --------
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.scatter([1, 2, 3])  # doctest: +SKIP
        [<Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,
         <Future: status: finished, key: 58e78e1b34eb49a68c65b54815d1b158>,
         <Future: status: finished, key: d3395e15f605bc35ab1bac6341a285e2>]

        >>> c.scatter({'x': 1, 'y': 2, 'z': 3})  # doctest: +SKIP
        {'x': <Future: status: finished, key: x>,
         'y': <Future: status: finished, key: y>,
         'z': <Future: status: finished, key: z>}

        Constrain location of data to subset of workers

        >>> c.scatter([1, 2, 3], workers=[('hostname', 8788)])   # doctest: +SKIP

        Handle streaming sequences of data with iterators or queues

        >>> seq = c.scatter(iter([1, 2, 3]))  # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        <Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,

        Broadcast data to all workers

        >>> [future] = c.scatter([element], broadcast=True)  # doctest: +SKIP

        See Also
        --------
        Client.gather: Gather data back to local process
        """
        if isqueue(data) or isinstance(data, Iterator):
            logger.debug("Starting thread for streaming data")
            qout = pyQueue(maxsize=maxsize)

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
        keys = {tokey(f.key) for f in futures_of(futures)}
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
    def _publish_dataset(self, **kwargs):
        coroutines = []
        for name, data in kwargs.items():
            keys = [tokey(f.key) for f in futures_of(data)]
            coroutines.append(self.scheduler.publish_dataset(keys=keys,
                name=tokey(name), data=dumps(data), client=self.id))

        outs = yield coroutines

    def publish_dataset(self, **kwargs):
        """
        Publish named datasets to scheduler

        This stores a named reference to a dask collection or list of futures
        on the scheduler.  These references are available to other Clients
        which can download the collection or futures with ``get_dataset``.

        Datasets are not immediately computed.  You may wish to call
        ``Client.persist`` prior to publishing a dataset.

        Parameters
        ----------
        kwargs: dict
            named collections to publish on the scheduler

        Examples
        --------
        Publishing client:

        >>> df = dd.read_csv('s3://...')  # doctest: +SKIP
        >>> df = c.persist(df) # doctest: +SKIP
        >>> c.publish_dataset(my_dataset=df)  # doctest: +SKIP

        Receiving client:

        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> df2 = c.get_dataset('my_dataset')  # doctest: +SKIP

        Returns
        -------
        None

        See Also
        --------
        Client.list_datasets
        Client.get_dataset
        Client.unpublish_dataset
        Client.persist
        """
        return sync(self.loop, self._publish_dataset, **kwargs)

    def unpublish_dataset(self, name):
        """
        Remove named datasets from scheduler

        Examples
        --------
        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> c.unpublish_datasets('my_dataset')  # doctest: +SKIP
        >>> c.list_datasets()  # doctest: +SKIP
        []

        See Also
        --------
        Client.publish_dataset
        """
        return sync(self.loop, self.scheduler.unpublish_dataset, name=name)

    def list_datasets(self):
        """
        List named datasets available on the scheduler

        See Also
        --------
        Client.publish_dataset
        Client.get_dataset
        """
        return sync(self.loop, self.scheduler.list_datasets)

    @gen.coroutine
    def _get_dataset(self, name):
        out = yield self.scheduler.get_dataset(name=name, client=self.id)

        with temp_default_client(self):
            data = loads(out['data'])
        raise Return(data)

    def get_dataset(self, name):
        """
        Get named dataset from the scheduler

        See Also
        --------
        Client.publish_dataset
        Client.list_datasets
        """
        return sync(self.loop, self._get_dataset, tokey(name))

    @gen.coroutine
    def _run(self, function, *args, **kwargs):
        nanny = kwargs.pop('nanny', False)
        workers = kwargs.pop('workers', None)
        responses = yield self.scheduler.broadcast(msg=dict(op='run',
                                                function=dumps(function),
                                                args=dumps(args),
                                                kwargs=dumps(kwargs)),
                                                workers=workers, nanny=nanny)
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
        >>> c.run(os.getpid)  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321,
         '192.168.0.102:9000': 5555}

        Restrict computation to particular workers with the ``workers=``
        keyword argument.

        >>> c.run(os.getpid, workers=['192.168.0.100:9000',
        ...                           '192.168.0.101:9000'])  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321}
        """
        return sync(self.loop, self._run, function, *args, **kwargs)

    def _graph_to_futures(self, dsk, keys, restrictions=None,
            loose_restrictions=None, allow_other_workers=True, priority=None):

        keyset = set(keys)
        flatkeys = list(map(tokey, keys))
        futures = {key: Future(key, self) for key in keyset}

        values = {k for k, v in dsk.items() if isinstance(v, Future)
                                            and k not in keyset}
        if values:
            dsk = dask.optimize.inline(dsk, keys=values)

        d = {k: unpack_remotedata(v) for k, v in dsk.items()}
        extra_keys = set.union(*[v[1] for v in d.values()]) if d else set()
        dsk2 = str_graph({k: v[0] for k, v in d.items()}, extra_keys)
        dsk3 = {k: v for k, v in dsk2.items() if k is not v}

        if restrictions:
            restrictions = keymap(tokey, restrictions)
            restrictions = valmap(list, restrictions)

        if loose_restrictions is not None:
            loose_restrictions = list(map(tokey, loose_restrictions))

        dependencies = {tokey(k): set(map(tokey, v[1])) for k, v in d.items()}

        for s in dependencies.values():
            for v in s:
                if v not in self.futures:
                    raise CancelledError(v)

        for k, v in dsk3.items():
            dependencies[k] |= set(_deps(dsk3, v))

        self._send_to_scheduler({'op': 'update-graph',
                                 'tasks': valmap(dumps_task, dsk3),
                                 'dependencies': valmap(list, dependencies),
                                 'keys': list(flatkeys),
                                 'restrictions': restrictions or {},
                                 'loose_restrictions': loose_restrictions,
                                 'client': self.id,
                                 'priority': priority})

        return futures

    @gen.coroutine
    def _get(self, dsk, keys, restrictions=None, raise_on_error=True):
        futures = self._graph_to_futures(dsk, set(flatten([keys])), restrictions)

        packed = pack_data(keys, futures)
        try:
            result = yield self._gather(packed)
        except Exception as e:
            if raise_on_error:
                raise
            else:
                result = 'error', e
                raise gen.Return(result)
        finally:
            for f in futures.values():
                f.release()
        if not raise_on_error:
            result = 'OK', result
        raise gen.Return(result)

    def get(self, dsk, keys, restrictions=None, loose_restrictions=None,
            **kwargs):
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
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3

        See Also
        --------
        Client.compute: Compute asynchronous collections
        """
        futures = self._graph_to_futures(dsk, set(flatten([keys])),
                restrictions, loose_restrictions)

        try:
            results = self.gather(futures)
        except (KeyboardInterrupt, Exception) as e:
            for f in futures.values():
                f.release()
            raise

        results2 = pack_data(keys, results)
        return results2

    def compute(self, collections, sync=False, optimize_graph=True,
            workers=None, allow_other_workers=False, **kwargs):
        """ Compute dask collections on cluster

        Parameters
        ----------
        collections: iterable of dask objects or single dask object
            Collections like dask.array or dataframe or dask.value objects
        sync: bool (optional)
            Returns Futures if False (default) or concrete values if True
        optimize_graph: bool
            Whether or not to optimize the underlying graphs
        workers: str, list, dict
            Which workers can run which parts of the computation
            If a string a list then the output collections will run on the listed
                workers, but other sub-computations can run anywhere
            If a dict then keys should be (tuples of) collections and values
                should be addresses or lists.
        allow_other_workers: bool, list
            If True then all restrictions in workers= are considered loose
            If a list then only the keys for the listed collections are loose
        **kwargs:
            Options to pass to the graph optimize calls

        Returns
        -------
        List of Futures if input is a sequence, or a single future otherwise

        Examples
        --------
        >>> from dask import do, value
        >>> from operator import add
        >>> x = dask.do(add)(1, 2)
        >>> y = dask.do(add)(x, x)
        >>> xx, yy = client.compute([x, y])  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6

        Also support single arguments

        >>> xx = client.compute(x)  # doctest: +SKIP


        See Also
        --------
        Client.get: Normal synchronous dask.get function
        """
        if isinstance(collections, (list, tuple, set, frozenset)):
            singleton = False
        else:
            collections = [collections]
            singleton = True

        variables = [a for a in collections if isinstance(a, Base)]

        if optimize_graph:
            groups = groupby(lambda x: x._optimize, variables)
            dsk = merge([opt(merge([v.dask for v in val]),
                             [v._keys() for v in val], **kwargs)
                        for opt, val in groups.items()])
        else:
            dsk = merge(c.dask for c in variables)

        names = ['finalize-%s' % tokenize(v) for v in variables]
        dsk2 = {name: (v._finalize, v._keys()) for name, v in zip(names, variables)}


        restrictions, loose_restrictions = get_restrictions(collections,
                workers, allow_other_workers)

        futures_dict = self._graph_to_futures(merge(dsk2, dsk), names,
                                              restrictions, loose_restrictions)

        i = 0
        futures = []
        for arg in collections:
            if isinstance(arg, Base):
                futures.append(futures_dict[names[i]])
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

    def persist(self, collections, optimize_graph=True, workers=None,
                allow_other_workers=None, **kwargs):
        """ Persist dask collections on cluster

        Starts computation of the collection on the cluster in the background.
        Provides a new dask collection that is semantically identical to the
        previous one, but now based off of futures currently in execution.

        Parameters
        ----------
        collections: sequence or single dask object
            Collections like dask.array or dataframe or dask.value objects
        optimize_graph: bool
            Whether or not to optimize the underlying graphs
        workers: str, list, dict
            Which workers can run which parts of the computation
            If a string a list then the output collections will run on the listed
                workers, but other sub-computations can run anywhere
            If a dict then keys should be (tuples of) collections and values
                should be addresses or lists.
        allow_other_workers: bool, list
            If True then all restrictions in workers= are considered loose
            If a list then only the keys for the listed collections are loose
        kwargs:
            Options to pass to the graph optimize calls

        Returns
        -------
        List of collections, or single collection, depending on type of input.

        Examples
        --------
        >>> xx = client.persist(x)  # doctest: +SKIP
        >>> xx, yy = client.persist([x, y])  # doctest: +SKIP


        See Also
        --------
        Client.compute
        """
        if isinstance(collections, (tuple, list, set, frozenset)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        assert all(isinstance(c, Base) for c in collections)

        if optimize_graph:
            groups = groupby(lambda x: x._optimize, collections)
            dsk = merge([opt(merge([v.dask for v in val]),
                             [v._keys() for v in val], **kwargs)
                        for opt, val in groups.items()])
        else:
            dsk = merge(c.dask for c in collections)

        names = {k for c in collections for k in flatten(c._keys())}

        restrictions, loose_restrictions = get_restrictions(collections,
                workers, allow_other_workers)

        futures = self._graph_to_futures(dsk, names, restrictions,
                loose_restrictions)

        result = [redict_collection(c, {k: futures[k]
                                        for k in flatten(c._keys())})
                  for c in collections]
        if singleton:
            return first(result)
        else:
            return result

    @gen.coroutine
    def _upload_environment(self, zipfile):
        name = os.path.split(zipfile)[1]
        yield self._upload_large_file(zipfile, name)

        def unzip(dask_worker=None):
            from distributed.utils import log_errors
            import zipfile
            import shutil
            with log_errors():
                a = os.path.join(dask_worker.worker_dir, name)
                b = os.path.join(dask_worker.local_dir, name)
                c = os.path.dirname(b)
                shutil.move(a, b)

                with zipfile.ZipFile(b) as f:
                    f.extractall(path=c)

                for fn in glob(os.path.join(c, name[:-4], 'bin', '*')):
                    st = os.stat(fn)
                    os.chmod(fn, st.st_mode | 64)  # chmod u+x fn

                assert os.path.exists(os.path.join(c, name[:-4]))
                return c

        responses = yield self._run(unzip, nanny=True)
        raise gen.Return(name[:-4])

    def upload_environment(self, name, zipfile):
        return sync(self.loop, self._upload_environment, name, zipfile)

    @gen.coroutine
    def _restart(self, environment=None):
        if environment:
            environment = yield self._upload_environment(environment)
        self._send_to_scheduler({'op': 'restart', 'environment': environment})
        self._restart_event = Event()
        yield self._restart_event.wait()
        self.generation += 1
        self.refcount.clear()

        raise gen.Return(self)

    def restart(self, environment=None):
        """ Restart the distributed network

        This kills all active work, deletes all data on the network, and
        restarts the worker processes.
        """
        return sync(self.loop, self._restart, environment=environment)

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

    @gen.coroutine
    def _upload_large_file(self, local_filename, remote_filename=None):
        if remote_filename is None:
            remote_filename = os.path.split(local_filename)[1]

        with open(local_filename, 'rb') as f:
            data = f.read()

        [future] = yield self._scatter([data])
        key = future.key
        yield self._replicate(future)

        def dump_to_file(dask_worker=None):
            if not os.path.isabs(remote_filename):
                fn = os.path.join(dask_worker.local_dir, remote_filename)
            else:
                fn = remote_filename
            with open(fn, 'wb') as f:
                f.write(dask_worker.data[key])

            return len(dask_worker.data[key])

        response = yield self._run(dump_to_file)

        assert all(len(data) == v for v in response.values())

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
        >>> client.upload_file('mylibrary.egg')  # doctest: +SKIP
        >>> from mylibrary import myfunc  # doctest: +SKIP
        >>> L = c.map(myfunc, seq)  # doctest: +SKIP
        """
        result = sync(self.loop, self._upload_file, filename,
                        raise_on_error=False)
        if isinstance(result, Exception):
            raise result

    @gen.coroutine
    def _rebalance(self, futures=None, workers=None):
        yield _wait(futures)
        keys = list({tokey(f.key) for f in self.futures_of(futures)})
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
        futures = self.futures_of(futures)
        yield _wait(futures)
        keys = {tokey(f.key) for f in futures}
        yield self.scheduler.replicate(keys=list(keys), n=n, workers=workers,
                branching_factor=branching_factor)

    def replicate(self, futures, n=None, workers=None, branching_factor=2):
        """ Set replication of futures within network

        Copy data onto many workers.  This helps to broadcast frequently
        accessed data and it helps to improve resilience.

        This performs a tree copy of the data throughout the network
        individually on each piece of data.  This operation blocks until
        complete.  It does not guarantee replication of data to future workers.

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
        >>> x = c.submit(func, *args)  # doctest: +SKIP
        >>> c.replicate([x])  # send to all workers  # doctest: +SKIP
        >>> c.replicate([x], n=3)  # send to three workers  # doctest: +SKIP
        >>> c.replicate([x], workers=['alice', 'bob'])  # send to specific  # doctest: +SKIP
        >>> c.replicate([x], n=1, workers=['alice', 'bob'])  # send to one of specific workers  # doctest: +SKIP
        >>> c.replicate([x], n=1)  # reduce replications # doctest: +SKIP

        See also
        --------
        Client.rebalance
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
        >>> c.ncores()  # doctest: +SKIP
        {'192.168.1.141:46784': 8,
         '192.167.1.142:47548': 8,
         '192.167.1.143:47329': 8,
         '192.167.1.144:37297': 8}

        See Also
        --------
        Client.who_has
        Client.has_what
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return sync(self.loop, self.scheduler.ncores, workers=workers)

    def who_has(self, futures=None):
        """ The workers storing each future's data

        Parameters
        ----------
        futures: list (optional)
            A list of futures, defaults to all data

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.who_has()  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784'],
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': ['192.168.1.141:46784']}

        >>> c.who_has([x, y])  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784']}

        See Also
        --------
        Client.has_what
        Client.ncores
        """
        if futures is not None:
            futures = self.futures_of(futures)
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
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.has_what()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.who_has
        Client.ncores
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return sync(self.loop, self.scheduler.has_what, workers=workers)

    def stacks(self, workers=None):
        """ The task queues on each worker

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.stacks()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.processing
        Client.who_has
        Client.has_what
        Client.ncores
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return sync(self.loop, self.scheduler.stacks, workers=workers)

    def processing(self, workers=None):
        """ The tasks currently running on each worker

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.processing()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.stacks
        Client.who_has
        Client.has_what
        Client.ncores
        """
        if (isinstance(workers, tuple)
            and all(isinstance(i, (str, tuple)) for i in workers)):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (list, set)):
            workers = [workers]
        return valmap(set, sync(self.loop, self.scheduler.processing,
                                workers=workers))

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
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.nbytes(summary=False)  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': 28,
         'inc-1e297fc27658d7b67b3a758f16bcf47a': 28,
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': 28}

        >>> c.nbytes(summary=True)  # doctest: +SKIP
        {'inc': 84}

        See Also
        --------
        Client.who_has
        """
        return sync(self.loop, self.scheduler.nbytes, keys=keys,
                    summary=summary)

    def scheduler_info(self):
        """ Basic information about the workers in the cluster

        Examples
        --------
        >>> c.scheduler_info()  # doctest: +SKIP
        {'id': '2de2b6da-69ee-11e6-ab6a-e82aea155996',
         'services': {},
         'type': 'Scheduler',
         'workers': {'127.0.0.1:40575': {'active': 0,
                                         'last-seen': 1472038237.4845693,
                                         'name': '127.0.0.1:40575',
                                         'services': {},
                                         'stored': 0,
                                         'time-delay': 0.0061032772064208984}}}
        """
        return sync(self.loop, self.scheduler.identity)

    def futures_of(self, futures):
        return futures_of(futures, client=self)

    def start_ipython(self, *args, **kwargs):
        raise Exception("Method moved to start_ipython_workers")

    @gen.coroutine
    def _start_ipython_workers(self, workers):
        if workers is None:
            workers = yield self.scheduler.ncores()

        responses = yield self.scheduler.broadcast(
            msg=dict(op='start_ipython'), workers=workers,
        )
        raise gen.Return((workers, responses))

    def start_ipython_workers(self, workers=None, magic_names=False,
                              qtconsole=False, qtconsole_args=None):
        """ Start IPython kernels on workers

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        magic_names: str or list(str) (optional)
            If defined, register IPython magics with these names for
            executing code on the workers.

        qtconsole: bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).

        qtconsole_args: list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> info = c.start_ipython_workers() # doctest: +SKIP
        >>> %remote info['192.168.1.101:5752'] worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', magic_names='w') # doctest: +SKIP
        >>> %w worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', qtconsole=True) # doctest: +SKIP

        Returns
        -------
        iter_connection_info: list
            List of connection_info dicts containing info necessary
            to connect Jupyter clients to the workers.

        See Also
        --------
        Client.start_ipython_scheduler: start ipython on the scheduler
        """

        if magic_names and isinstance(magic_names, six.string_types):
            magic_names = [magic_names]
        if isinstance(workers, six.string_types):
            workers = [workers]

        (workers, info_dict) = sync(self.loop, self._start_ipython_workers, workers)

        if 'IPython' in sys.modules:
            from ._ipython_utils import register_remote_magic
            register_remote_magic()
        if magic_names:
            from ._ipython_utils import register_worker_magic
            for worker, magic_name in zip(workers, magic_names):
                connection_info = info_dict[worker]
                register_worker_magic(connection_info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole
            for worker, connection_info in info_dict.items():
                connect_qtconsole(connection_info,
                                  name='dask-' + worker.replace(':','-'),
                                  extra_args=qtconsole_args,
                )
        return info_dict

    def start_ipython_scheduler(self, magic_name='scheduler_if_ipython',
                                qtconsole=False, qtconsole_args=None):
        """ Start IPython kernel on the scheduler

        Parameters
        ----------
        magic_name: str or None (optional)
            If defined, register IPython magic with this name for
            executing code on the scheduler.
            If not defined, register %scheduler magic if IPython is running.

        qtconsole: bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).

        qtconsole_args: list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> c.start_ipython_scheduler() # doctest: +SKIP
        >>> %scheduler scheduler.processing  # doctest: +SKIP
        {'127.0.0.1:3595': {'inc-1', 'inc-2'},
         '127.0.0.1:53589': {'inc-2', 'add-5'}}

        >>> c.start_ipython_scheduler(qtconsole=True) # doctest: +SKIP

        Returns
        -------
        connection_info: dict
            connection_info dict containing info necessary
            to connect Jupyter clients to the scheduler.

        See Also
        --------
        Client.start_ipython_workers: Start IPython on the workers
        """
        info = sync(self.loop, self.scheduler.start_ipython)
        if magic_name == 'scheduler_if_ipython':
            # default to %scheduler if in IPython, no magic otherwise
            in_ipython = False
            if 'IPython' in sys.modules:
                from IPython import get_ipython
                in_ipython = bool(get_ipython())
            if in_ipython:
                magic_name = 'scheduler'
            else:
                magic_name = None
        if magic_name:
            from ._ipython_utils import register_worker_magic
            register_worker_magic(info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole
            connect_qtconsole(info, name='dask-scheduler',
                              extra_args=qtconsole_args,)
        return info


Executor = Client


class CompatibleExecutor(Client):
    """ A concurrent.futures-compatible Client

    A subclass of Client that conforms to concurrent.futures API,
    allowing swapping in for other Clients.
    """

    def map(self, func, *iterables, **kwargs):
        """ Map a function on a sequence of arguments

        Returns
        -------
        iter_results: iterable
            Iterable yielding results of the map.

        See Also
        --------
        Client.map: for more info
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
        try:
            yield All({f.event.wait() for f in fs})
        except KeyError:
            raise CancelledError([f.key for f in fs
                                        if f.key not in f.client.futures])
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
    client = default_client()
    result = sync(client.loop, _wait, fs, timeout, return_when)
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
    fs = list(fs)
    if len(set(f.client for f in fs)) == 1:
        loop = first(fs).client.loop
    else:
        # TODO: Groupby client, spawn many _as_completed coroutines
        raise NotImplementedError(
        "as_completed on many event loops not yet supported")

    queue = pyQueue()

    coroutine = lambda: _as_completed(fs, queue)
    loop.add_callback(coroutine)

    for i in range(len(fs)):
        yield queue.get()


def default_client(c=None):
    """ Return an client if exactly one has started """
    if c:
        return c
    if _global_client[0]:
        return _global_client[0]
    else:
        raise ValueError("No clients found\n"
                "Start an client and point it to the scheduler address\n"
                "  from distributed import Client\n"
                "  client = Client('ip-addr-of-scheduler:8786')\n")


def ensure_default_get(client):
    if _globals['get'] != client.get:
        print("Setting global dask scheduler to use distributed")
        dask.set_options(get=client.get)


def redict_collection(c, dsk):
    from dask.delayed import Delayed
    if isinstance(c, Delayed):
        assert len(dsk) == 1
        return Delayed(first(dsk), [dsk])
    else:
        cc = copy.copy(c)
        cc.dask = dsk
        return cc


def futures_of(o, client=None):
    """ Future objects in a collection """
    stack = [o]
    futures = set()
    while stack:
        x = stack.pop()
        if type(x) in (tuple, set, list):
            stack.extend(x)
        if type(x) is dict:
            stack.extend(x.values())
        if type(x) is Future:
            futures.add(x)
        if hasattr(x, 'dask'):
            stack.extend(x.dask.values())

    if client is not None:
        bad = {f for f in futures if f.cancelled()}
        if bad:
            raise CancelledError(bad)

    return list(futures)


@contextmanager
def temp_default_client(c):
    """ Set the default client for the duration of the context

    Parameters
    ----------
    c : Client
        This is what default_client() will return within the with-block.
    """
    old_exec = default_client()
    _global_client[0] = c
    try:
        yield
    finally:
        _global_client[0] = old_exec

def get_restrictions(collections, workers, allow_other_workers):
    """ Get restrictions from inputs to compute/persist """
    if isinstance(workers, (str, tuple, list)):
        workers = {tuple(collections): workers}
    if isinstance(workers, dict):
        restrictions = {}
        for colls, ws in workers.items():
            if isinstance(ws, str):
                ws = [ws]
            if hasattr(colls, '._keys'):
                keys = flatten(colls._keys())
            else:
                keys = list({k for c in flatten(colls)
                                for k in flatten(c._keys())})
            restrictions.update({k: ws for k in keys})
    else:
        restrictions = {}

    if allow_other_workers is True:
        loose_restrictions = list(restrictions)
    elif allow_other_workers:
        loose_restrictions = list({k for c in flatten(allow_other_workers)
                                     for k in c._keys()})
    else:
        loose_restrictions = []

    return restrictions, loose_restrictions
