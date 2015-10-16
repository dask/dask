from __future__ import print_function, division, absolute_import

import itertools
import logging
import uuid

from dask.base import tokenize
from dask.core import flatten
from tornado import gen
from tornado.gen import Return
from tornado.locks import Event
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.queues import Queue

from .core import read, write, connect, rpc, coerce_to_rpc
from .client import WrappedKey, _gather, unpack_remotedata, pack_data
from .dask import scheduler, worker, delete
from .utils import All, sync, funcname

logger = logging.getLogger(__name__)


tokens = (str(i) for i in itertools.count(1))


class Future(WrappedKey):
    """ The result of a remotely running computation """
    def __init__(self, key, executor):
        self.key = key
        self.executor = executor

    @property
    def status(self):
        return self.executor.futures[self.key]['status']

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
            raise result
        else:
            return result

    @gen.coroutine
    def _result(self, raiseit=True):
        yield self.event.wait()
        result = yield _gather(self.executor.center, [self.key])
        if self.status == 'error' and raiseit:
            raise result[0]
        else:
            raise gen.Return(result[0])

    def __del__(self):
        self.executor._release_key(self.key)


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
    def __init__(self, center, start=False):
        self.center = coerce_to_rpc(center)
        self.futures = dict()
        self.dask = dict()
        self.report_queue = Queue()
        self.scheduler_queue = Queue()
        self._shutdown_event = Event()

        if start:
            self.start()

    def start(self):
        """ Start scheduler running in separate thread """
        from threading import Thread
        self.loop = IOLoop()
        self.loop.add_callback(self._go)
        self._loop_thread = Thread(target=self.loop.start)
        self._loop_thread.start()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown()

    def _release_key(self, key):
        """ Release key from distributed memory """
        self.futures[key]['event'].clear()
        del self.futures[key]
        self.scheduler_queue.put_nowait({'op': 'release-held-data',
                                         'key': key})

    @gen.coroutine
    def report(self):
        """ Listen to scheduler """
        while True:
            msg = yield self.report_queue.get()
            if msg['op'] == 'close':
                break
            if msg['op'] == 'task-finished':
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
                    self.futures[msg['key']]['event'].set()

    @gen.coroutine
    def _shutdown(self):
        """ Send shutdown signal and wait until _go completes """
        self.report_queue.put_nowait({'op': 'close'})
        self.scheduler_queue.put_nowait({'op': 'close'})
        yield self._shutdown_event.wait()

    def shutdown(self):
        """ Send shutdown signal and wait until scheduler terminates """
        self.report_queue.put_nowait({'op': 'close'})
        self.scheduler_queue.put_nowait({'op': 'close'})
        self.loop.stop()
        self._loop_thread.join()

    @gen.coroutine
    def _go(self):
        """ Setup and run all other coroutines.  Block until finished. """
        self.who_has, self.has_what, self.ncores = yield [self.center.who_has(),
                                                         self.center.has_what(),
                                                         self.center.ncores()]

        worker_queues = {worker: Queue() for worker in self.ncores}
        delete_queue = Queue()

        coroutines = ([
            self.report(),
            scheduler(self.scheduler_queue, self.report_queue, worker_queues,
                      delete_queue, self.who_has, self.has_what, self.ncores,
                      self.dask),
            delete(self.scheduler_queue, delete_queue,
                   self.center.ip, self.center.port)]
         + [worker(self.scheduler_queue, worker_queues[w], w, n)
            for w, n in self.ncores.items()])

        results = yield All(coroutines)
        self._shutdown_event.set()

    def submit(self, func, *args, **kwargs):
        """ Submit a function application to the scheduler

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
        key = kwargs.pop('key', None)
        pure = kwargs.pop('pure', True)

        if key is None:
            if pure:
                key = funcname(func) + '-' + tokenize(func, *args, **kwargs)
            else:
                key = funcname(func) + '-' + next(tokens)

        if key in self.futures:
            return Future(key, self)

        if kwargs:
            task = (apply, func, args, kwargs)
        else:
            task = (func,) + args

        if key not in self.futures:
            self.futures[key] = {'event': Event(), 'status': None}

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': {key: task},
                                         'keys': [key]})

        return Future(key, self)

    def map(self, func, seq, pure=True):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

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
        if pure:
            keys = [funcname(func) + '-' + tokenize(func, arg) for arg in seq]
        else:
            uid = str(uuid.uuid1())
            keys = [funcname(func) + '-' + uid + '-' + next(tokens) for arg in seq]

        dsk = {key: (func, arg) for key, arg in zip(keys, seq)}

        for key in dsk:
            if key not in self.futures:
                self.futures[key] = {'event': Event(), 'status': None}

        logger.debug("map(%s, ...)", funcname(func))
        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': keys})

        return [Future(key, self) for key in keys]

    @gen.coroutine
    def _gather(self, futures):
        futures2, keys = unpack_remotedata(futures)
        keys = list(keys)

        yield All([self.futures[key]['event'].wait() for key in keys])

        data = yield _gather(self.center, keys)
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
    def _get(self, dsk, keys):
        flatkeys = list(flatten(keys))
        for key in flatkeys:
            if key not in self.futures:
                self.futures[key] = {'event': Event(), 'status': None}
        futures = {key: Future(key, self) for key in flatkeys}

        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': flatkeys})

        packed = pack_data(keys, futures)
        result = yield self._gather(packed)
        raise gen.Return(result)

    def get(self, dsk, keys):
        """ Gather futures from distributed memory

        Accepts a future or any nested core container of futures

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3
        """
        return sync(self.loop, self._get, dsk, keys)
