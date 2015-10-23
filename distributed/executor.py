from __future__ import print_function, division, absolute_import

from collections import defaultdict
from concurrent.futures._base import DoneAndNotDoneFutures
from concurrent import futures
from functools import wraps, partial
import itertools
import logging
import uuid

from dask.base import tokenize, normalize_token
from dask.core import flatten
from toolz import first, groupby, valmap
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


tokens = (str(uuid.uuid4()) for i in itertools.count(1))


class Future(WrappedKey):
    """ The result of a remotely running computation """
    def __init__(self, key, executor):
        self.key = key
        self.executor = executor
        self.executor._inc_ref(key)

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
        result = yield self.executor._gather([self])
        if self.status == 'error' and raiseit:
            raise result[0]
        else:
            raise gen.Return(result[0])

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
    def __init__(self, center, start=True, delete_batch_time=1):
        self.center = coerce_to_rpc(center)
        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self.dask = dict()
        self.restrictions = dict()
        self.loop = IOLoop()
        self.report_queue = Queue()
        self.scheduler_queue = Queue()
        self._shutdown_event = Event()
        self._delete_batch_time = delete_batch_time

        if start:
            self.start()

    def start(self):
        """ Start scheduler running in separate thread """
        from threading import Thread
        self.loop.add_callback(self._go)
        self._loop_thread = Thread(target=self.loop.start)
        self._loop_thread.start()

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
        self.futures[key]['event'].clear()
        logger.debug("Release key %s", key)
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
        self.waiting = {}
        self.processing = {}
        self.stacks = {}

        worker_queues = {worker: Queue() for worker in self.ncores}
        delete_queue = Queue()

        coroutines = ([
            self.report(),
            scheduler(self.scheduler_queue, self.report_queue, worker_queues,
                      delete_queue, self.who_has, self.has_what, self.ncores,
                      self.dask, self.restrictions, self.waiting, self.stacks,
                      self.processing),
            delete(self.scheduler_queue, delete_queue,
                   self.center.ip, self.center.port, self._delete_batch_time)]
         + [worker(self.scheduler_queue, worker_queues[w], w, n)
            for w, n in self.ncores.items()])

        results = yield All(coroutines)
        self._shutdown_event.set()

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
                key = funcname(func) + '-' + tokenize(func, *args, **kwargs)
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

        if key not in self.futures:
            self.futures[key] = {'event': Event(), 'status': 'waiting'}

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self.scheduler_queue.put_nowait({'op': 'update-graph',
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
        pure = kwargs.get('pure', True)
        workers = kwargs.get('workers', None)
        if not callable(func):
            raise TypeError("First input to map must be a callable function")
        iterables = [list(it) for it in iterables]
        if pure:
            keys = [funcname(func) + '-' + tokenize(func, *args)
                    for args in zip(*iterables)]
        else:
            uid = str(uuid.uuid4())
            keys = [funcname(func) + '-' + uid + '-' + next(tokens)
                    for i in range(min(map(len, iterables)))]

        dsk = {key: (func,) + args for key, args in zip(keys, zip(*iterables))}

        for key in dsk:
            if key not in self.futures:
                self.futures[key] = {'event': Event(), 'status': 'waiting'}

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
        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': keys,
                                         'restrictions': restrictions})

        return [Future(key, self) for key in keys]

    @gen.coroutine
    def _gather(self, futures):
        futures2, keys = unpack_remotedata(futures)
        keys = list(keys)

        while True:
            yield All([self.futures[key]['event'].wait() for key in keys])
            try:
                data = yield _gather(self.center, keys)
            except KeyError as e:
                self.scheduler_queue.put_nowait({'op': 'missing-data',
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
    def _get(self, dsk, keys, restrictions=None):
        flatkeys = list(flatten(keys))
        for key in flatkeys:
            if key not in self.futures:
                self.futures[key] = {'event': Event(), 'status': None}
        futures = {key: Future(key, self) for key in flatkeys}

        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': flatkeys,
                                         'restrictions': restrictions or {}})

        packed = pack_data(keys, futures)
        result = yield self._gather(packed)
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
        return sync(self.loop, self._get, dsk, keys, **kwargs)


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
