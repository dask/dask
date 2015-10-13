from __future__ import print_function, division, absolute_import

from dask.base import tokenize
from dask.utils import funcname
from tornado import gen
from tornado.gen import Return
from tornado.locks import Event
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.queues import Queue

from .core import read, write, connect, rpc, coerce_to_rpc
from .client import WrappedKey, _gather
from .dask import scheduler, worker, delete
from .utils import All


log = print


class Future(WrappedKey):
    """ The result of a remotely running computation """
    def __init__(self, key, event, executor):
        self.key = key
        self.event = event
        self.executor = executor
        self.status = None
        self.event = Event()

    def _set_ready(self, status):
        if status:
            self.event.set()
        else:
            self.event.clear()

    def done(self):
        return self.event.is_set()

    @gen.coroutine
    def _result(self):
        yield self.event.wait()
        result = yield _gather(self.executor.center, [self.key])
        raise gen.Return(result[0])

    def __del__(self):
        self.executor._release_key(self.key)


class Executor(object):
    """ Distributed executor with data dependencies

    This executor resembles executors in concurrent.futures but also allows
    Futures within submit/map calls.

    Provide center address on initialization

    >>> executor = Executor(('127.0.0.1', 8787))

    Use ``submit`` method like normal

    >>> a = executor.submit(add, 1, 2)
    >>> b = executor.submit(add, 10, 20)

    Additionally, provide results of submit calls (futures) to further submit
    calls:

    >>> c = executor.submit(add, a, b)

    This allows for the dynamic creation of complex dependencies.
    """
    def __init__(self, center):
        self.center = coerce_to_rpc(center)
        self.futures = dict()
        self.dask = dict()
        self.interact_queue = Queue()
        self.scheduler_queue = Queue()
        self._shutdown_event = Event()

    def _release_key(self, key):
        self.futures[key].event.clear()
        del self.futures[key]
        self.scheduler_queue.put_nowait({'op': 'release-held-data',
                                         'key': key})

    @gen.coroutine
    def interact(self):
        """ Listen to scheduler """
        while True:
            msg = yield self.interact_queue.get()
            if msg['op'] == 'close':
                self.scheduler_queue.put_nowait(msg)
                break
            if msg['op'] == 'task-finished':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]._set_ready(True)
            if msg['op'] == 'lost-data':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]._set_ready(False)

    @gen.coroutine
    def _shutdown(self):
        """ Send shutdown signal and wait until _go completes """
        self.interact_queue.put_nowait({'op': 'close'})
        yield self._shutdown_event.wait()

    @gen.coroutine
    def _go(self):
        """ Setup and run all other coroutines.  Block until finished. """
        self.who_has, self.has_what, self.ncores = yield [self.center.who_has(),
                                                         self.center.has_what(),
                                                         self.center.ncores()]

        worker_queues = {worker: Queue() for worker in self.ncores}
        delete_queue = Queue()

        coroutines = ([self.interact(),
                       scheduler(self.scheduler_queue, self.interact_queue, worker_queues, delete_queue,
                                 self.who_has, self.has_what, self.ncores, self.dask),
                       delete(self.scheduler_queue, delete_queue, self.center.ip, self.center.port)]
                    + [worker(self.scheduler_queue, worker_queues[w], w, n)
                       for w, n in self.ncores.items()])

        results = yield All(coroutines)
        self._shutdown_event.set()


    def submit(self, func, *args, **kwargs):
        """ Submit a function application to the scheduler

        Returns
        -------
        Future

        See Also
        --------
        distributed.executor.Executor.submit:
        """
        key = kwargs.pop('key', None)
        if key is None:
            key = funcname(func) + '-' + tokenize(func, *args, **kwargs)

        if kwargs:
            task = (apply, func, args, kwargs)
        else:
            task = (func,) + args

        f = Future(key, Event(), self)
        self.futures[key] = f

        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': {key: task},
                                         'keys': [key]})

        return f

    def map(self, func, seq, key=None):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

        Returns
        -------
        list of futures

        See also
        --------
        distributed.executor.Executor.submit
        """
        if key is None:
            keys = [funcname(func) + '-' + tokenize(func, arg) for arg in seq]
        else:
            keys = [key + '-%d' % i for i in range(len(seq))]

        dsk = {key: (func, arg) for key, arg in zip(keys, seq)}

        futures = [Future(key, Event(), self) for key in keys]
        self.futures.update(dict(zip(keys, futures)))

        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': keys})

        return futures
