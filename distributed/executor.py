from __future__ import print_function, division, absolute_import

import itertools
import logging
import uuid

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
from .client import WrappedKey, _gather, unpack_remotedata, pack_data
from .dask import scheduler, worker, delete
from .utils import All, sync

logger = logging.getLogger(__name__)


tokens = (str(i) for i in itertools.count(1))


class Future(WrappedKey):
    """ The result of a remotely running computation """
    def __init__(self, key, executor):
        self.key = key
        self.executor = executor
        self.event = Event()
        self.status = None

    def _set_ready(self, status):
        self.status = status
        if status:
            self.event.set()
        else:
            self.event.clear()

    def done(self):
        return self.event.is_set()

    def result(self):
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
    def __init__(self, center):
        self.center = coerce_to_rpc(center)
        self.futures = dict()
        self.dask = dict()
        self.interact_queue = Queue()
        self.scheduler_queue = Queue()
        self._shutdown_event = Event()

    def start(self):
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
                break
            if msg['op'] == 'task-finished':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]._set_ready('finished')
            if msg['op'] == 'lost-data':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]._set_ready(False)
            if msg['op'] == 'task-erred':
                if msg['key'] in self.futures:
                    self.futures[msg['key']]._set_ready('error')

    @gen.coroutine
    def _shutdown(self):
        """ Send shutdown signal and wait until _go completes """
        self.interact_queue.put_nowait({'op': 'close'})
        self.scheduler_queue.put_nowait({'op': 'close'})
        yield self._shutdown_event.wait()

    def shutdown(self):
        self.interact_queue.put_nowait({'op': 'close'})
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
        pure = kwargs.pop('pure', True)

        if key is None:
            if pure:
                key = funcname(func) + '-' + tokenize(func, *args, **kwargs)
            else:
                key = funcname(func) + '-' + next(tokens)

        if key in self.futures:
            return self.futures[key]

        if kwargs:
            task = (apply, func, args, kwargs)
        else:
            task = (func,) + args

        f = Future(key, self)
        self.futures[key] = f

        logger.debug("Submit %s(...), %s", funcname(func), key)
        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': {key: task},
                                         'keys': [key]})

        return f

    def map(self, func, seq, pure=True):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

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

        futures = {key: Future(key, self) for key in dsk}
        self.futures.update(futures)

        logger.debug("map(%s, ...)", funcname(func))
        self.scheduler_queue.put_nowait({'op': 'update-graph',
                                         'dsk': dsk,
                                         'keys': keys})

        return [futures[key] for key in keys]

    @gen.coroutine
    def _gather(self, futures):
        futures2, keys = unpack_remotedata(futures)
        keys = list(keys)

        yield All([self.futures[key].event.wait() for key in keys])

        data = yield _gather(self.center, keys)
        data = dict(zip(keys, data))

        result = pack_data(futures2, data)
        raise gen.Return(result)
