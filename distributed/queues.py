from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import uuid

from tornado import gen
import tornado.queues

from .client import Future, _get_global_client, Client
from .utils import tokey, sync

logger = logging.getLogger(__name__)


class QueueExtension(object):
    """ An extension for the scheduler to manage queues

    This adds the following routes to the scheduler

    *  queue_create
    *  queue_release
    *  queue_put
    *  queue_get
    *  queue_size
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.queues = dict()
        self.client_refcount = dict()
        self.future_refcount = defaultdict(lambda: 0)

        self.scheduler.handlers.update({'queue_create': self.create,
                                        'queue_release': self.release,
                                        'queue_put': self.put,
                                        'queue_get': self.get,
                                        'queue_qsize': self.qsize})

        self.scheduler.client_handlers['queue-future-release'] = self.future_release

        self.scheduler.extensions['queues'] = self

    def create(self, stream=None, name=None, client=None, maxsize=0):
        if name not in self.queues:
            self.queues[name] = tornado.queues.Queue(maxsize=maxsize)
            self.client_refcount[name] = 1
        else:
            self.client_refcount[name] += 1

    def release(self, stream=None, name=None, client=None):
        self.client_refcount[name] -= 1
        if self.client_refcount[name] == 0:
            del self.client_refcount[name]
            futures = self.queues[name].queue
            del self.queues[name]
            self.scheduler.client_releases_keys(keys=[f.key for f in futures],
                                                client='queue-%s' % name)

    @gen.coroutine
    def put(self, stream=None, name=None, key=None, data=None, client=None, timeout=None):
        if key is not None:
            record = {'type': 'Future', 'value': key}
            self.future_refcount[name, key] += 1
            self.scheduler.client_desires_keys(keys=[key], client='queue-%s' % name)
        else:
            record = {'type': 'msgpack', 'value': data}
        yield self.queues[name].put(record, timeout=timeout)

    def future_release(self, name=None, key=None, client=None):
        self.future_refcount[name, key] -= 1
        if self.future_refcount[name, key] == 0:
            self.scheduler.client_releases_keys(keys=[key],
                                                client='queue-%s' % name)
            del self.future_refcount[name, key]

    @gen.coroutine
    def get(self, stream=None, name=None, client=None, timeout=None):
        record = yield self.queues[name].get(timeout=timeout)
        raise gen.Return(record)

    def qsize(self, stream=None, name=None, client=None):
        return self.queues[name].qsize()


class Queue(object):
    """ Distributed Queue

    This allows multiple clients to share futures or small bits of data between
    each other with a multi-producer/multi-consumer queue.  All metadata is
    sequentialized through the scheduler.

    Elements of the Queue must be either Futures or msgpack-encodable data
    (ints, strings, lists, dicts).  All data is sent through the scheduler so
    it is wise not to send large objects.  To share large objects scatter the
    data and share the future instead.

    Examples
    --------
    >>> from dask.distributed import Client, Queue  # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> queue = Queue('x')  # doctest: +SKIP
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> queue.put(future)  # doctest: +SKIP

    See Also
    --------
    Variable: shared variable between clients
    """
    def __init__(self, name=None, client=None, maxsize=0):
        self.client = client or _get_global_client()
        self.name = name or 'queue-' + uuid.uuid4().hex
        if self.client.asynchronous:
            self._started = self.client.scheduler.queue_create(name=self.name,
                                                               maxsize=maxsize)
        else:
            sync(self.client.loop, self.client.scheduler.queue_create,
                 name=self.name, maxsize=maxsize)
            self._started = gen.moment

    def __await__(self):
        @gen.coroutine
        def _():
            yield self._started
            raise gen.Return(self)
        return _().__await__()

    @gen.coroutine
    def _put(self, value, timeout=None):
        if isinstance(value, Future):
            yield self.client.scheduler.queue_put(key=tokey(value.key),
                                                  timeout=timeout,
                                                  name=self.name)
        else:
            yield self.client.scheduler.queue_put(data=value,
                                                  timeout=timeout,
                                                  name=self.name)

    def put(self, value, timeout=None):
        """ Put data into the queue """
        return self.client.sync(self._put, value, timeout=timeout)

    def get(self, timeout=None):
        """ Get data from the queue """
        return self.client.sync(self._get, timeout=timeout)

    def qsize(self):
        """ Current number of elements in the queue """
        return self.client.sync(self._qsize)

    @gen.coroutine
    def _get(self, timeout=None):
        d = yield self.client.scheduler.queue_get(timeout=timeout, name=self.name)
        if d['type'] == 'Future':
            value = Future(d['value'], self.client, inform=True)
            self.client._send_to_scheduler({'op': 'queue-future-release',
                                            'name': self.name,
                                            'key': d['value']})
        else:
            value = d['value']
        raise gen.Return(value)

    @gen.coroutine
    def _qsize(self):
        result = yield self.client.scheduler.queue_qsize(name=self.name)
        raise gen.Return(result)

    def _release(self):
        if self.client.status == 'running':  # TODO: can leave zombie futures
            self.client._send_to_scheduler({'op': 'queue_release',
                                            'name': self.name})

    def __del__(self):
        self._release()

    def __getstate__(self):
        return (self.name, self.client.scheduler.address)

    def __setstate__(self, state):
        name, address = state
        client = _get_global_client()
        if client is None or client.scheduler.address != address:
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client)
