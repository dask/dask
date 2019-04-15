from collections import defaultdict, deque
import datetime
import logging
import weakref

import tornado.locks
from tornado import gen

from .compatibility import finalize, get_thread_identity
from .core import CommClosedError
from .utils import sync
from .protocol.serialize import to_serialize

logger = logging.getLogger(__name__)


class PubSubSchedulerExtension(object):
    """ Extend Dask's scheduler with routes to handle PubSub machinery """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.publishers = defaultdict(set)
        self.subscribers = defaultdict(set)
        self.client_subscribers = defaultdict(set)

        self.scheduler.handlers.update({"pubsub_add_publisher": self.add_publisher})

        self.scheduler.stream_handlers.update(
            {
                "pubsub-add-subscriber": self.add_subscriber,
                "pubsub-remove-publisher": self.remove_publisher,
                "pubsub-remove-subscriber": self.remove_subscriber,
                "pubsub-msg": self.handle_message,
            }
        )

        self.scheduler.extensions["pubsub"] = self

    def add_publisher(self, comm=None, name=None, worker=None):
        logger.debug("Add publisher: %s %s", name, worker)
        self.publishers[name].add(worker)
        return {
            "subscribers": {addr: {} for addr in self.subscribers[name]},
            "publish-scheduler": name in self.client_subscribers
            and len(self.client_subscribers[name]) > 0,
        }

    def add_subscriber(self, comm=None, name=None, worker=None, client=None):
        if worker:
            logger.debug("Add worker subscriber: %s %s", name, worker)
            self.subscribers[name].add(worker)
            for pub in self.publishers[name]:
                self.scheduler.worker_send(
                    pub,
                    {"op": "pubsub-add-subscriber", "address": worker, "name": name},
                )
        elif client:
            logger.debug("Add client subscriber: %s %s", name, client)
            for pub in self.publishers[name]:
                self.scheduler.worker_send(
                    pub,
                    {"op": "pubsub-publish-scheduler", "name": name, "publish": True},
                )
            self.client_subscribers[name].add(client)

    def remove_publisher(self, comm=None, name=None, worker=None):
        if worker in self.publishers[name]:
            logger.debug("Remove publisher: %s %s", name, worker)
            self.publishers[name].remove(worker)

            if not self.subscribers[name] and not self.publishers[name]:
                del self.subscribers[name]
                del self.publishers[name]

    def remove_subscriber(self, comm=None, name=None, worker=None, client=None):
        if worker:
            logger.debug("Add worker subscriber: %s %s", name, worker)
            self.subscribers[name].remove(worker)
            for pub in self.publishers[name]:
                self.scheduler.worker_send(
                    pub,
                    {"op": "pubsub-remove-subscriber", "address": worker, "name": name},
                )
        elif client:
            logger.debug("Add client subscriber: %s %s", name, client)
            self.client_subscribers[name].remove(client)
            if not self.client_subscribers[name]:
                del self.client_subscribers[name]
                for pub in self.publishers[name]:
                    self.scheduler.worker_send(
                        pub,
                        {
                            "op": "pubsub-publish-scheduler",
                            "name": name,
                            "publish": False,
                        },
                    )

        if not self.subscribers[name] and not self.publishers[name]:
            logger.debug("Remove PubSub topic %s", name)
            del self.subscribers[name]
            del self.publishers[name]

    def handle_message(self, name=None, msg=None, worker=None, client=None):
        for c in list(self.client_subscribers[name]):
            try:
                self.scheduler.client_comms[c].send(
                    {"op": "pubsub-msg", "name": name, "msg": msg}
                )
            except (KeyError, CommClosedError):
                self.remove_subscriber(name=name, client=c)

        if client:
            for sub in self.subscribers[name]:
                self.scheduler.worker_send(
                    sub, {"op": "pubsub-msg", "name": name, "msg": msg}
                )


class PubSubWorkerExtension(object):
    """ Extend Dask's Worker with routes to handle PubSub machinery """

    def __init__(self, worker):
        self.worker = worker
        self.worker.stream_handlers.update(
            {
                "pubsub-add-subscriber": self.add_subscriber,
                "pubsub-remove-subscriber": self.remove_subscriber,
                "pubsub-msg": self.handle_message,
                "pubsub-publish-scheduler": self.publish_scheduler,
            }
        )

        self.subscribers = defaultdict(weakref.WeakSet)
        self.publishers = defaultdict(weakref.WeakSet)
        self.publish_to_scheduler = defaultdict(lambda: False)

        self.worker.extensions["pubsub"] = self  # circular reference

    def add_subscriber(self, name=None, address=None, **info):
        for pub in self.publishers[name]:
            pub.subscribers[address] = info

    def remove_subscriber(self, name=None, address=None):
        for pub in self.publishers[name]:
            del pub.subscribers[address]

    def publish_scheduler(self, name=None, publish=None):
        self.publish_to_scheduler[name] = publish

    def handle_message(self, name=None, msg=None):
        for sub in self.subscribers.get(name, []):
            sub._put(msg)

    def trigger_cleanup(self):
        self.worker.loop.add_callback(self.cleanup)

    def cleanup(self):
        for name, s in dict(self.subscribers).items():
            if not len(s):
                msg = {"op": "pubsub-remove-subscriber", "name": name}
                self.worker.batched_stream.send(msg)
                del self.subscribers[name]

        for name, p in dict(self.publishers).items():
            if not len(p):
                msg = {"op": "pubsub-remove-publisher", "name": name}
                self.worker.batched_stream.send(msg)
                del self.publishers[name]
                del self.publish_to_scheduler[name]


class PubSubClientExtension(object):
    """ Extend Dask's Client with handlers to handle PubSub machinery """

    def __init__(self, client):
        self.client = client
        self.client._stream_handlers.update({"pubsub-msg": self.handle_message})

        self.subscribers = defaultdict(weakref.WeakSet)
        self.client.extensions["pubsub"] = self  # TODO: circular reference

    def handle_message(self, name=None, msg=None):
        for sub in self.subscribers[name]:
            sub._put(msg)

        if not self.subscribers[name]:
            self.client.scheduler_comm.send(
                {"op": "pubsub-remove-subscribers", "name": name}
            )

    def trigger_cleanup(self):
        self.client.loop.add_callback(self.cleanup)

    def cleanup(self):
        for name, s in self.subscribers.items():
            if not s:
                msg = {"op": "pubsub-remove-subscriber", "name": name}
                self.client.scheduler_comm.send(msg)


class Pub(object):
    """ Publish data with Publish-Subscribe pattern

    This allows clients and workers to directly communicate data between each
    other with a typical Publish-Subscribe pattern.  This involves two
    components,

    Pub objects, into which we put data:

        >>> pub = Pub('my-topic')
        >>> pub.put(123)

    And Sub objects, from which we collect data:

        >>> sub = Sub('my-topic')
        >>> sub.get()
        123

    Many Pub and Sub objects can exist for the same topic.  All data sent from
    any Pub will be sent to all Sub objects on that topic that are currently
    connected.  Pub's and Sub's find each other using the scheduler, but they
    communicate directly with each other without coordination from the
    scheduler.

    Pubs and Subs use the central scheduler to find each other, but not to
    mediate the communication.  This means that there is very little additional
    latency or overhead, and they are appropriate for very frequent data
    transfers.  For context, most data transfer first checks with the scheduler to find which
    workers should participate, and then does direct worker-to-worker
    transfers.  This checking in with the scheduler provides some stability
    guarantees, but also adds in a few extra network hops.  PubSub doesn't do
    this, and so is faster, but also can easily drop messages if Pubs or Subs
    disappear without notice.

    When using a Pub or Sub from a Client all communications will be routed
    through the scheduler.  This can cause some performance degredation.  Pubs
    an Subs only operate at top-speed when they are both on workers.

    Parameters
    ----------
    name: object (msgpack serializable)
        The name of the group of Pubs and Subs on which to participate

    Examples
    --------
    >>> pub = Pub('my-topic')
    >>> sub = Sub('my-topic')
    >>> pub.put([1, 2, 3])
    >>> sub.get()
    [1, 2, 3]

    You can also use sub within a for loop:

    >>> for msg in sub:  # doctest: +SKIP
    ...     print(msg)

    or an async for loop

    >>> async for msg in sub:  # doctest: +SKIP
    ...     print(msg)

    Similarly the ``.get`` method will return an awaitable if used by an async
    client or within the IOLoop thread of a worker

    >>> await sub.get()  # doctest: +SKIP

    You can see the set of connected worker subscribers by looking at the
    ``.subscribers`` attribute:

    >>> pub.subscribers
    {'tcp://...': {},
     'tcp://...': {}}

    See Also
    --------
    Sub
    """

    def __init__(self, name, worker=None, client=None):
        if worker is None and client is None:
            from distributed import get_worker, get_client

            try:
                worker = get_worker()
            except Exception:
                client = get_client()

        self.subscribers = dict()
        self.worker = worker
        self.client = client
        assert client or worker
        if self.worker:
            self.scheduler = self.worker.scheduler
            self.loop = self.worker.loop
        elif self.client:
            self.scheduler = self.client.scheduler
            self.loop = self.client.loop

        self.name = name
        self._started = False
        self._buffer = []

        self.loop.add_callback(self._start)

        if self.worker:
            pubsub = self.worker.extensions["pubsub"]
            self.loop.add_callback(pubsub.publishers[name].add, self)
            finalize(self, pubsub.trigger_cleanup)

    @gen.coroutine
    def _start(self):
        if self.worker:
            result = yield self.scheduler.pubsub_add_publisher(
                name=self.name, worker=self.worker.address
            )
            pubsub = self.worker.extensions["pubsub"]
            self.subscribers.update(result["subscribers"])
            pubsub.publish_to_scheduler[self.name] = result["publish-scheduler"]

        self._started = True

        for msg in self._buffer:
            self.put(msg)
        del self._buffer[:]

    def _put(self, msg):
        if not self._started:
            self._buffer.append(msg)
            return

        data = {"op": "pubsub-msg", "name": self.name, "msg": to_serialize(msg)}

        if self.worker:
            for sub in self.subscribers:
                self.worker.send_to_worker(sub, data)

            if self.worker.extensions["pubsub"].publish_to_scheduler[self.name]:
                self.worker.batched_stream.send(data)
        elif self.client:
            self.client.scheduler_comm.send(data)

    def put(self, msg):
        """ Publish a message to all subscribers of this topic """
        self.loop.add_callback(self._put, msg)


class Sub(object):
    """ Subscribe to a Publish/Subscribe topic

    See Also
    --------
    Pub: for full docstring
    """

    def __init__(self, name, worker=None, client=None):
        if worker is None and client is None:
            from distributed.worker import get_worker, get_client

            try:
                worker = get_worker()
            except Exception:
                client = get_client()

        self.worker = worker
        self.client = client
        if self.worker:
            self.loop = self.worker.loop
        elif self.client:
            self.loop = self.client.loop
        self.name = name
        self.buffer = deque()
        self.condition = tornado.locks.Condition()

        if self.worker:
            pubsub = self.worker.extensions["pubsub"]
        elif self.client:
            pubsub = self.client.extensions["pubsub"]
        self.loop.add_callback(pubsub.subscribers[name].add, self)

        msg = {"op": "pubsub-add-subscriber", "name": self.name}
        if self.worker:
            self.loop.add_callback(self.worker.batched_stream.send, msg)
        elif self.client:
            self.loop.add_callback(self.client.scheduler_comm.send, msg)
        else:
            raise Exception()

        finalize(self, pubsub.trigger_cleanup)

    @gen.coroutine
    def _get(self, timeout=None):
        if timeout is not None:
            timeout = datetime.timedelta(seconds=timeout)
        start = datetime.datetime.now()
        while not self.buffer:
            if timeout is not None:
                timeout2 = timeout - (datetime.datetime.now() - start)
                if timeout2.total_seconds() < 0:
                    raise gen.TimeoutError()
            else:
                timeout2 = None
            yield self.condition.wait(timeout=timeout2)

        raise gen.Return(self.buffer.popleft())

    __anext__ = _get

    def get(self, timeout=None):
        """ Get a single message """
        if self.client:
            return self.client.sync(self._get, timeout=timeout)
        elif self.worker.thread_id == get_thread_identity():
            return self._get()
        else:
            if self.buffer:  # fastpath
                return self.buffer.popleft()
            return sync(self.loop, self._get, timeout=timeout)

    next = __next__ = get

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def _put(self, msg):
        self.buffer.append(msg)
        self.condition.notify()
