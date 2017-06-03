from __future__ import print_function, division, absolute_import

from collections import deque
import logging
from time import sleep
import threading
import warnings

from .client import Future
from .core import CommClosedError
from .utils import tokey, log_errors

logger = logging.getLogger(__name__)


class ChannelScheduler(object):
    """ A plugin for the scheduler to manage channels

    This adds the following routes to the scheduler

    *  channel-subscribe
    *  channel-unsubsribe
    *  channel-append
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.deques = dict()
        self.counts = dict()
        self.clients = dict()
        self.stopped = dict()

        handlers = {'channel-subscribe': self.subscribe,
                    'channel-unsubscribe': self.unsubscribe,
                    'channel-append': self.append,
                    'channel-stop': self.stop}

        self.scheduler.client_handlers.update(handlers)
        self.scheduler.extensions['channels'] = self

    def subscribe(self, channel=None, client=None, maxlen=None):
        logger.info("Add new client to channel, %s, %s", client, channel)
        if channel not in self.deques:
            logger.info("Add new channel %s", channel)
            self.deques[channel] = deque(maxlen=maxlen)
            self.counts[channel] = 0
            self.clients[channel] = set()
            self.stopped[channel] = False
        self.clients[channel].add(client)

        comm = self.scheduler.comms[client]
        for type, value in self.deques[channel]:
            comm.send({'op': 'channel-append',
                       'type': type,
                       'value': value,
                       'channel': channel})

        if self.stopped[channel]:
            comm.send({'op': 'channel-stop',
                       'channel': channel})

    def unsubscribe(self, channel=None, client=None):
        logger.info("Remove client from channel, %s, %s", client, channel)
        self.clients[channel].remove(client)
        if not self.clients[channel]:
            del self.deques[channel]
            del self.counts[channel]
            del self.clients[channel]
            del self.stopped[channel]

    def append(self, channel=None, type=None, value=None, client=None):
        if self.stopped[channel]:
            return

        if len(self.deques[channel]) == self.deques[channel].maxlen:
            # TODO: future might still be in deque
            typ, val = self.deques[channel].popleft()
            if typ == 'Future':
                self.scheduler.client_releases_keys(keys=[val],
                                                    client='streaming-%s' % channel)

        self.deques[channel].append((type, value))
        self.counts[channel] += 1
        self.report(channel, type, value)

        client = 'streaming-%s' % channel
        if type == 'Future':
            self.scheduler.client_desires_keys(keys=[value], client=client)

    def stop(self, channel=None, client=None):
        self.stopped[channel] = True
        logger.info("Stop channel %s", channel)
        for client in list(self.clients[channel]):
            try:
                comm = self.scheduler.comms[client]
                comm.send({'op': 'channel-stop',
                           'channel': channel})
            except (KeyError, CommClosedError):
                self.unsubscribe(channel, client)

    def report(self, channel, type, value):
        for client in list(self.clients[channel]):
            try:
                comm = self.scheduler.comms[client]
                comm.send({'op': 'channel-append',
                           'type': type,
                           'value': value,
                           'channel': channel})
            except (KeyError, CommClosedError):
                self.unsubscribe(channel, client)


class ChannelClient(object):
    def __init__(self, client):
        self.client = client
        self.channels = dict()
        self.client.extensions['channels'] = self

        handlers = {'channel-append': self.receive_key,
                    'channel-stop': self.receive_stop}

        self.client._handlers.update(handlers)

        self.client.channel = self._create_channel  # monkey patch

    def _create_channel(self, channel, maxlen=None):
        if channel not in self.channels:
            c = Channel(self.client, channel, maxlen=maxlen)
            self.channels[channel] = c
            return c
        else:
            return self.channels[channel]

    def receive_key(self, channel=None, type=None, value=None):
        if channel not in self.channels:
            self._create_channel(channel)
        self.channels[channel]._receive_update(type, value)

    def receive_stop(self, channel=None):
        self.channels[channel]._receive_stop()


class Channel(object):
    """
    A changing stream of futures or data shared between clients

    Several clients connected to the same scheduler can communicate a sequence
    of data or futures between each other through shared *channels*.  All
    clients can append to the channel at any time.  All clients will be updated
    when a channel updates.  The central scheduler maintains consistency and
    ordering of events.

    Channels can contain Future objects or generic data.  All data should be
    small and msgpack encodable (strings, numbers, lists, dicts.)  Channels
    should not be used to send large datasets directly.  Instead scatter the
    data into a Future and send that instead.

    Examples
    --------

    Create channels from your Client:

    >>> client = Client('scheduler-address:8786')  # doctest: +SKIP
    >>> chan = client.channel('my-channel')  # doctest: +SKIP

    Append futures onto a channel

    >>> future = client.submit(add, 1, 2)  # doctest: +SKIP
    >>> chan.append(future)  # doctest: +SKIP

    A channel maintains a collection of current futures added by both your
    client, and others.

    >>> chan.data  # doctest: +SKIP
    deque([<Future: status: pending, key: add-12345>,
           <Future: status: pending, key: sub-56789>])

    You can iterate over a channel to get back futures.

    >>> for future in chan:  # doctest: +SKIP
    ...     pass

    You can send small and simple data as well

    >>> chan.append({'score': 123})  # doctest: +SKIP

    To publish large amounts of data, scatter the data into a future

    >>> [future] = client.scatter([large_numpy_array])  # doctest: +SKIP
    >>> chan.append(future)  # doctest: +SKIP
    """
    def __init__(self, client, name, maxlen=None):
        self.client = client
        self.name = name
        self.data = deque(maxlen=maxlen)
        self.stopped = False
        self.count = 0
        self._pending = dict()
        self._lock = threading.Lock()
        self._thread_condition = threading.Condition()

        self.client._send_to_scheduler({'op': 'channel-subscribe',
                                        'channel': name,
                                        'maxlen': maxlen,
                                        'client': self.client.id})

    @property
    def futures(self):
        warnings.warn("The .futures attribute has moved to .data")
        return self.data

    def append(self, value):
        """ Append a future onto the channel """
        if self.stopped:
            raise StopIteration()
        if isinstance(value, Future):
            msg = {'op': 'channel-append',
                   'channel': self.name,
                   'type': 'Future',
                   'value': tokey(value.key)}
            self._pending[value.key] = value  # hold on to reference until ack
        else:
            msg = {'op': 'channel-append',
                   'channel': self.name,
                   'type': 'value',
                   'value': value}

        self.client._send_to_scheduler(msg)

    def stop(self):
        if self.stopped:
            return
        self.client._send_to_scheduler({'op': 'channel-stop',
                                        'channel': self.name})

    def _receive_update(self, type=None, value=None):
        with self._lock:
            self.count += 1
            if type == 'Future':
                self.data.append(Future(value, self.client, inform=True))
            else:
                self.data.append(value)
        if type == 'Future':
            if value in self._pending:
                del self._pending[value]

        with self._thread_condition:
            self._thread_condition.notify_all()

    def _receive_stop(self):
        logger.info("Channel stopped: %s", self.name)
        self.stopped = True
        with self._thread_condition:
            self._thread_condition.notify_all()

    def flush(self):
        """
        Wait for acknowledgement from the scheduler on any pending futures
        """
        while self._pending:
            sleep(0.01)

    def __del__(self):
        if not self.client.scheduler_comm.comm:
            self.client._send_to_scheduler({'op': 'channel-unsubscribe',
                                            'channel': self.name,
                                            'client': self.client.id})

    def __iter__(self):
        with log_errors():
            with self._lock:
                last = self.count
                L = list(self.data)
            for future in L:
                yield future

            while True:
                while self.count == last:
                    if self.stopped:
                        return
                    self._thread_condition.acquire()
                    self._thread_condition.wait()
                    self._thread_condition.release()

                with self._lock:
                    n = min(self.count - last, len(self.data))
                    L = [self.data[i] for i in range(-n, 0)]
                    last = self.count
                for f in L:
                    yield f

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return "<Channel: %s - %d elements>" % (self.name, len(self.data))

    __repr__ = __str__
