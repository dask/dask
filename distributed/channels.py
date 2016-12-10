from __future__ import print_function, division, absolute_import

from collections import deque, defaultdict
from functools import partial
import logging
from time import sleep
import threading

from tornado.iostream import StreamClosedError

from .client import Future
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

        self.scheduler.compute_handlers.update(handlers)
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

        stream = self.scheduler.streams[client]
        for key in self.deques[channel]:
            stream.send({'op': 'channel-append',
                         'key': key,
                         'channel': channel})

        if self.stopped[channel]:
            stream.send({'op': 'channel-stop',
                         'channel': channel})

    def unsubscribe(self, channel=None, client=None):
        logger.info("Remove client from channel, %s, %s", client, channel)
        self.clients[channel].remove(client)
        if not self.clients[channel]:
            del self.deques[channel]
            del self.counts[channel]
            del self.clients[channel]
            del self.stopped[channel]

    def append(self, channel=None, key=None):
        if self.stopped[channel]:
            return

        if len(self.deques[channel]) == self.deques[channel].maxlen:
            # TODO: future might still be in deque
            self.scheduler.client_releases_keys(keys=[self.deques[channel][0]],
                                                client='streaming-%s' % channel)

        self.deques[channel].append(key)
        self.counts[channel] += 1
        self.report(channel, key)

        client='streaming-%s' % channel
        self.scheduler.client_desires_keys(keys=[key], client=client)

    def stop(self, channel=None):
        self.stopped[channel] = True
        logger.info("Stop channel %s", channel)
        for client in list(self.clients[channel]):
            try:
                stream = self.scheduler.streams[client]
                stream.send({'op': 'channel-stop',
                             'channel': channel})
            except (KeyError, StreamClosedError):
                self.unsubscribe(channel, client)

    def report(self, channel, key):
        for client in list(self.clients[channel]):
            try:
                stream = self.scheduler.streams[client]
                stream.send({'op': 'channel-append',
                             'key': key,
                             'channel': channel})
            except (KeyError, StreamClosedError):
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

    def receive_key(self, channel=None, key=None):
        self.channels[channel]._receive_update(key)

    def receive_stop(self, channel=None):
        self.channels[channel]._receive_stop()


class Channel(object):
    """
    A changing stream of futures shared between clients

    Several clients connected to the same scheduler can communicate a sequence
    of futures between each other through shared *channels*.  All clients can
    append to the channel at any time.  All clients will be updated when a
    channel updates.  The central scheduler maintains consistency and ordering
    of events.

    Examples
    --------

    Create channels from your Client:

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')

    Append futures onto a channel

    >>> future = client.submit(add, 1, 2)
    >>> chan.append(future)

    A channel maintains a collection of current futures added by both your
    client, and others.

    >>> chan.futures
    deque([<Future: status: pending, key: add-12345>,
           <Future: status: pending, key: sub-56789>])

    You can iterate over a channel to get back futures.

    >>> for future in chan:
    ...     pass
    """
    def __init__(self, client, name, maxlen=None):
        self.client = client
        self.name = name
        self.futures = deque(maxlen=maxlen)
        self.stopped = False
        self.count = 0
        self._pending = dict()
        self._lock = threading.Lock()
        self._thread_condition = threading.Condition()

        self.client._send_to_scheduler({'op': 'channel-subscribe',
                                        'channel': name,
                                        'maxlen': maxlen,
                                        'client': self.client.id})

    def append(self, future):
        """ Append a future onto the channel """
        if self.stopped:
            raise StopIteration()
        self.client._send_to_scheduler({'op': 'channel-append',
                                        'channel': self.name,
                                        'key': tokey(future.key)})
        self._pending[future.key] = future  # hold on to reference until ack

    def stop(self):
        if self.stopped:
            return
        self.client._send_to_scheduler({'op': 'channel-stop',
                                        'channel': self.name})

    def _receive_update(self, key=None):
        with self._lock:
            self.count += 1
            self.futures.append(Future(key, self.client))
        self.client._send_to_scheduler({'op': 'client-desires-keys',
                                        'keys': [key],
                                        'client': self.client.id})
        if key in self._pending:
            del self._pending[key]

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
        if not self.client.scheduler_stream.stream:
            self.client._send_to_scheduler({'op': 'channel-unsubscribe',
                                            'channel': self.name,
                                            'client': self.client.id})

    def __iter__(self):
        with log_errors():
            with self._lock:
                last = self.count
                L = list(self.futures)
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
                    n = min(self.count - last, len(self.futures))
                    L = [self.futures[i] for i in range(-n, 0)]
                    last = self.count
                for f in L:
                    yield f


    def __len__(self):
        return len(self.futures)

    def __str__(self):
        return "<Channel: %s - %d elements>" % (self.name, len(self.futures))

    __repr__ = __str__
