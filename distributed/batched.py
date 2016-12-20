from __future__ import print_function, division, absolute_import

from datetime import timedelta
from functools import partial
import logging
from timeit import default_timer

from tornado import gen, locks
from tornado.queues import Queue
from tornado.iostream import StreamClosedError
from tornado.ioloop import PeriodicCallback, IOLoop

from .core import read, write, close
from .utils import ignoring, log_errors


logger = logging.getLogger(__name__)


class BatchedSend(object):
    """ Batch messages in batches on a stream

    This takes an IOStream and an interval (in ms) and ensures that we send no
    more than one message every interval milliseconds.  We send lists of
    messages.

    Batching several messages at once helps performance when sending
    a myriad of tiny messages.

    Example
    -------
    >>> stream = yield connect(ip, port)
    >>> bstream = BatchedSend(interval=10)  # 10 ms
    >>> bstream.start(stream)
    >>> bstream.send('Hello,')
    >>> bstream.send('world!')

    On the other side, the recipient will get a message like the following::

        ['Hello,', 'world!']
    """
    def __init__(self, interval, loop=None):
        self.loop = loop or IOLoop.current()
        self.interval = interval / 1000.

        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.please_stop = False
        self.buffer = []
        self.stream = None
        self.message_count = 0
        self.batch_count = 0
        self.byte_count = 0
        self.next_deadline = None

    def start(self, stream):
        self.stream = stream
        self.loop.add_callback(self._background_send)

    def __str__(self):
        return '<BatchedSend: %d in buffer>' % len(self.buffer)

    __repr__ = __str__

    @gen.coroutine
    def _background_send(self):
        while not self.please_stop:
            with ignoring(gen.TimeoutError):
                yield self.waker.wait(self.next_deadline)
                self.waker.clear()
            if not self.buffer:
                # Nothing to send
                self.next_deadline = None
                continue
            if (self.next_deadline is not None and
                self.loop.time() < self.next_deadline):
                # Send interval not expired yet
                continue
            payload, self.buffer = self.buffer, []
            self.batch_count += 1
            self.next_deadline = self.loop.time() + self.interval
            try:
                nbytes = yield write(self.stream, payload)
                self.byte_count += nbytes
            except StreamClosedError:
                logger.info("Batched Stream Closed")
                break
            except Exception:
                logger.exception("Error in batched write")
                break

        self.stopped.set()

    def send(self, msg):
        """ Schedule a message for sending to the other side

        This completes quickly and synchronously
        """
        if self.stream is not None and self.stream._closed:
            raise StreamClosedError()

        self.message_count += 1
        self.buffer.append(msg)
        # Avoid spurious wakeups if possible
        if self.next_deadline is None:
            self.waker.set()

    @gen.coroutine
    def close(self, ignore_closed=False):
        """ Flush existing messages and then close stream """
        if self.stream is None:
            return
        self.please_stop = True
        self.waker.set()
        yield self.stopped.wait()
        try:
            if self.buffer:
                self.buffer, payload = [], self.buffer
                yield write(self.stream, payload)
        except StreamClosedError:
            if not ignore_closed:
                raise
        yield close(self.stream)
