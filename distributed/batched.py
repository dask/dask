from __future__ import print_function, division, absolute_import

from collections import deque
import logging

import dask
from tornado import gen, locks
from tornado.ioloop import IOLoop

from .core import CommClosedError
from .utils import parse_timedelta


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
    >>> bstream = BatchedSend(interval='10 ms')
    >>> bstream.start(stream)
    >>> bstream.send('Hello,')
    >>> bstream.send('world!')

    On the other side, the recipient will get a message like the following::

        ['Hello,', 'world!']
    """

    # XXX why doesn't BatchedSend follow either the IOStream or Comm API?

    def __init__(self, interval, loop=None, serializers=None):
        # XXX is the loop arg useful?
        self.loop = loop or IOLoop.current()
        self.interval = parse_timedelta(interval, default="ms")
        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.please_stop = False
        self.buffer = []
        self.comm = None
        self.message_count = 0
        self.batch_count = 0
        self.byte_count = 0
        self.next_deadline = None
        self.recent_message_log = deque(
            maxlen=dask.config.get("distributed.comm.recent-messages-log-length")
        )
        self.serializers = serializers

    def start(self, comm):
        self.comm = comm
        self.loop.add_callback(self._background_send)

    def closed(self):
        return self.comm and self.comm.closed()

    def __repr__(self):
        if self.closed():
            return "<BatchedSend: closed>"
        else:
            return "<BatchedSend: %d in buffer>" % len(self.buffer)

    __str__ = __repr__

    @gen.coroutine
    def _background_send(self):
        while not self.please_stop:
            try:
                yield self.waker.wait(self.next_deadline)
                self.waker.clear()
            except gen.TimeoutError:
                pass
            if not self.buffer:
                # Nothing to send
                self.next_deadline = None
                continue
            if self.next_deadline is not None and self.loop.time() < self.next_deadline:
                # Send interval not expired yet
                continue
            payload, self.buffer = self.buffer, []
            self.batch_count += 1
            self.next_deadline = self.loop.time() + self.interval
            try:
                nbytes = yield self.comm.write(
                    payload, serializers=self.serializers, on_error="raise"
                )
                if nbytes < 1e6:
                    self.recent_message_log.append(payload)
                else:
                    self.recent_message_log.append("large-message")
                self.byte_count += nbytes
            except CommClosedError as e:
                logger.info("Batched Comm Closed: %s", e)
                break
            except Exception:
                logger.exception("Error in batched write")
                break
            finally:
                payload = None  # lose ref

        self.stopped.set()

    def send(self, msg):
        """ Schedule a message for sending to the other side

        This completes quickly and synchronously
        """
        if self.comm is not None and self.comm.closed():
            raise CommClosedError

        self.message_count += 1
        self.buffer.append(msg)
        # Avoid spurious wakeups if possible
        if self.next_deadline is None:
            self.waker.set()

    @gen.coroutine
    def close(self):
        """ Flush existing messages and then close comm """
        if self.comm is None:
            return
        self.please_stop = True
        self.waker.set()
        yield self.stopped.wait()
        if not self.comm.closed():
            try:
                if self.buffer:
                    self.buffer, payload = [], self.buffer
                    yield self.comm.write(
                        payload, serializers=self.serializers, on_error="raise"
                    )
            except CommClosedError:
                pass
            yield self.comm.close()

    def abort(self):
        if self.comm is None:
            return
        self.please_stop = True
        self.buffer = []
        self.waker.set()
        if not self.comm.closed():
            self.comm.abort()
