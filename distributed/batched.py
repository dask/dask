from collections import deque
import logging

import dask
from tornado import gen, locks
from tornado.ioloop import IOLoop

from .core import CommClosedError
from .utils import parse_timedelta


logger = logging.getLogger(__name__)


class BatchedSend:
    """Batch messages in batches on a stream

    This takes an IOStream and an interval (in ms) and ensures that we send no
    more than one message every interval milliseconds.  We send lists of
    messages.

    Batching several messages at once helps performance when sending
    a myriad of tiny messages.

    Examples
    --------
    >>> stream = yield connect(address)
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
        self._consecutive_failures = 0

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
                # If the comm is known to be closed, we'll immediately
                # give up.
                logger.info("Batched Comm Closed: %s", e)
                break
            except Exception:
                # In other cases we'll retry a few times.
                # https://github.com/pangeo-data/pangeo/issues/788
                if self._consecutive_failures <= 5:
                    logger.warning("Error in batched write, retrying")
                    yield gen.sleep(0.100 * 1.5 ** self._consecutive_failures)
                    self._consecutive_failures += 1
                    # Exponential backoff for retries.
                    # Ensure we don't drop any messages.
                    if self.buffer:
                        # Someone could call send while we yielded above?
                        self.buffer = payload + self.buffer
                    else:
                        self.buffer = payload
                    continue
                else:
                    logger.exception("Error in batched write")
                    break
            finally:
                payload = None  # lose ref
        else:
            # nobreak. We've been gracefully closed.
            self.stopped.set()
            return

        # If we've reached here, it means our comm is known to be closed or
        # we've repeatedly failed to send a message. We can't close gracefully
        # via `.close()` since we can't send messages. So we just abort.
        # This means that any messages in our buffer our lost.
        # To propagate exceptions, we rely on subsequent `BatchedSend.send`
        # calls to raise CommClosedErrors.
        self.stopped.set()
        self.abort()

    def send(self, msg):
        """Schedule a message for sending to the other side

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
    def close(self, timeout=None):
        """Flush existing messages and then close comm

        If set, raises `tornado.util.TimeoutError` after a timeout.
        """
        if self.comm is None:
            return
        self.please_stop = True
        self.waker.set()
        yield self.stopped.wait(timeout=timeout)
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
