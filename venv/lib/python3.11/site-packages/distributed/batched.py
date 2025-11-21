from __future__ import annotations

import contextlib
import logging
from collections import deque
from typing import Any

from tornado import gen, locks
from tornado.ioloop import IOLoop

import dask
from dask.utils import parse_timedelta

from distributed.core import CommClosedError
from distributed.metrics import time

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
    >>> stream = await connect(address)
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
            maxlen=dask.config.get("distributed.admin.low-level-log-length")
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
            if self.next_deadline is not None and time() < self.next_deadline:
                # Send interval not expired yet
                continue
            payload, self.buffer = self.buffer, []
            self.batch_count += 1
            self.next_deadline = time() + self.interval
            try:
                # NOTE: Since `BatchedSend` doesn't have a handle on the running
                # `_background_send` coroutine, the only thing with a reference to this
                # coroutine is the event loop itself. If the event loop stops while
                # we're waiting on a `write`, the `_background_send` coroutine object
                # may be garbage collected. If that happens, the `yield coro` will raise
                # `GeneratorExit`. But because this is an old-school `gen.coroutine`,
                # and we're using `yield` and not `await`, the `write` coroutine object
                # will not actually have been awaited, and it will remain sitting around
                # for someone to retrieve it. At interpreter exit, this will warn
                # sommething like `RuntimeWarning: coroutine 'TCP.write' was never
                # awaited`. By using the `closing` contextmanager, the `write` coroutine
                # object is always cleaned up, even if `yield` raises `GeneratorExit`.
                with contextlib.closing(
                    self.comm.write(
                        payload, serializers=self.serializers, on_error="raise"
                    )
                ) as coro:
                    nbytes = yield coro
                if nbytes < 1e6:
                    self.recent_message_log.append(payload)
                else:
                    self.recent_message_log.append("large-message")
                self.byte_count += nbytes
            except CommClosedError:
                logger.info("Batched Comm Closed %r", self.comm, exc_info=True)
                break
            except Exception:
                # We cannot safely retry self.comm.write, as we have no idea
                # what (if anything) was actually written to the underlying stream.
                # Re-writing messages could result in complete garbage (e.g. if a frame
                # header has been written, but not the frame payload), therefore
                # the only safe thing to do here is to abort the stream without
                # any attempt to re-try `write`.
                logger.exception("Error in batched write")
                break
            finally:
                payload = None  # lose ref
        else:
            # nobreak. We've been gracefully closed.
            self.stopped.set()
            return

        # If we've reached here, it means `break` was hit above and
        # there was an exception when using `comm`.
        # We can't close gracefully via `.close()` since we can't send messages.
        # So we just abort.
        # This means that any messages in our buffer our lost.
        # To propagate exceptions, we rely on subsequent `BatchedSend.send`
        # calls to raise CommClosedErrors.
        self.stopped.set()
        self.abort()

    def send(self, *msgs: Any) -> None:
        """Schedule a message for sending to the other side

        This completes quickly and synchronously
        """
        if self.comm is not None and self.comm.closed():
            raise CommClosedError(f"Comm {self.comm!r} already closed.")

        self.message_count += len(msgs)
        self.buffer.extend(msgs)
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
                    # See note in `_background_send` for explanation of `closing`.
                    with contextlib.closing(
                        self.comm.write(
                            payload, serializers=self.serializers, on_error="raise"
                        )
                    ) as coro:
                        yield coro
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
