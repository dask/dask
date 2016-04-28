from __future__ import print_function, division, absolute_import

import logging
from timeit import default_timer

from tornado import gen
from tornado.queues import Queue
from tornado.iostream import StreamClosedError
from tornado.ioloop import PeriodicCallback, IOLoop

from .core import read, write
from .utils import log_errors


logger = logging.getLogger(__name__)


class BatchedSend(object):
    """ Batch messages in batches on a stream

    This takes an IOStream and an interval (in ms) and ensures that we send no
    more than one message every interval milliseconds.  We send lists of
    messages.

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
        self.last_transmission = 0
        self.next_send = None
        self.buffer = []
        self.stream = None
        self.last_send = gen.sleep(0)

    def start(self, stream):
        self.stream = stream
        if self.buffer:
            self.send_next()

    @gen.coroutine
    def send_next(self, wait=True):
        try:
            now = default_timer()
            if wait:
                wait_time = min(self.last_transmission + self.interval - now,
                                self.interval)
                yield gen.sleep(wait_time)
            yield self.last_send
            self.buffer, payload = [], self.buffer
            self.last_transmission = now
            self.next_send = write(self.stream, payload)
        except Exception as e:
            logger.exception(e)

    @gen.coroutine
    def _write(self, payload):
        yield gen.sleep(0)
        yield write(self.stream, payload)

    def send(self, msg):
        """ Send a message to the other side

        This completes quickly and synchronously
        """
        try:
            if self.stream is None:  # not yet started
                self.buffer.append(msg)
                return

            if self.stream._closed:
                raise StreamClosedError()

            if self.buffer:
                self.buffer.append(msg)
                return

            # If we're new and early,
            now = default_timer()
            if (now < self.last_transmission + self.interval
                or not self.last_send._done):
                self.buffer.append(msg)
                self.loop.add_callback(self.send_next)
                return

            self.buffer.append(msg)
            self.loop.add_callback(self.send_next, wait=False)
        except Exception as e:
            logger.exception(e)
            raise

    @gen.coroutine
    def close(self, ignore_closed=False):
        """ Flush existing messages and then close stream """
        try:
            yield self.last_send
            if self.buffer:
                self.buffer, payload = [], self.buffer
                yield write(self.stream, payload)
        except StreamClosedError:
            if not ignore_closed:
                raise
        self.stream.close()


class BatchedStream(object):
    """ Mostly obsolete, see BatchedSend """
    def __init__(self, stream, interval):
        self.stream = stream
        self.interval = interval / 1000.
        self.last_transmission = default_timer()
        self.send_q = Queue()
        self.recv_q = Queue()
        self._background_send_coroutine = self._background_send()
        self._background_recv_coroutine = self._background_recv()
        self._broken = None

        self.pc = PeriodicCallback(lambda: None, 100)
        self.pc.start()

    @gen.coroutine
    def _background_send(self):
        with log_errors():
            while True:
                msg = yield self.send_q.get()
                if msg == 'close':
                    break
                msgs = [msg]
                now = default_timer()
                wait_time = self.last_transmission + self.interval - now
                if wait_time > 0:
                    yield gen.sleep(wait_time)
                while not self.send_q.empty():
                    msgs.append(self.send_q.get_nowait())

                try:
                    yield write(self.stream, msgs)
                except StreamClosedError:
                    self.recv_q.put_nowait('close')
                    self._broken = True
                    break

                if len(msgs) > 1:
                    logger.debug("Batched messages: %d", len(msgs))
                for _ in msgs:
                    self.send_q.task_done()

    @gen.coroutine
    def _background_recv(self):
        with log_errors():
            while True:
                try:
                    msgs = yield read(self.stream)
                except StreamClosedError:
                    self.recv_q.put_nowait('close')
                    self.send_q.put_nowait('close')
                    self._broken = True
                    break
                assert isinstance(msgs, list)
                if len(msgs) > 1:
                    logger.debug("Batched messages: %d", len(msgs))
                for msg in msgs:
                    self.recv_q.put_nowait(msg)

    @gen.coroutine
    def flush(self):
        yield self.send_q.join()

    @gen.coroutine
    def send(self, msg):
        if self._broken:
            raise StreamClosedError('Batch Stream is Closed')
        else:
            self.send_q.put_nowait(msg)

    @gen.coroutine
    def recv(self):
        result = yield self.recv_q.get()
        if result == 'close':
            raise StreamClosedError('Batched Stream is Closed')
        else:
            raise gen.Return(result)

    @gen.coroutine
    def close(self):
        yield self.flush()
        raise gen.Return(self.stream.close())

    def closed(self):
        return self.stream.closed()
