from __future__ import print_function, division, absolute_import

import logging
from timeit import default_timer

from toolz import partition_all
from tornado import gen
from tornado.queues import Queue
from tornado.iostream import StreamClosedError, IOStream
from tornado.ioloop import PeriodicCallback, IOLoop

from .core import read, write, coerce_to_address, connect
from .utils import log_errors


logger = logging.getLogger(__name__)


class BatchedSend(object):
    """ Batch messages on a stream

    Like a one-sided BatchedStream, but faster, because sometimes Queues are
    too slow.
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
        with log_errors(pdb=True):
            self.stream = stream
            if self.buffer:
                self.send_next()

    @gen.coroutine
    def send_next(self, wait=True):
        with log_errors():
            now = default_timer()
            if wait:
                wait_time = min(self.last_transmission + self.interval - now,
                                self.interval)
                yield gen.sleep(wait_time)
            yield self.last_send
            self.buffer, payload = [], self.buffer
            self.last_transmission = now
            future = write(self.stream, payload)
            self.next_send = future

    @gen.coroutine
    def _write(self, payload):
        yield gen.sleep(0)
        with log_errors():
            yield write(self.stream, payload)


    def send(self, msg):
        with log_errors():
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

    @gen.coroutine
    def close(self):
        yield self.last_send
        if self.buffer:
            self.buffer, payload = [], self.buffer
            yield write(self.stream, payload)
        self.stream.close()


class BatchedStream(object):
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
