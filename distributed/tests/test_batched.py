
from contextlib import contextmanager
from datetime import timedelta
import random

import pytest
from toolz import first, assoc
from tornado import gen
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.iostream import StreamClosedError

from distributed.batched import BatchedSend
from distributed.core import read, write
from distributed.metrics import time
from distributed.utils import sync, All
from distributed.utils_test import gen_test, slow, gen_cluster


class EchoServer(TCPServer):
    count = 0
    @gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                msg = yield read(stream)
                self.count += 1
                yield write(stream, msg)
            except StreamClosedError as e:
                return

    def listen(self, port=0):
        while True:
            try:
                super(EchoServer, self).listen(port)
                break
            except OSError as e:
                if port:
                    raise
                else:
                    pass
        self.port = first(self._sockets.values()).getsockname()[1]



@contextmanager
def echo_server():
    server = EchoServer()
    server.listen(0)

    try:
        yield server
    finally:
        server.stop()


@gen_test()
def test_BatchedSend():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)
        assert str(len(b.buffer)) in str(b)
        assert str(len(b.buffer)) in repr(b)
        b.start(stream)

        yield gen.sleep(0.020)

        b.send('hello')
        b.send('hello')
        b.send('world')
        yield gen.sleep(0.020)
        b.send('HELLO')
        b.send('HELLO')

        result = yield read(stream); assert result == ['hello', 'hello', 'world']
        result = yield read(stream); assert result == ['HELLO', 'HELLO']

        assert b.byte_count > 1


@gen_test()
def test_send_before_start():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)

        b.send('hello')
        b.send('world')

        b.start(stream)
        result = yield read(stream); assert result == ['hello', 'world']


@gen_test()
def test_send_after_stream_start():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)

        b.start(stream)
        b.send('hello')
        b.send('world')
        result = yield read(stream)
        if len(result) < 2:
            result += yield read(stream)
        assert result == ['hello', 'world']


@gen_test()
def test_send_before_close():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)
        b.start(stream)

        cnt = int(e.count)
        b.send('hello')
        yield b.close()         # close immediately after sending
        assert not b.buffer

        start = time()
        while e.count != cnt + 1:
            yield gen.sleep(0.01)
            assert time() < start + 5

        with pytest.raises(StreamClosedError):
            b.send('123')


@gen_test()
def test_close_closed():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)
        b.start(stream)

        b.send(123)
        stream.close()  # external closing

        yield b.close(ignore_closed=True)


@gen_test()
def test_close_not_started():
    b = BatchedSend(interval=10)
    yield b.close()


@gen_test()
def test_close_twice():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=10)
        b.start(stream)
        yield b.close()
        yield b.close()


@slow
@gen_test(timeout=50)
def test_stress():
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)
        L = []

        @gen.coroutine
        def send():
            b = BatchedSend(interval=3)
            b.start(stream)
            for i in range(0, 10000, 2):
                b.send(i)
                b.send(i + 1)
                yield gen.sleep(0.00001 * random.randint(1, 10))

        @gen.coroutine
        def recv():
            while True:
                result = yield gen.with_timeout(timedelta(seconds=1), read(stream))
                L.extend(result)
                if result[-1] == 9999:
                    break

        yield All([send(), recv()])

        assert L == list(range(0, 10000, 1))
        stream.close()


@gen.coroutine
def _run_traffic_jam(nsends, nbytes):
    # This test eats `nsends * nbytes` bytes in RAM
    np = pytest.importorskip('numpy')
    from distributed.protocol import to_serialize
    data = bytes(np.random.randint(0, 255, size=(nbytes,)).astype('u1').data)
    with echo_server() as e:
        client = TCPClient()
        stream = yield client.connect('127.0.0.1', e.port)

        b = BatchedSend(interval=0.01)
        b.start(stream)

        msg = {'x': to_serialize(data)}
        for i in range(nsends):
            b.send(assoc(msg, 'i', i))
            if np.random.random() > 0.5:
                yield gen.sleep(0.001)

        results = []
        count = 0
        while len(results) < nsends:
            # If this times out then I think it's a backpressure issue
            # Somehow we're able to flood the socket so that the receiving end
            # loses some of our messages
            L = yield gen.with_timeout(timedelta(seconds=5), read(stream))
            count += 1
            results.extend(r['i'] for r in L)

        assert count == b.batch_count == e.count
        assert b.message_count == nsends

        assert results == list(range(nsends))

        stream.close()  # external closing
        yield b.close(ignore_closed=True)


@gen_test()
def test_sending_traffic_jam():
    yield _run_traffic_jam(50, 300000)


@slow
@gen_test()
def test_large_traffic_jam():
    yield _run_traffic_jam(500, 1500000)
