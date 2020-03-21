import asyncio
import random

import pytest
from tlz import assoc

from distributed.batched import BatchedSend
from distributed.core import listen, connect, CommClosedError
from distributed.metrics import time
from distributed.utils import All, TimeoutError
from distributed.utils_test import captured_logger
from distributed.protocol import to_serialize


class EchoServer:
    count = 0

    async def handle_comm(self, comm):
        while True:
            try:
                msg = await comm.read()
                self.count += 1
                await comm.write(msg)
            except CommClosedError as e:
                return

    async def listen(self):
        listener = await listen("", self.handle_comm)
        self.address = listener.contact_address
        self.stop = listener.stop

    async def __aenter__(self):
        await self.listen()
        return self

    async def __aexit__(self, exc, typ, tb):
        self.stop()


@pytest.mark.asyncio
async def test_BatchedSend():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)
        assert str(len(b.buffer)) in str(b)
        assert str(len(b.buffer)) in repr(b)
        b.start(comm)

        await asyncio.sleep(0.020)

        b.send("hello")
        b.send("hello")
        b.send("world")
        await asyncio.sleep(0.020)
        b.send("HELLO")
        b.send("HELLO")

        result = await comm.read()
        assert result == ("hello", "hello", "world")
        result = await comm.read()
        assert result == ("HELLO", "HELLO")

        assert b.byte_count > 1


@pytest.mark.asyncio
async def test_send_before_start():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)

        b.send("hello")
        b.send("world")

        b.start(comm)
        result = await comm.read()
        assert result == ("hello", "world")


@pytest.mark.asyncio
async def test_send_after_stream_start():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)

        b.start(comm)
        b.send("hello")
        b.send("world")
        result = await comm.read()
        if len(result) < 2:
            result += await comm.read()
        assert result == ("hello", "world")


@pytest.mark.asyncio
async def test_send_before_close():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)

        cnt = int(e.count)
        b.send("hello")
        await b.close()  # close immediately after sending
        assert not b.buffer

        start = time()
        while e.count != cnt + 1:
            await asyncio.sleep(0.01)
            assert time() < start + 5

        with pytest.raises(CommClosedError):
            b.send("123")


@pytest.mark.asyncio
async def test_close_closed():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)

        b.send(123)
        await comm.close()  # external closing

        await b.close()
        assert "closed" in repr(b)
        assert "closed" in str(b)


@pytest.mark.asyncio
async def test_close_not_started():
    b = BatchedSend(interval=10)
    await b.close()


@pytest.mark.asyncio
async def test_close_twice():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)
        await b.close()
        await b.close()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_stress():
    async with EchoServer() as e:
        comm = await connect(e.address)
        L = []

        async def send():
            b = BatchedSend(interval=3)
            b.start(comm)
            for i in range(0, 10000, 2):
                b.send(i)
                b.send(i + 1)
                await asyncio.sleep(0.00001 * random.randint(1, 10))

        async def recv():
            while True:
                result = await asyncio.wait_for(comm.read(), 1)
                L.extend(result)
                if result[-1] == 9999:
                    break

        await All([send(), recv()])

        assert L == list(range(0, 10000, 1))
        await comm.close()


async def run_traffic_jam(nsends, nbytes):
    # This test eats `nsends * nbytes` bytes in RAM
    np = pytest.importorskip("numpy")
    from distributed.protocol import to_serialize

    data = bytes(np.random.randint(0, 255, size=(nbytes,)).astype("u1").data)
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval=0.01)
        b.start(comm)

        msg = {"x": to_serialize(data)}
        for i in range(nsends):
            b.send(assoc(msg, "i", i))
            if np.random.random() > 0.5:
                await asyncio.sleep(0.001)

        results = []
        count = 0
        while len(results) < nsends:
            # If this times out then I think it's a backpressure issue
            # Somehow we're able to flood the socket so that the receiving end
            # loses some of our messages
            L = await asyncio.wait_for(comm.read(), 5)
            count += 1
            results.extend(r["i"] for r in L)

        assert count == b.batch_count == e.count
        assert b.message_count == nsends

        assert results == list(range(nsends))

        await comm.close()  # external closing
        await b.close()


@pytest.mark.asyncio
async def test_sending_traffic_jam():
    await run_traffic_jam(50, 300000)


@pytest.mark.slow
@pytest.mark.asyncio
async def test_large_traffic_jam():
    await run_traffic_jam(500, 1500000)


@pytest.mark.asyncio
async def test_serializers():
    async with EchoServer() as e:
        comm = await connect(e.address)

        b = BatchedSend(interval="10ms", serializers=["msgpack"])
        b.start(comm)

        b.send({"x": to_serialize(123)})
        b.send({"x": to_serialize("hello")})
        await asyncio.sleep(0.100)

        b.send({"x": to_serialize(lambda x: x + 1)})

        with captured_logger("distributed.protocol") as sio:
            await asyncio.sleep(0.100)

        value = sio.getvalue()
        assert "serialize" in value
        assert "type" in value
        assert "function" in value

        msg = await comm.read()
        assert list(msg) == [{"x": 123}, {"x": "hello"}]

        with pytest.raises(TimeoutError):
            msg = await asyncio.wait_for(comm.read(), 0.1)
