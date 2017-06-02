# flake8: noqa
import pytest

asyncio = pytest.importorskip('asyncio')

import functools
from time import time
from operator import add
from toolz import isdistinct
from concurrent.futures import CancelledError
from distributed.utils_test import slow
from distributed.utils_test import slowinc

from tornado.ioloop import IOLoop
from tornado.platform.asyncio import BaseAsyncIOLoop

from distributed.asyncio import AioClient, AioFuture, as_completed, wait
from distributed.utils_test import inc, div


def coro_test(fn):
    assert asyncio.iscoroutinefunction(fn)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        loop = None
        try:
            IOLoop.clear_current()
            loop = asyncio.new_event_loop()
            loop.run_until_complete(fn(*args, **kwargs))
        finally:
            if loop is not None:
                loop.close()

            IOLoop.clear_current()
            asyncio.set_event_loop(None)

    return wrapper


@coro_test
async def test_asyncio_start_shutdown():
    c = await AioClient(processes=False)

    assert c.status == 'running'
    # AioClient has installed its AioLoop shim.
    assert isinstance(IOLoop.current(instance=False), BaseAsyncIOLoop)

    result = await c.submit(inc, 10)
    assert result == 11

    await c.shutdown()
    assert c.status == 'closed'
    assert IOLoop.current(instance=False) is None


@coro_test
async def test_asyncio_submit():
    async with AioClient(processes=False) as c:
        x = c.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, AioFuture)
        assert x.client is c

        result = await x.result()
        assert result == 11
        assert x.done()

        y = c.submit(inc, 20)
        z = c.submit(add, x, y)

        result = await z.result()
        assert result == 11 + 21


@coro_test
async def test_asyncio_future_await():
    async with AioClient(processes=False) as c:
        x = c.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, AioFuture)
        assert x.client is c

        result = await x
        assert result == 11
        assert x.done()

        y = c.submit(inc, 20)
        z = c.submit(add, x, y)

        result = await z
        assert result == 11 + 21


@coro_test
async def test_asyncio_map():
    async with AioClient(processes=False) as c:
        L1 = c.map(inc, range(5))
        assert len(L1) == 5
        assert isdistinct(x.key for x in L1)
        assert all(isinstance(x, AioFuture) for x in L1)

        result = await L1[0]
        assert result == inc(0)

        L2 = c.map(inc, L1)

        result = await L2[1]
        assert result == inc(inc(1))

        total = c.submit(sum, L2)
        result = await total
        assert result == sum(map(inc, map(inc, range(5))))

        L3 = c.map(add, L1, L2)
        result = await L3[1]
        assert result == inc(1) + inc(inc(1))

        L4 = c.map(add, range(3), range(4))
        results = await c.gather(L4)
        assert results == list(map(add, range(3), range(4)))

        def f(x, y=10):
            return x + y

        L5 = c.map(f, range(5), y=5)
        results = await c.gather(L5)
        assert results == list(range(5, 10))

        y = c.submit(f, 10)
        L6 = c.map(f, range(5), y=y)
        results = await c.gather(L6)
        assert results == list(range(20, 25))


@coro_test
async def test_asyncio_gather():
    async with AioClient(processes=False) as c:
        x = c.submit(inc, 10)
        y = c.submit(inc, x)

        result = await c.gather(x)
        assert result == 11
        result = await c.gather([x])
        assert result == [11]
        result = await c.gather({'x': x, 'y': [y]})
        assert result == {'x': 11, 'y': [12]}


@coro_test
async def test_asyncio_get():
    async with AioClient(processes=False) as c:
        result = await c.get({'x': (inc, 1)}, 'x')
        assert result == 2

        result = await c.get({'x': (inc, 1)}, ['x'])
        assert result == [2]

        result = await c.get({}, [])
        assert result == []

        result = await c.get({('x', 1): (inc, 1), ('x', 2): (inc, ('x', 1))},
                              ('x', 2))
        assert result == 3


@coro_test
async def test_asyncio_exceptions():
    async with AioClient(processes=False) as c:
        result = await c.submit(div, 1, 2)
        assert result == 1 / 2

        with pytest.raises(ZeroDivisionError):
            result = await c.submit(div, 1, 0)

        result = await c.submit(div, 10, 2)  # continues to operate
        assert result == 10 / 2


@coro_test
async def test_asyncio_channels():
    async with AioClient(processes=False) as c:
        x = c.channel('x')
        y = c.channel('y')

        assert len(x) == 0

        while set(c.extensions['channels'].channels) != {'x', 'y'}:
            await asyncio.sleep(0.01)

        xx = c.channel('x')
        yy = c.channel('y')

        assert len(x) == 0

        await asyncio.sleep(0.1)
        assert set(c.extensions['channels'].channels) == {'x', 'y'}

        future = c.submit(inc, 1)

        x.append(future)

        while not x.data:
            await asyncio.sleep(0.01)

        assert len(x) == 1

        assert xx.data[0].key == future.key

        xxx = c.channel('x')
        while not xxx.data:
            await asyncio.sleep(0.01)

        assert xxx.data[0].key == future.key

        assert 'x' in repr(x)
        assert '1' in repr(x)


@coro_test
async def test_asyncio_exception_on_exception():
    async with AioClient(processes=False) as c:
        x = c.submit(lambda: 1 / 0)
        y = c.submit(inc, x)

        with pytest.raises(ZeroDivisionError):
            await y

        z = c.submit(inc, y)
        with pytest.raises(ZeroDivisionError):
            await z


@coro_test
async def test_asyncio_as_completed():
    async with AioClient(processes=False) as c:
        futures = c.map(inc, range(10))

        results = []
        async for future in as_completed(futures):
            results.append(await future)

        assert set(results) == set(range(1, 11))


@coro_test
async def test_asyncio_cancel():
    async with AioClient(processes=False) as c:
        s = c.cluster.scheduler

        x = c.submit(slowinc, 1)
        y = c.submit(slowinc, x)

        while y.key not in s.tasks:
            await asyncio.sleep(0.01)

        await c.cancel([x])

        assert x.cancelled()
        assert 'cancel' in str(x)
        s.validate_state()

        start = time()
        while not y.cancelled():
            await asyncio.sleep(0.01)
            assert time() < start + 5

        assert not s.tasks
        assert not s.who_has
        s.validate_state()


@coro_test
async def test_asyncio_cancel_tuple_key():
    async with AioClient(processes=False) as c:
        x = c.submit(inc, 1, key=('x', 0, 1))
        await x
        await c.cancel(x)
        with pytest.raises(CancelledError):
            await x


@coro_test
async def test_asyncio_wait():
    async with AioClient(processes=False) as c:
        x = c.submit(inc, 1)
        y = c.submit(inc, 2)
        z = c.submit(inc, 3)

        await wait(x)
        assert x.done() is True

        await wait([y, z])
        assert y.done() is True
        assert z.done() is True


@coro_test
async def test_asyncio_run():
    async with AioClient(processes=False) as c:
        results = await c.run(inc, 1)
        assert len(results) > 0
        assert [value == 2 for value in results.values()]

        results = await c.run(inc, 1, workers=[])
        assert results == {}


@coro_test
async def test_asyncio_run_on_scheduler():
    def f(dask_scheduler=None):
        return dask_scheduler.address

    async with AioClient(processes=False) as c:
        address = await c.run_on_scheduler(f)
        assert address == c.cluster.scheduler.address

        with pytest.raises(ZeroDivisionError):
            await c.run_on_scheduler(div, 1, 0)


@coro_test
async def test_asyncio_run_coroutine():
    async def aioinc(x, delay=0.02):
        await asyncio.sleep(delay)
        return x + 1

    async def aiothrows(x, delay=0.02):
        await asyncio.sleep(delay)
        raise RuntimeError('hello')

    async with AioClient(processes=False) as c:
        results = await c.run_coroutine(aioinc, 1, delay=0.05)
        assert len(results) > 0
        assert [value == 2 for value in results.values()]

        results = await c.run_coroutine(aioinc, 1, workers=[])
        assert results == {}

        with pytest.raises(RuntimeError) as exc_info:
            await c.run_coroutine(aiothrows, 1)
        exc_info.match("hello")


@slow
@coro_test
async def test_asyncio_restart():
    c = await AioClient(processes=False)

    assert c.status == 'running'
    x = c.submit(inc, 1)
    assert x.key in c.refcount

    await c.restart()
    assert x.key not in c.refcount

    key = x.key
    del x
    import gc; gc.collect()

    assert key not in c.refcount
    await c.shutdown()


@coro_test
async def test_asyncio_nanny_workers():
    async with AioClient(n_workers=2) as c:
        assert await c.submit(inc, 1) == 2
