import asyncio
from collections.abc import Iterator
from operator import add
import queue
import random
from time import sleep

import pytest

from distributed.client import _as_completed, as_completed, _first_completed, wait
from distributed.metrics import time
from distributed.utils import CancelledError
from distributed.utils_test import gen_cluster, inc, throws
from distributed.utils_test import client, cluster_fixture, loop  # noqa: F401


@gen_cluster(client=True)
async def test__as_completed(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)
    z = c.submit(inc, 2)

    q = queue.Queue()
    await _as_completed([x, y, z], q)

    assert q.qsize() == 3
    assert {q.get(), q.get(), q.get()} == {x, y, z}

    result = await _first_completed([x, y, z])
    assert result in [x, y, z]


def test_as_completed(client):
    x = client.submit(inc, 1)
    y = client.submit(inc, 2)
    z = client.submit(inc, 1)

    seq = as_completed([x, y, z])
    assert seq.count() == 3
    assert isinstance(seq, Iterator)
    assert set(seq) == {x, y, z}
    assert seq.count() == 0

    assert list(as_completed([])) == []


def test_as_completed_with_non_futures(client):
    with pytest.raises(TypeError):
        list(as_completed([1, 2, 3]))


def test_as_completed_add(client):
    total = 0
    expected = sum(map(inc, range(10)))
    futures = client.map(inc, range(10))
    ac = as_completed(futures)
    for future in ac:
        result = future.result()
        total += result
        if random.random() < 0.5:
            future = client.submit(add, future, 10)
            ac.add(future)
            expected += result + 10
    assert total == expected


def test_as_completed_update(client):
    total = 0
    todo = list(range(10))
    expected = sum(map(inc, todo))
    ac = as_completed([])
    while todo or not ac.is_empty():
        if todo:
            work, todo = todo[:4], todo[4:]
            ac.update(client.map(inc, work))
        batch = ac.next_batch(block=True)
        total += sum(r.result() for r in batch)
    assert total == expected


def test_as_completed_repeats(client):
    ac = as_completed()
    x = client.submit(inc, 1)
    ac.add(x)
    ac.add(x)

    assert next(ac) is x
    assert next(ac) is x

    with pytest.raises(StopIteration):
        next(ac)

    ac.add(x)
    assert next(ac) is x


def test_as_completed_is_empty(client):
    ac = as_completed()
    assert ac.is_empty()
    x = client.submit(inc, 1)
    ac.add(x)
    assert not ac.is_empty()
    assert next(ac) is x
    assert ac.is_empty()


def test_as_completed_cancel(client):
    x = client.submit(inc, 1)
    y = client.submit(inc, 1)

    ac = as_completed([x, y])
    x.cancel()

    assert next(ac) is x or y
    assert next(ac) is y or x

    with pytest.raises(queue.Empty):
        ac.queue.get(timeout=0.1)

    res = list(as_completed([x, y, x]))
    assert len(res) == 3
    assert set(res) == {x, y}
    assert res.count(x) == 2


def test_as_completed_cancel_last(client):
    w = client.submit(inc, 0.3)
    x = client.submit(inc, 1)
    y = client.submit(inc, 0.3)

    async def _():
        await asyncio.sleep(0.1)
        await w.cancel(asynchronous=True)
        await y.cancel(asynchronous=True)

    client.loop.add_callback(_)

    ac = as_completed([x, y])
    result = set(ac)

    assert result == {x, y}


@gen_cluster(client=True)
async def test_async_for_py2_equivalent(c, s, a, b):
    futures = c.map(sleep, [0.01] * 3, pure=False)
    seq = as_completed(futures)
    x, y, z = [el async for el in seq]
    assert x.done()
    assert y.done()
    assert z.done()
    assert x.key != y.key


@gen_cluster(client=True)
async def test_as_completed_error_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 1)

    ac = as_completed([x, y])
    result = {el async for el in ac}
    assert result == {x, y}
    assert x.status == "error"
    assert y.status == "finished"


def test_as_completed_error(client):
    x = client.submit(throws, 1)
    y = client.submit(inc, 1)

    ac = as_completed([x, y])
    result = set(ac)

    assert result == {x, y}
    assert x.status == "error"
    assert y.status == "finished"


def test_as_completed_with_results(client):
    x = client.submit(throws, 1)
    y = client.submit(inc, 5)
    z = client.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True)
    y.cancel()
    with pytest.raises(RuntimeError) as exc:
        res = list(ac)
    assert str(exc.value) == "hello!"


@gen_cluster(client=True)
async def test_as_completed_with_results_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 5)
    z = c.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True)
    await y.cancel()
    with pytest.raises(RuntimeError) as exc:
        async for _ in ac:
            pass
    assert str(exc.value) == "hello!"


def test_as_completed_with_results_no_raise(client):
    x = client.submit(throws, 1)
    y = client.submit(inc, 5)
    z = client.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True, raise_errors=False)
    y.cancel()
    res = list(ac)

    dd = {r[0]: r[1:] for r in res}
    assert set(dd.keys()) == {y, x, z}
    assert x.status == "error"
    assert y.status == "cancelled"
    assert z.status == "finished"

    assert isinstance(dd[y][0], CancelledError) or dd[y][0] == 6
    assert isinstance(dd[x][0][1], RuntimeError)
    assert dd[z][0] == 2


@gen_cluster(client=True)
async def test_str(c, s, a, b):
    futures = c.map(inc, range(3))
    ac = as_completed(futures)
    assert "waiting=3" in str(ac)
    assert "waiting=3" in repr(ac)
    assert "done=0" in str(ac)
    assert "done=0" in repr(ac)

    await ac.__anext__()

    start = time()
    while "done=2" not in str(ac):
        await asyncio.sleep(0.01)
        assert time() < start + 2


@gen_cluster(client=True)
async def test_as_completed_with_results_no_raise_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 5)
    z = c.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True, raise_errors=False)
    c.loop.add_callback(y.cancel)
    res = [el async for el in ac]

    dd = {r[0]: r[1:] for r in res}
    assert set(dd.keys()) == {y, x, z}
    assert x.status == "error"
    assert y.status == "cancelled"
    assert z.status == "finished"

    assert isinstance(dd[y][0], CancelledError)
    assert isinstance(dd[x][0][1], RuntimeError)
    assert dd[z][0] == 2


@gen_cluster(client=True, timeout=None)
async def test_clear(c, s, a, b):
    futures = c.map(inc, range(3))
    ac = as_completed(futures)
    await wait(futures)
    ac.clear()
    with pytest.raises(StopAsyncIteration):
        await ac.__anext__()
    del futures

    while s.tasks:
        await asyncio.sleep(0.3)
