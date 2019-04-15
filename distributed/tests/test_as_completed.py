from concurrent.futures._base import CancelledError
from operator import add
import random
from time import sleep

import pytest
from tornado import gen

from distributed.client import _as_completed, as_completed, _first_completed
from distributed.compatibility import Empty, StopAsyncIteration, Queue, Iterator
from distributed.utils_test import gen_cluster, inc, throws
from distributed.utils_test import client, cluster_fixture, loop  # noqa: F401


@gen_cluster(client=True)
def test__as_completed(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)
    z = c.submit(inc, 2)

    queue = Queue()
    yield _as_completed([x, y, z], queue)

    assert queue.qsize() == 3
    assert {queue.get(), queue.get(), queue.get()} == {x, y, z}

    result = yield _first_completed([x, y, z])
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

    with pytest.raises(Empty):
        ac.queue.get(timeout=0.1)

    res = list(as_completed([x, y, x]))
    assert len(res) == 3
    assert set(res) == {x, y}
    assert res.count(x) == 2


def test_as_completed_cancel_last(client):
    w = client.submit(inc, 0.3)
    x = client.submit(inc, 1)
    y = client.submit(inc, 0.3)

    @gen.coroutine
    def _():
        yield gen.sleep(0.1)
        yield w.cancel(asynchronous=True)
        yield y.cancel(asynchronous=True)

    client.loop.add_callback(_)

    ac = as_completed([x, y])
    result = set(ac)

    assert result == {x, y}


@gen_cluster(client=True)
def test_async_for_py2_equivalent(c, s, a, b):
    futures = c.map(sleep, [0.01] * 3, pure=False)
    seq = as_completed(futures)
    x = yield seq.__anext__()
    y = yield seq.__anext__()
    z = yield seq.__anext__()

    assert x.done()
    assert y.done()
    assert z.done()
    assert x.key != y.key

    with pytest.raises(StopAsyncIteration):
        yield seq.__anext__()


@gen_cluster(client=True)
def test_as_completed_error_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 1)

    ac = as_completed([x, y])
    first = yield ac.__anext__()
    second = yield ac.__anext__()
    result = {first, second}

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
def test_as_completed_with_results_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 5)
    z = c.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True)
    y.cancel()
    with pytest.raises(RuntimeError) as exc:
        first = yield ac.__anext__()
        second = yield ac.__anext__()
        third = yield ac.__anext__()
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

    assert isinstance(dd[y][0], CancelledError)
    assert isinstance(dd[x][0][1], RuntimeError)
    assert dd[z][0] == 2


@gen_cluster(client=True)
def test_as_completed_with_results_no_raise_async(c, s, a, b):
    x = c.submit(throws, 1)
    y = c.submit(inc, 5)
    z = c.submit(inc, 1)

    ac = as_completed([x, y, z], with_results=True, raise_errors=False)
    y.cancel()
    first = yield ac.__anext__()
    second = yield ac.__anext__()
    third = yield ac.__anext__()
    res = [first, second, third]

    dd = {r[0]: r[1:] for r in res}
    assert set(dd.keys()) == {y, x, z}
    assert x.status == "error"
    assert y.status == "cancelled"
    assert z.status == "finished"

    assert isinstance(dd[y][0], CancelledError)
    assert isinstance(dd[x][0][1], RuntimeError)
    assert dd[z][0] == 2
