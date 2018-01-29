from collections import Iterator
from operator import add
import random
from time import sleep

import pytest
from tornado import gen

from distributed import Client
from distributed.client import _as_completed, as_completed, _first_completed
from distributed.compatibility import Empty
from distributed.utils_test import cluster, gen_cluster, inc
from distributed.utils_test import loop  # noqa: F401
from distributed.compatibility import Queue


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


def test_as_completed(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(inc, 1)
            y = c.submit(inc, 2)
            z = c.submit(inc, 1)

            seq = as_completed([x, y, z])
            assert isinstance(seq, Iterator)
            assert set(seq) == {x, y, z}

            assert list(as_completed([])) == []


def test_as_completed_with_non_futures(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop):
            with pytest.raises(TypeError):
                list(as_completed([1, 2, 3]))


def test_as_completed_add(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            total = 0
            expected = sum(map(inc, range(10)))
            futures = c.map(inc, range(10))
            ac = as_completed(futures)
            for future in ac:
                result = future.result()
                total += result
                if random.random() < 0.5:
                    future = c.submit(add, future, 10)
                    ac.add(future)
                    expected += result + 10
            assert total == expected


def test_as_completed_update(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            total = 0
            todo = list(range(10))
            expected = sum(map(inc, todo))
            ac = as_completed([])
            while todo or not ac.is_empty():
                if todo:
                    work, todo = todo[:4], todo[4:]
                    ac.update(c.map(inc, work))
                batch = ac.next_batch(block=True)
                total += sum(r.result() for r in batch)
            assert total == expected


def test_as_completed_repeats(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            ac = as_completed()
            x = c.submit(inc, 1)
            ac.add(x)
            ac.add(x)

            assert next(ac) is x
            assert next(ac) is x

            with pytest.raises(StopIteration):
                next(ac)

            ac.add(x)
            assert next(ac) is x


def test_as_completed_is_empty(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            ac = as_completed()
            assert ac.is_empty()
            x = c.submit(inc, 1)
            ac.add(x)
            assert not ac.is_empty()
            assert next(ac) is x
            assert ac.is_empty()


def test_as_completed_cancel(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.submit(sleep, 1)
            y = c.submit(inc, 1)

            ac = as_completed([x, y])
            x.cancel()

            assert next(ac) is y

            with pytest.raises(Empty):
                ac.queue.get(timeout=0.1)

            assert list(as_completed([x, y, x])) == [y]


def test_as_completed_cancel_last(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            w = c.submit(sleep, 0.3)
            x = c.submit(inc, 1)
            y = c.submit(sleep, 0.3)

            @gen.coroutine
            def _():
                yield gen.sleep(0.1)
                yield w.cancel(asynchronous=True)
                yield y.cancel(asynchronous=True)

            loop.add_callback(_)

            ac = as_completed([x, y])
            result = list(ac)

            assert result == [x]
