import asyncio
import collections

import pytest

from distributed.client import wait
from distributed.diagnostics.eventstream import EventStream, eventstream
from distributed.metrics import time
from distributed.utils_test import div, gen_cluster


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_eventstream(c, s, *workers):
    pytest.importorskip("bokeh")

    es = EventStream()
    s.add_plugin(es)
    assert es.buffer == []

    futures = c.map(div, [1] * 10, range(10))
    total = c.submit(sum, futures[1:])
    await wait(total)
    await wait(futures)

    assert len(es.buffer) == 11

    from distributed.diagnostics.progress_stream import task_stream_append

    lists = {
        name: collections.deque(maxlen=100)
        for name in "start duration key name color worker worker_thread y alpha".split()
    }
    workers = dict()
    for msg in es.buffer:
        task_stream_append(lists, msg, workers)

    assert len([n for n in lists["name"] if n.startswith("transfer")]) == 2
    for name, color in zip(lists["name"], lists["color"]):
        if name == "transfer":
            assert color == "red"

    assert any(c == "black" for c in lists["color"])


@gen_cluster(client=True)
async def test_eventstream_remote(c, s, a, b):
    base_plugins = len(s.plugins)
    comm = await eventstream(s.address, interval=0.010)

    start = time()
    while len(s.plugins) == base_plugins:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    futures = c.map(div, [1] * 10, range(10))

    start = time()
    total = []
    while len(total) < 10:
        msgs = await comm.read()
        assert isinstance(msgs, tuple)
        total.extend(msgs)
        assert time() < start + 5

    await comm.close()
    start = time()
    while len(s.plugins) > base_plugins:
        await asyncio.sleep(0.01)
        assert time() < start + 5
