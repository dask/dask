from __future__ import print_function, division, absolute_import

from time import sleep

import pytest
from tornado import gen

from distributed import Client, Scheduler, Worker
from distributed.core import read
from distributed.client import _wait
from distributed.diagnostics.eventstream import EventStream, eventstream
from distributed.metrics import time
from distributed.utils_test import inc, div, dec, gen_cluster
from distributed.worker import dumps_task


@gen_cluster(client=True)
def test_eventstream(c, s, a, b):
    es = EventStream()
    s.add_plugin(es)
    assert es.buffer == []

    futures = c.map(div, [1] * 10, range(10))
    yield _wait(futures)

    assert len(es.buffer) == 10


@gen_cluster(client=True)
def test_eventstream_remote(c, s, a, b):
    base_plugins = len(s.plugins)
    stream = yield eventstream(s.address, interval=0.010)

    start = time()
    while len(s.plugins) == base_plugins:
        yield gen.sleep(0.01)
        assert time() < start + 5

    futures = c.map(div, [1] * 10, range(10))

    start = time()
    total = []
    while len(total) < 10:
        msgs = yield read(stream)
        assert isinstance(msgs, list)
        total.extend(msgs)
        assert time() < start + 5

    stream.close()
    start = time()
    while len(s.plugins) > base_plugins:
        yield gen.sleep(0.01)
        assert time() < start + 5
