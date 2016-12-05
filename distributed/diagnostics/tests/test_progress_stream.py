from __future__ import print_function, division, absolute_import

from time import sleep

import pytest
from tornado import gen

from dask import do
from distributed.core import read
from distributed.client import _wait
from distributed.diagnostics.progress_stream import (progress_quads,
        nbytes_bar, progress_stream, task_stream_append)
from distributed.metrics import time
from distributed.utils_test import inc, div, dec, gen_cluster
from distributed.worker import dumps_task

def test_progress_quads():
    msg = {'all': {'inc': 5, 'dec': 1, 'add': 4},
           'memory': {'inc': 2, 'dec': 0, 'add': 1},
           'erred': {'inc': 0, 'dec': 1, 'add': 0},
           'released': {'inc': 1, 'dec': 0, 'add': 1}}

    d = progress_quads(msg, nrows=2)
    color = d.pop('color')
    assert len(set(color)) == 3
    assert d == {'name': ['inc', 'add', 'dec'],
                 'show-name': ['inc', 'add', 'dec'],
                 'left': [0, 0, 1],
                 'right': [0.9, 0.9, 1.9],
                 'top': [0, -1, 0],
                 'bottom': [-.8, -1.8, -.8],
                 'all': [5, 4, 1],
                 'released': [1, 1, 0],
                 'memory': [2, 1, 0],
                 'erred': [0, 0, 1],
                 'done': ['3 / 5', '2 / 4', '1 / 1'],
                 'released-loc': [.9 * 1 / 5, .25 * 0.9, 1.0],
                 'memory-loc': [.9 * 3 / 5, .5 * 0.9, 1.0],
                 'erred-loc': [.9 * 3 / 5, .5 * 0.9, 1.9]}


def test_progress_quads_too_many():
    keys = ['x-%d' % i for i in range(1000)]
    msg = {'all': {k: 1 for k in keys},
           'memory': {k: 0 for k in keys},
           'erred': {k: 0 for k in keys},
           'released': {k: 0 for k in keys}}

    d = progress_quads(msg, nrows=6, ncols=3)
    assert len(d['name']) == 6 * 3


@gen_cluster(client=True, timeout=None)
def test_progress_stream(c, s, a, b):
    futures = c.map(div, [1] * 10, range(10))

    x = 1
    for i in range(5):
        x = do(inc)(x)
    future = c.compute(x)

    yield _wait(futures + [future])

    stream = yield progress_stream(s.address, interval=0.010)
    msg = yield read(stream)
    nbytes = msg.pop('nbytes')
    assert msg == {'all': {'div': 10, 'inc': 5, 'finalize': 1},
                   'erred': {'div': 1},
                   'memory': {'div': 9, 'finalize': 1},
                   'released': {'inc': 5}}
    assert set(nbytes) == set(msg['all'])
    assert all(v > 0 for v in nbytes.values())

    assert progress_quads(msg)

    stream.close()


def test_nbytes_bar():
    nbytes = {'inc': 1000, 'dec': 3000}
    expected = {'name': ['dec', 'inc'],
                'left': [0, 0.75],
                'center': [0.375, 0.875],
                'right': [0.75, 1.0],
                'percent': [75, 25],
                'MB': [0.003, 0.001],
                'text': ['dec', 'inc']}

    result = nbytes_bar(nbytes)
    color = result.pop('color')
    assert len(set(color)) == 2
    assert result == expected


def test_progress_quads_many_functions():
    funcnames = ['fn%d' % i for i in range(1000)]
    msg = {'all': {fn: 1 for fn in funcnames},
           'memory': {fn: 1 for fn in funcnames},
           'erred': {fn: 0 for fn in funcnames},
           'released': {fn: 0 for fn in funcnames}}

    d = progress_quads(msg, nrows=2)
    color = d.pop('color')
    assert len(set(color)) < 100

def test_task_stream_append():
    msgs = [{'status': 'OK', 'compute_start': 10, 'compute_stop': 20,
             'key':'inc-1', 'thread': 5855, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute_start': 15, 'compute_stop': 25,
             'key':'inc-2', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'error', 'compute_start': 10, 'compute_stop': 14,
             'key':'inc-3', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute_start': 10, 'compute_stop': 30,
             'transfer_start': 8, 'transfer_stop': 10,
             'key':'add-1', 'thread': 4000, 'worker':'127.0.0.2:9999'}]

    lists = {name: [] for name in
            'start duration key name color worker worker_thread y alpha'.split()}
    workers = {'127.0.0.1:9999-5855': 0}

    for msg in msgs:
        task_stream_append(lists, msg, workers)
    assert len(lists['start']) == len(msgs) + 1  # +1 for a transfer
    assert len(workers) == 3
    assert set(workers) == set(lists['worker_thread'])
    assert set(workers.values()) == set(range(len(workers)))
    assert lists['color'][-1] == '#FF0020'
    L = lists['color']
    assert L[0] == L[1]
    assert L[2] == 'black'
    assert L[3] != L[0]
