from __future__ import print_function, division, absolute_import

import pytest

from tornado import gen

from dask import do
from distributed.core import read
from distributed.executor import _wait
from distributed.diagnostics.progress_stream import (progress_quads,
        progress_stream)
from distributed.utils_test import inc, div, dec, gen_cluster
from distributed.worker import dumps_task
from time import time, sleep

def test_progress_quads():
    msg = {'all': {'inc': 5, 'dec': 1},
           'in_memory': {'inc': 2, 'dec': 0},
           'erred': {'inc': 0, 'dec': 1},
           'released': {'inc': 1, 'dec': 0}}

    d = progress_quads(msg)
    assert d['name'] == ['inc', 'dec']
    assert d['all'] == [5, 1]
    assert d['in_memory'] == [2, 0]
    assert d['fraction'] == ['3 / 5', '0 / 1']
    assert d['erred'] == [0, 1]
    assert d['released'] == [1, 0]
    assert d['released_right'] == [1/5, 0]
    assert d['in_memory_right'] == [3 / 5, 0]
    assert d['erred_left'] == [1, 0]


@gen_cluster(executor=True, timeout=None)
def test_progress_stream(e, s, a, b):
    futures = e.map(div, [1] * 10, range(10))

    x = 1
    for i in range(5):
        x = do(inc)(x)
    future = e.compute(x)

    yield _wait(futures + [future])

    stream = yield progress_stream(s.address, interval=0.010)
    msg = yield read(stream)
    assert msg == {'all': {'div': 10, 'inc': 5, 'finalize': 1},
                   'erred': {'div': 1},
                   'in_memory': {'div': 9, 'finalize': 1},
                   'released': {'div': 1, 'inc': 5}}

    d = progress_quads(msg)

    assert d == {'name': ['div', 'inc', 'finalize'],
                 'all': [10, 5, 1],
                 'in_memory': [9, 0, 1],
                 'in_memory_right': [1, 1, 1],
                 'fraction': ['10 / 10', '5 / 5', '1 / 1'],
                 'erred': [1, 0, 0],
                 'erred_left': [0.9, 1, 1],
                 'released': [1, 5, 0],
                 'released_right': [0.1, 1, 0],
                 'top': [0.7, 1.7, 2.7],
                 'center': [0.5, 1.5, 2.5],
                 'bottom': [0.3, 1.3, 2.3]}

    stream.close()
