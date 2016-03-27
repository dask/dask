from __future__ import print_function, division, absolute_import

import pytest

from tornado import gen

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
    assert d['fraction'] == ['2 / 5', '0 / 1']
    assert d['erred'] == [0, 1]
    assert d['released'] == [1, 0]
    assert d['released_right'] == [1/5, 0]
    assert d['in_memory_right'] == [3 / 5, 0]
    assert d['erred_left'] == [1, 0]


@gen_cluster(executor=True, timeout=None)
def test_progress_stream(e, s, a, b):
    futures = e.map(div, [1] * 10, range(10))
    yield _wait(futures)

    stream = yield progress_stream(s.address, interval=0.010)
    msg = yield read(stream)
    assert msg == {'all': {'div': 10}, 'erred': {'div': 1},
                   'in_memory': {'div': 9}, 'released': {'div': 1}}

    d = progress_quads(msg)

    assert d == {'name': ['div'],
                 'all': [10],
                 'in_memory': [9],
                 'in_memory_right': [1],
                 'fraction': ['9 / 10'],
                 'erred': [1],
                 'erred_left': [0.9],
                 'released': [1],
                 'released_right': [0.1],
                 'top': [0.7],
                 'center': [0.5],
                 'bottom': [0.3]}

    stream.close()
