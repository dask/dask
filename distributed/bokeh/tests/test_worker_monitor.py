from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from tornado import gen

from distributed.bokeh.worker_monitor import resource_append
from distributed.utils_test import gen_cluster


def test_resource_append():
    lists = {'time': [], 'cpu': [], 'memory_percent': [],
             'network-send': [], 'network-recv': []}
    msg = {'10.10.20.86': {'cpu': 10, 'memory_percent': 50, 'time': 2000,
                           'network-send': 2**16, 'network-recv': 2**15},
           '10.10.20.87': {'cpu': 30, 'memory_percent': 70, 'time': 1000,
                           'network-send': 2**17, 'network-recv': 2**16}}

    resource_append(lists, msg)
    assert lists == {'time': [1500000], 'cpu': [0.2], 'memory_percent': [0.6],
                     'network-send': [0.1875], 'network-recv': [0.09375]}


def test_processing_update():
    from distributed.diagnostics.scheduler import processing
    from distributed.bokeh.components import ProcessingStacks

    class C(object):
        pass

    s = C()
    s.stacks = {'alice': ['inc', 'inc', 'add'], 'bob': ['add', 'add']}
    s.processing = {'alice': {'inc', 'add'}, 'bob': {'inc'}}
    s.ready = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    s.waiting = {'x': set()}
    s.who_has = {'z': set()}
    s.ncores = {'alice': 4, 'bob': 4}

    msg = processing(s)

    assert msg == {'processing': {'alice': 2, 'bob': 1},
                   'stacks': {'alice': 3, 'bob': 2},
                   'ready': 7,
                   'waiting': 1,
                   'memory': 1,
                   'ncores': {'alice': 4, 'bob': 4}}

    data = ProcessingStacks.processing_update(msg)
    expected = {'name': ['alice', 'bob', 'ready'],
                'processing': [2, 1, 0],
                'stacks': [3, 2, 7],
                'left': [-3, -2, -7/2],
                'right': [2, 1, 0],
                'top': [2, 1, 2],
                'bottom': [1, 0, 0],
                'ncores': [4, 4, 8],
                'alpha': [0.7, 0.7, 0.2]}

    assert data == expected
