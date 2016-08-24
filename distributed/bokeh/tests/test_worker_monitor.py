from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from collections import deque
import datetime

from tornado import gen

from distributed.bokeh.worker_monitor import (resource_profile_plot,
        resource_profile_update, resource_append, worker_table_plot,
        worker_table_update)
from distributed.diagnostics.scheduler import workers, tasks
from distributed.utils_test import gen_cluster


@gen_cluster()
def test_resource_monitor_plot(s, a, b):
    while any('last-seen' not in v for v in s.host_info.values()):
        yield gen.sleep(0.01)

    times_buffer = [1000, 1001, 1003]
    workers_buffer = [{},
                      {'10.10.20.86': {'cpu': 15.9, 'memory_percent': 63.0}},
                      {'10.10.20.86': {'cpu': 14.9, 'memory_percent': 64.0,
                                       'network-send': 2**16,
                                       'network-recv': 2**15},
                       '10.10.20.87': {'cpu': 13.9, 'memory_percent': 64.0,
                                       'network-send': 2**17,
                                       'network-recv': 2**16}}]

    source, _, _, _ = resource_profile_plot()
    resource_profile_update(source, workers_buffer, times_buffer)

    assert source.data['workers'] == ['10.10.20.86', '10.10.20.87']
    assert len(source.data['cpu']) == 2
    assert source.data['cpu'][0] == ['null', 15.9, 14.9]
    assert source.data['cpu'][1] == ['null', 'null', 13.9]

    assert source.data['times'][0] == ['null', 1001000, 1003000]
    assert source.data['times'][1] == ['null','null', 1003000]
    assert len(source.data['times']) == 2
    assert len(source.data['memory_percent']) == 2

    assert len(source.data['network-send']) == 2
    assert source.data['network-send'][1] == ['null', 'null', 2**17]
    assert len(source.data['network-recv']) == 2
    assert source.data['network-recv'][1] == ['null', 'null', 2**16]


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


@gen_cluster()
def test_worker_table(s, a, b):
    while any('last-seen' not in v for v in s.host_info.values()):
        yield gen.sleep(0.01)
    data = workers(s)
    source, plot = worker_table_plot()
    worker_table_update(source, data)

    assert source.data['host'] == ['127.0.0.1']


def test_processing_update():
    from distributed.diagnostics.scheduler import processing
    from distributed.bokeh.worker_monitor import processing_update

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

    data = processing_update(msg)
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


def test_processing_plot():
    from distributed.bokeh.worker_monitor import processing_plot
    source, fig = processing_plot()
