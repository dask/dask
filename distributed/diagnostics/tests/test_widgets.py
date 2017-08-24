from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('ipywidgets')

from ipykernel.comm import Comm
from ipywidgets import Widget

#################
# Utility stuff #
#################

# Taken from ipywidgets/widgets/tests/test_interaction.py
#            https://github.com/ipython/ipywidgets
# Licensed under Modified BSD.  Copyright IPython Development Team.  See:
#   https://github.com/ipython/ipywidgets/blob/master/COPYING.md


class DummyComm(Comm):
    comm_id = 'a-b-c-d'

    def open(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


_widget_attrs = {}
displayed = []
undefined = object()


def setup():
    _widget_attrs['_comm_default'] = getattr(Widget, '_comm_default', undefined)
    Widget._comm_default = lambda self: DummyComm()
    _widget_attrs['_ipython_display_'] = Widget._ipython_display_

    def raise_not_implemented(*args, **kwargs):
        raise NotImplementedError()
    Widget._ipython_display_ = raise_not_implemented


def teardown():
    for attr, value in _widget_attrs.items():
        if value is undefined:
            delattr(Widget, attr)
        else:
            setattr(Widget, attr, value)


def f(**kwargs):
    pass


def clear_display():
    global displayed
    displayed = []


def record_display(*args):
    displayed.extend(args)


# End code taken from ipywidgets

#####################
# Distributed stuff #
#####################

from operator import add
import re

from toolz import valmap

from distributed.client import Client, wait
from distributed.worker import dumps_task
from distributed.utils_test import (cluster, inc, dec, throws, gen_cluster)
from distributed.utils_test import loop  # flake8: noqa
from distributed.utils import sync
from distributed.diagnostics.progressbar import (ProgressWidget,
                                                 MultiProgressWidget, progress)


@gen_cluster()
def test_progressbar_widget(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {'x': (inc, 1),
                                             'y': (inc, 'x'),
                                             'z': (inc, 'y')}),
                   keys=['z'],
                   dependencies={'y': {'x'}, 'z': {'y'}})

    progress = ProgressWidget(['z'], scheduler=(s.ip, s.port))
    yield progress.listen()

    assert progress.bar.value == 1.0
    assert '3 / 3' in progress.bar_text.value

    progress = ProgressWidget(['z'], scheduler=(s.ip, s.port))
    yield progress.listen()


@gen_cluster()
def test_multi_progressbar_widget(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {'x-1': (inc, 1),
                                             'x-2': (inc, 'x-1'),
                                             'x-3': (inc, 'x-2'),
                                             'y-1': (dec, 'x-3'),
                                             'y-2': (dec, 'y-1'),
                                             'e': (throws, 'y-2'),
                                             'other': (inc, 123)}),
                   keys=['e'],
                   dependencies={'x-2': ['x-1'], 'x-3': ['x-2'],
                                 'y-1': ['x-3'], 'y-2': ['y-1'],
                                 'e': ['y-2']})

    p = MultiProgressWidget(['e'], scheduler=(s.ip, s.port))
    yield p.listen()

    assert p.bars['x'].value == 1.0
    assert p.bars['y'].value == 1.0
    assert p.bars['e'].value == 0.0
    assert '3 / 3' in p.bar_texts['x'].value
    assert '2 / 2' in p.bar_texts['y'].value
    assert '0 / 1' in p.bar_texts['e'].value

    assert p.bars['x'].bar_style == 'success'
    assert p.bars['y'].bar_style == 'success'
    assert p.bars['e'].bar_style == 'danger'

    assert p.status == 'error'
    assert 'Exception' in p.elapsed_time.value

    capacities = [int(re.search(r'\d+ / \d+', row.children[0].value)
                      .group().split(' / ')[1])
                  for row in p.bar_widgets.children]
    assert sorted(capacities, reverse=True) == capacities


@gen_cluster()
def test_multi_progressbar_widget_after_close(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {'x-1': (inc, 1),
                                             'x-2': (inc, 'x-1'),
                                             'x-3': (inc, 'x-2'),
                                             'y-1': (dec, 'x-3'),
                                             'y-2': (dec, 'y-1'),
                                             'e': (throws, 'y-2'),
                                             'other': (inc, 123)}),
                   keys=['e'],
                   dependencies={'x-2': {'x-1'}, 'x-3': {'x-2'},
                                 'y-1': {'x-3'}, 'y-2': {'y-1'},
                                 'e': {'y-2'}})

    p = MultiProgressWidget(['x-1', 'x-2', 'x-3'], scheduler=(s.ip, s.port))
    yield p.listen()

    assert 'x' in p.bars


def test_values(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = [c.submit(inc, i) for i in range(5)]
            wait(L)
            p = MultiProgressWidget(L)
            sync(loop, p.listen)
            assert set(p.bars) == {'inc'}
            assert p.status == 'finished'
            assert p.comm.closed()
            assert '5 / 5' in p.bar_texts['inc'].value
            assert p.bars['inc'].value == 1.0

            x = c.submit(throws, 1)
            p = MultiProgressWidget([x])
            sync(loop, p.listen)
            assert p.status == 'error'


def test_progressbar_done(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = [c.submit(inc, i) for i in range(5)]
            wait(L)
            p = ProgressWidget(L)
            sync(loop, p.listen)
            assert p.status == 'finished'
            assert p.bar.value == 1.0
            assert p.bar.bar_style == 'success'
            assert 'Finished' in p.elapsed_time.value

            f = c.submit(throws, L)
            wait([f])

            p = ProgressWidget([f])
            sync(loop, p.listen)
            assert p.status == 'error'
            assert p.bar.value == 0.0
            assert p.bar.bar_style == 'danger'
            assert 'Exception' in p.elapsed_time.value


def test_progressbar_cancel(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            import time
            L = [c.submit(lambda: time.sleep(0.3), i) for i in range(5)]
            p = ProgressWidget(L)
            sync(loop, p.listen)
            L[-1].cancel()
            wait(L[:-1])
            assert p.status == 'error'
            assert p.bar.value == 0  # no tasks finish before cancel is called


@gen_cluster()
def test_multibar_complete(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {'x-1': (inc, 1),
                                             'x-2': (inc, 'x-1'),
                                             'x-3': (inc, 'x-2'),
                                             'y-1': (dec, 'x-3'),
                                             'y-2': (dec, 'y-1'),
                                             'e': (throws, 'y-2'),
                                             'other': (inc, 123)}),
                   keys=['e'],
                   dependencies={'x-2': {'x-1'}, 'x-3': {'x-2'},
                                 'y-1': {'x-3'}, 'y-2': {'y-1'},
                                 'e': {'y-2'}})

    p = MultiProgressWidget(['e'], scheduler=(s.ip, s.port), complete=True)
    yield p.listen()

    assert p._last_response['all'] == {'x': 3, 'y': 2, 'e': 1}
    assert all(b.value == 1.0 for k, b in p.bars.items() if k != 'e')
    assert '3 / 3' in p.bar_texts['x'].value
    assert '2 / 2' in p.bar_texts['y'].value


def test_fast(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            L = c.map(inc, range(100))
            L2 = c.map(dec, L)
            L3 = c.map(add, L, L2)
            p = progress(L3, multi=True, complete=True, notebook=True)
            sync(loop, p.listen)
            assert set(p._last_response['all']) == {'inc', 'dec', 'add'}
