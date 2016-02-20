from ipykernel.comm import Comm
import ipywidgets as widgets
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

from distributed.executor import Executor, wait
from distributed.scheduler import dumps_task
from distributed.utils_test import (cluster, loop, inc,
        div, dec, throws, gen_cluster)
from distributed.utils import sync
from distributed.diagnostics.progressbar import (ProgressWidget,
        MultiProgressWidget, progress)


@gen_cluster()
def test_progressbar_widget(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {b'x': (inc, 1),
                                             b'y': (inc, b'x'),
                                             b'z': (inc, b'y')}),
                   keys=[b'z'],
                   dependencies={b'y': {b'x'}, b'z': {b'y'}})

    progress = ProgressWidget(['z'], scheduler=(s.ip, s.port))
    yield progress.listen()

    assert progress.bar.value == 1.0
    assert '3 / 3' in progress.bar_text.value

    progress = ProgressWidget(['z'], scheduler=(s.ip, s.port))
    yield progress.listen()


@gen_cluster()
def test_multi_progressbar_widget(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {b'x-1': (inc, 1),
                                             b'x-2': (inc, b'x-1'),
                                             b'x-3': (inc, b'x-2'),
                                             b'y-1': (dec, b'x-3'),
                                             b'y-2': (dec, b'y-1'),
                                             b'e': (throws, b'y-2'),
                                             b'other': (inc, 123)}),
                   keys=[b'e'],
                   dependencies={b'x-2': [b'x-1'], b'x-3': [b'x-2'],
                                 b'y-1': [b'x-3'], b'y-2': [b'y-1'],
                                 b'e': [b'y-2']})

    p = MultiProgressWidget(['e'], scheduler=(s.ip, s.port))
    yield p.listen()

    assert p.bars[b'x'].value == 1.0
    assert p.bars[b'y'].value == 1.0
    assert p.bars[b'e'].value == 0.0
    assert '3 / 3' in p.bar_texts[b'x'].value
    assert '2 / 2' in p.bar_texts[b'y'].value
    assert '0 / 1' in p.bar_texts[b'e'].value

    assert p.bars[b'x'].bar_style == 'success'
    assert p.bars[b'y'].bar_style == 'success'
    # assert p.bars['e'].bar_style == 'danger'

    assert p.status == 'error'

    capacities = [int(re.search(r'\d+ / \d+', row.children[0].value)
                    .group().split(' / ')[1])
                  for row in p.bar_widgets.children]
    assert sorted(capacities, reverse=True) == capacities


@gen_cluster()
def test_multi_progressbar_widget_after_close(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {b'x-1': (inc, 1),
                                             b'x-2': (inc, b'x-1'),
                                             b'x-3': (inc, b'x-2'),
                                             b'y-1': (dec, b'x-3'),
                                             b'y-2': (dec, b'y-1'),
                                             b'e': (throws, b'y-2'),
                                             b'other': (inc, 123)}),
                   keys=[b'e'],
                   dependencies={b'x-2': {b'x-1'}, b'x-3': {b'x-2'},
                                 b'y-1': {b'x-3'}, b'y-2': {b'y-1'},
                                 b'e': {b'y-2'}})

    p = MultiProgressWidget(['x-1', 'x-2', 'x-3'], scheduler=(s.ip, s.port))
    yield p.listen()

    assert b'x' in p.bars


def test_values(loop):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = [e.submit(inc, i) for i in range(5)]
            wait(L)
            p = MultiProgressWidget(L)
            sync(loop, p.listen)
            assert set(p.bars) == {b'inc'}
            assert p.status == 'finished'
            assert p.stream.closed()
            assert '5 / 5' in p.bar_texts[b'inc'].value
            assert p.bars[b'inc'].value == 1.0

            x = e.submit(throws, 1)
            p = MultiProgressWidget([x])
            sync(loop, p.listen)
            assert p.status == 'error'


def test_progressbar_done(loop):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = [e.submit(inc, i) for i in range(5)]
            wait(L)
            p = ProgressWidget(L)
            sync(loop, p.listen)
            assert p.status == 'finished'
            assert p.bar.value == 1.0
            assert p.bar.bar_style == 'success'

            f = e.submit(throws, L)
            wait([f])

            p = ProgressWidget([f])
            sync(loop, p.listen)
            assert p.status == 'error'
            assert p.bar.value == 0.0
            assert p.bar.bar_style == 'danger'


@gen_cluster()
def test_multibar_complete(s, a, b):
    s.update_graph(tasks=valmap(dumps_task, {b'x-1': (inc, 1),
                                             b'x-2': (inc, b'x-1'),
                                             b'x-3': (inc, b'x-2'),
                                             b'y-1': (dec, b'x-3'),
                                             b'y-2': (dec, b'y-1'),
                                             b'e': (throws, b'y-2'),
                                             b'other': (inc, 123)}),
                   keys=[b'e'],
                   dependencies={b'x-2': {b'x-1'}, b'x-3': {b'x-2'},
                                 b'y-1': {b'x-3'}, b'y-2': {b'y-1'},
                                 b'e': {b'y-2'}})

    p = MultiProgressWidget(['e'], scheduler=(s.ip, s.port), complete=True)
    yield p.listen()

    assert p._last_response['all'] == {b'x': 3, b'y': 2, b'e': 1}
    assert all(b.value == 1.0 for k, b in p.bars.items() if k != b'e')
    assert '3 / 3' in p.bar_texts[b'x'].value
    assert '2 / 2' in p.bar_texts[b'y'].value


def test_fast(loop):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            L = e.map(inc, range(100))
            L2 = e.map(dec, L)
            L3 = e.map(add, L, L2)
            p = progress(L3, multi=True, complete=True, notebook=True)
            sync(loop, p.listen)
            assert set(p._last_response['all']) == {b'inc', b'dec', b'add'}
