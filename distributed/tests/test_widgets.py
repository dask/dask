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

import pytest
from tornado import gen

from distributed.scheduler import Scheduler
from distributed.executor import Executor, wait
from distributed.utils_test import (cluster, _test_cluster, loop, inc,
        div, dec)
from distributed.diagnostics import (ProgressWidget, MultiProgressWidget)

def bad(*args):
    raise Exception()

def test_progressbar_widget(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()

        s.update_graph(dsk={'x': (inc, 1),
                            'y': (inc, 'x'),
                            'z': (inc, 'y')},
                       keys=['z'])
        progress = ProgressWidget(['z'], scheduler=s)

        while True:
            msg = yield s.report_queue.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        progress._update_bar()
        assert progress.bar.value == 1.0
        assert 's' in progress.bar.description

        s.scheduler_queue.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_multi_progressbar_widget(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port), loop=loop)
        yield s._sync_center()
        done = s.start()

        s.update_graph(dsk={'x-1': (inc, 1),
                            'x-2': (inc, 'x-1'),
                            'x-3': (inc, 'x-2'),
                            'y-1': (dec, 'x-3'),
                            'y-2': (dec, 'y-1'),
                            'e': (bad, 'y-2'),
                            'other': (inc, 123)},
                       keys=['e'])

        p = MultiProgressWidget(['e'], scheduler=s)

        assert p.keys == {'x': {'x-1', 'x-2', 'x-3'},
                          'y': {'y-1', 'y-2'},
                          'e': {'e'}}

        while True:
            msg = yield s.report_queue.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'x-3':
                break

        assert p.keys == {'x': set(),
                          'y': {'y-1', 'y-2'},
                          'e': {'e'}}

        p._update_bar()
        assert p.bars['x'].value == 1.0
        assert p.bars['y'].value == 0.0
        assert p.bars['e'].value == 0.0
        assert '3 / 3' in p.texts['x'].value
        assert '0 / 2' in p.texts['y'].value
        assert '0 / 1' in p.texts['e'].value

        while True:
            msg = yield s.report_queue.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'y-2':
                break

        p._update_bar()
        assert p.bars['x'].value == 1.0
        assert p.bars['y'].value == 1.0
        assert p.bars['e'].value == 0.0

        assert p.keys == {'x': set(),
                          'y': set(),
                          'e': {'e'}}

        while True:
            msg = yield s.report_queue.get()
            if msg['op'] == 'task-erred' and msg['key'] == 'e':
                break

        assert p.bars['x'].bar_style == 'success'
        assert p.bars['y'].bar_style == 'success'
        assert p.bars['e'].bar_style == 'danger'

        assert p.status == 'error'

        s.scheduler_queue.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)
