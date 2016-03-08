from __future__ import print_function, division, absolute_import

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


d = {"address": "SCHEDULER_ADDRESS:9999",
     "ready": 5,
     "ncores": {"192.168.1.107:44544": 4,
                "192.168.1.107:36441": 4},
     "in-memory": 30, "waiting": 20,
     "processing": {"192.168.1.107:44544": {'inc': 3, 'add': 1},
                    "192.168.1.107:36441": {'inc': 2}},
     "tasks": 70,
     "failed": 9,
     "bytes": {"192.168.1.107:44544": 1000,
               "192.168.1.107:36441": 2000}}

from distributed.diagnostics.scheduler_widgets import (scheduler_status_widget,
        scheduler_status)
from distributed.http.scheduler import HTTPScheduler
from time import time

from distributed.utils_test import gen_cluster, inc

from tornado import gen
from tornado.httpclient import AsyncHTTPClient


def test_scheduler_status_widget():
    widget = scheduler_status_widget(d)
    assert d['address'] in widget.children[0].value


@gen_cluster(executor=True)
def test_scheduler_status(e, s, a, b):
    ss = HTTPScheduler(s)
    ss.listen(0)

    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/status.json' % ss.port)

    widget = scheduler_status(e, 50, port=ss.port, loop=e.loop)

    yield gen.sleep(1)

    start = time()
    while s.address not in widget.children[0].value:
        yield gen.sleep(0.01)
        assert time() < start + 1

    futures = e.map(inc, range(11))
    yield e._gather(futures)

    start = time()
    while '11' not in widget.children[1].value:
        yield gen.sleep(0.01)
        assert time() < start + 1

    ss.stop()
