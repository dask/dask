from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from bokeh.models import ColumnDataSource, Model

from distributed.bokeh import messages
from distributed.utils_test import slowinc

from distributed.bokeh.components import (
    TaskStream, TaskProgress, MemoryUsage, WorkerTable,
    Processing, ProfilePlot, ProfileTimePlot
)


@pytest.mark.parametrize('Component', [TaskStream,
                                       TaskProgress,
                                       MemoryUsage,
                                       WorkerTable,
                                       Processing])
def test_basic(Component):
    c = Component()
    assert isinstance(c.source, ColumnDataSource)
    assert isinstance(c.root, Model)
    c.update(messages)


from distributed.utils_test import gen_cluster
from tornado import gen
from distributed.diagnostics.scheduler import workers


@gen_cluster()
def test_worker_table(s, a, b):
    while any('last-seen' not in v for v in s.host_info.values()):
        yield gen.sleep(0.01)
    data = workers(s)

    messages = {'workers': {'deque': [data]}}

    c = WorkerTable()
    c.update(messages)
    assert c.source.data['host'] == ['127.0.0.1']


@gen_cluster(client=True)
def test_profile_plot(c, s, a, b):
    p = ProfilePlot()
    assert len(p.source.data['left']) <= 1
    yield c.map(slowinc, range(10), delay=0.05)
    p.update(a.profile_recent)
    assert len(p.source.data['left']) > 1


@gen_cluster(client=True)
def test_profile_time_plot(c, s, a, b):
    from bokeh.io import curdoc
    sp = ProfileTimePlot(s, doc=curdoc())
    sp.trigger_update()

    ap = ProfileTimePlot(a, doc=curdoc())
    ap.trigger_update()

    assert len(sp.source.data['left']) <= 1
    assert len(ap.source.data['left']) <= 1

    yield c.map(slowinc, range(10), delay=0.05)
    ap.trigger_update()
    sp.trigger_update()
    yield gen.sleep(0.05)
