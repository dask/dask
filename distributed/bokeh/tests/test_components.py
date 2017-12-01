from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from bokeh.models import ColumnDataSource, Model
from tornado import gen

from distributed.bokeh import messages
from distributed.utils_test import slowinc, gen_cluster

from distributed.bokeh.components import (
    TaskStream, TaskProgress, MemoryUsage,
    Processing, ProfilePlot, ProfileTimePlot
)


@pytest.mark.parametrize('Component', [TaskStream,
                                       TaskProgress,
                                       MemoryUsage,
                                       Processing])
def test_basic(Component):
    c = Component()
    assert isinstance(c.source, ColumnDataSource)
    assert isinstance(c.root, Model)
    c.update(messages)


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
