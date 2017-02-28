from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from bokeh.models import ColumnDataSource, Model

from distributed.bokeh import messages

from distributed.bokeh.components import (
    TaskStream, TaskProgress, MemoryUsage, ResourceProfiles, WorkerTable,
    Processing
)

@pytest.mark.parametrize('Component', [TaskStream,
                                       TaskProgress,
                                       MemoryUsage,
                                       ResourceProfiles,
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
