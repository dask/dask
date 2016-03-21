from distributed.diagnostics.status_monitor import (worker_table_plot,
        worker_table_update)
from distributed.diagnostics.scheduler import workers

from distributed.utils_test import gen_cluster

from tornado import gen


@gen_cluster()
def test_simple(s, a, b):
    while any('last-seen' not in v for v in s.host_info.values()):
        yield gen.sleep(0.01)
    data = workers(s)
    source, plot = worker_table_plot()
    worker_table_update(source, data)

    assert source.data['workers'] = ['127.0.0.1']
