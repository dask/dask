from distributed.diagnostics.status_monitor import (worker_table_plot,
        worker_table_update, task_table_plot, task_table_update)
from distributed.diagnostics.scheduler import workers, tasks

from distributed.utils_test import gen_cluster, inc
from distributed.executor import _wait

from tornado import gen


@gen_cluster()
def test_worker_table(s, a, b):
    while any('last-seen' not in v for v in s.host_info.values()):
        yield gen.sleep(0.01)
    data = workers(s)
    source, plot = worker_table_plot()
    worker_table_update(source, data)

    assert source.data['workers'] == ['127.0.0.1']


@gen_cluster(executor=True)
def test_task_table(e, s, a, b):
    source, plot = task_table_plot()

    data = tasks(s)
    task_table_update(source, data)
    assert source.data['processing'] == [0]

    futures = e.map(inc, range(10))
    yield _wait(futures)

    data = tasks(s)
    task_table_update(source, data)
    assert source.data['processing'] == [0]
    assert source.data['total'] == [10]
