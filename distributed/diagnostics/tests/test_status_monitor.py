from distributed.diagnostics.status_monitor import (worker_table_plot,
        worker_table_update, task_table_plot, task_table_update,
        task_stream_plot, task_stream_update)
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


def test_task_stream():
    source, plot = task_stream_plot()
    msgs = [{'status': 'OK', 'compute-start': 10, 'compute-stop': 20,
             'key':'inc-1', 'thread': 5855, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute-start': 15, 'compute-stop': 25,
             'key':'inc-2', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute-start': 10, 'compute-stop': 14,
             'key':'inc-3', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute-start': 10, 'compute-stop': 30,
             'transfer-start': 8, 'transfer-stop': 10,
             'key':'add-1', 'thread': 4000, 'worker':'127.0.0.2:9999'}]

    task_stream_update(source, plot, msgs)
    assert len(source.data['start']) == len(msgs) + 1
    assert source.data['color'][-1] == 'red'
    L = source.data['color']
    assert L[0] == L[1] == L[2]
    assert L[3] != L[0]
