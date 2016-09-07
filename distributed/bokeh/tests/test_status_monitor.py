from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from distributed.bokeh.status_monitor import (
        task_stream_plot, task_stream_append, progress_plot)


def test_task_stream_plot():
    source, plot = task_stream_plot()


def test_task_stream_append():
    msgs = [{'status': 'OK', 'compute_start': 10, 'compute_stop': 20,
             'key':'inc-1', 'thread': 5855, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute_start': 15, 'compute_stop': 25,
             'key':'inc-2', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'error', 'compute_start': 10, 'compute_stop': 14,
             'key':'inc-3', 'thread': 6000, 'worker':'127.0.0.1:9999'},
            {'status': 'OK', 'compute_start': 10, 'compute_stop': 30,
             'transfer_start': 8, 'transfer_stop': 10,
             'key':'add-1', 'thread': 4000, 'worker':'127.0.0.2:9999'}]

    lists = {name: [] for name in
            'start duration key name color worker worker_thread y alpha'.split()}
    workers = {'127.0.0.1:9999-5855': 0}

    for msg in msgs:
        task_stream_append(lists, msg, workers)
    assert len(lists['start']) == len(msgs) + 1  # +1 for a transfer
    assert len(workers) == 3
    assert set(workers) == set(lists['worker_thread'])
    assert set(workers.values()) == set(range(len(workers)))
    assert lists['color'][-1] == '#FF0020'
    L = lists['color']
    assert L[0] == L[1]
    assert L[2] == 'black'
    assert L[3] != L[0]


def test_progress_plot():
    source, plot = progress_plot()
