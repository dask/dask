#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import json

from bokeh.plotting import curdoc, vplot
from toolz import valmap
from tornado import gen

from distributed.diagnostics.status_monitor import (
        worker_table_plot, worker_table_update, task_table_plot,
        task_table_update, task_stream_plot)
from distributed.diagnostics.worker_monitor import (
        resource_profile_plot, resource_profile_update)
from distributed.utils import log_errors
import distributed.diagnostics

messages = distributed.diagnostics.messages  # global message store

doc = curdoc()

width = 800

worker_source, worker_table = worker_table_plot(width=width)
@gen.coroutine
def worker_update():
    with log_errors():
        msg = messages['workers']['deque'][-1]
        worker_table_update(worker_source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])

task_source, task_table = task_table_plot(width=width)
@gen.coroutine
def task_update():
    with log_errors():
        msg = messages['tasks']['deque'][-1]
        task_table_update(task_source, msg)
doc.add_periodic_callback(task_update, messages['tasks']['interval'])

task_stream_source, task_stream_plot = task_stream_plot(width=width)
last_time = [None]
@gen.coroutine
def task_stream_update2():
    with log_errors():
        if messages['task-events']['last_seen'] == last_time:
            return
        else:
            last_time[0] = messages['task-events']['last_seen'][0]
        rectangles = valmap(list, messages['task-events']['rectangles'])
        workers = messages['task-events']['workers']
        workers = {w: i for i, w in enumerate(sorted(workers))}
        rectangles['y'] = [workers[wt] for wt in rectangles['worker_thread']]
        task_stream_source.data.update(rectangles)
doc.add_periodic_callback(task_stream_update2, messages['task-events']['interval'])

resource_source, resource_plot =\
    resource_profile_plot(x_range=task_stream_plot.x_range, width=width)
@gen.coroutine
def resource_update():
    with log_errors():
        worker_buffer = list(messages['workers']['deque'])
        times_buffer = list(messages['workers']['times'])
        resource_profile_update(resource_source, worker_buffer, times_buffer)
doc.add_periodic_callback(resource_update, messages['workers']['interval'])

doc.add_root(vplot(worker_table, task_table, resource_plot, task_stream_plot))
