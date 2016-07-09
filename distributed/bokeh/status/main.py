#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from distributed.bokeh.status_monitor import (
    worker_table_plot, worker_table_update, task_table_plot,
    task_table_update, progress_plot, task_stream_plot
)
from distributed.bokeh.worker_monitor import resource_profile_plot
from distributed.diagnostics.progress_stream import progress_quads
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

worker_source, worker_table = worker_table_plot()
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(worker_source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


task_source, task_table = task_table_plot()
def task_update():
    with log_errors():
        try:
            msg = messages['tasks']['deque'][-1]
        except IndexError:
            return
        task_table_update(task_source, msg)
doc.add_periodic_callback(task_update, messages['tasks']['interval'])


resource_index = [0]
resource_source, resource_plot, network_plot, combo_toolbar = resource_profile_plot(sizing_mode=SIZING_MODE)
def resource_update():
    with log_errors():
        index = messages['workers']['index']
        data = messages['workers']['plot-data']

        if not index or index[-1] == resource_index[0]:
            return

        if resource_index == [0]:
            data = valmap(list, data)

        ind = bisect(index, resource_index[0])
        indexes = list(range(ind, len(index)))
        data = {k: [v[i] for i in indexes] for k, v in data.items()}
        resource_index[0] = index[-1]
        resource_source.stream(data, 1000)

doc.add_periodic_callback(resource_update, messages['workers']['interval'])


progress_source, progress_plot = progress_plot()
def progress_update():
    with log_errors():
        msg = messages['progress']
        d = progress_quads(msg)
        progress_source.data.update(d)
        progress_plot.title.text = ("Progress -- total: %(total)s, "
            "in-memory: %(in-memory)s, processing: %(processing)s, "
            "ready: %(ready)s, waiting: %(waiting)s, failed: %(failed)s"
            % messages['tasks']['deque'][-1])
doc.add_periodic_callback(progress_update, 50)


task_stream_index = [0]
task_stream_source, task_stream_plot = task_stream_plot()
def task_stream_update():
    with log_errors():
        index = messages['task-events']['index']
        old = rectangles = messages['task-events']['rectangles']

        if not index or index[-1] == task_stream_index[0]:
            return

        ind = bisect(index, task_stream_index[0])
        rectangles = {k: [v[i] for i in range(ind, len(index))]
                      for k, v in rectangles.items()}
        task_stream_index[0] = index[-1]

        # If there has been a five second delay, clear old rectangles
        if rectangles['start']:
            last_end = old['start'][ind - 1] + old['duration'][ind - 1]
            if min(rectangles['start']) > last_end + 20000:  # long delay
                task_stream_source.data.update(rectangles)
                return

        task_stream_source.stream(rectangles, 1000)

doc.add_periodic_callback(task_stream_update, messages['task-events']['interval'])

layout = column(
    row(
        column(resource_plot, network_plot, sizing_mode=SIZING_MODE),
        column(combo_toolbar, sizing_mode=SIZING_MODE),
        sizing_mode=SIZING_MODE
    ),
    row(task_stream_plot, sizing_mode=SIZING_MODE),
    row(progress_plot, sizing_mode=SIZING_MODE),
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
