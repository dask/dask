#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect
import json

from bokeh.plotting import curdoc, vplot
from toolz import valmap

from distributed.bokeh.status_monitor import (
        worker_table_plot, worker_table_update, task_table_plot,
        task_table_update, progress_plot, task_stream_plot)
from distributed.bokeh.worker_monitor import (
        resource_profile_plot, resource_profile_update)
from distributed.diagnostics.progress_stream import progress_quads
from distributed.utils import log_errors
import distributed.bokeh


messages = distributed.bokeh.messages  # global message store
doc = curdoc()
table_width = 800
width = 600


worker_source, worker_table = worker_table_plot(width=table_width)
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(worker_source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


task_source, task_table = task_table_plot(width=table_width)
def task_update():
    with log_errors():
        try:
            msg = messages['tasks']['deque'][-1]
        except IndexError:
            return
        task_table_update(task_source, msg)
doc.add_periodic_callback(task_update, messages['tasks']['interval'])


resource_source, resource_plot = resource_profile_plot(height=int(width/3), width=width)
resource_plot.min_border_top -= 40
resource_plot.title = None
resource_plot.min_border_bottom -= 40
resource_plot.plot_height -= 80
resource_plot.logo = None
# resource_plot.toolbar_location = None
resource_plot.xaxis.axis_label = None
resource_index = [0]


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


progress_source, progress_plot = progress_plot(height=int(width/3), width=width)
progress_plot.min_border_top -= 40
progress_plot.title = None
progress_plot.min_border_bottom -= 40
progress_plot.plot_height -= 80
progress_plot.logo = None
# progress_plot.toolbar_location = None
progress_plot.xaxis.axis_label = None


def progress_update():
    with log_errors():
        msg = messages['progress']
        d = progress_quads(msg)
        progress_source.data.update(d)
doc.add_periodic_callback(progress_update, 50)


task_stream_source, task_stream_plot = task_stream_plot(height=int(width/2),
        width=width, follow_interval=None)
task_stream_plot.min_border_bottom = 0
task_stream_plot.min_border_left = 0
task_stream_plot.min_border_right = 10
task_stream_plot.xaxis.axis_label = None
task_stream_index = [0]
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


vbox = vplot(
           resource_plot,
           progress_plot,
           task_stream_plot,
           worker_table,
           task_table,
         )
doc.add_root(vbox)
