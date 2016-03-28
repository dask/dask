#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect
import json

from bokeh.plotting import curdoc, vplot
from toolz import valmap

from distributed.diagnostics.status_monitor import (
        worker_table_plot, worker_table_update, task_table_plot,
        task_table_update, progress_plot)
from distributed.diagnostics.worker_monitor import (
        resource_profile_plot, resource_profile_update)
from distributed.diagnostics.progress_stream import progress_quads
from distributed.utils import log_errors
import distributed.diagnostics.bokeh


messages = distributed.diagnostics.bokeh.messages  # global message store
doc = curdoc()
width = 800


worker_source, worker_table = worker_table_plot(width=width)
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(worker_source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


task_source, task_table = task_table_plot(width=width)
def task_update():
    with log_errors():
        try:
            msg = messages['tasks']['deque'][-1]
        except IndexError:
            return
        task_table_update(task_source, msg)
doc.add_periodic_callback(task_update, messages['tasks']['interval'])


resource_source, resource_plot = resource_profile_plot(height=200, width=width)
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
        resource_source.stream(data, 10000)

doc.add_periodic_callback(resource_update, messages['workers']['interval'])


progress_source, progress_plot = progress_plot(height=250, width=width)
progress_plot.min_border_top -= 40
progress_plot.title = None
progress_plot.min_border_bottom -= 40
progress_plot.plot_height -= 80
progress_plot.logo = None
progress_plot.toolbar_location = None
progress_plot.xaxis.axis_label = None


def progress_update():
    with log_errors():
        msg = messages['progress']
        d = progress_quads(msg)
        progress_source.data.update(d)
doc.add_periodic_callback(progress_update, 50)


vbox = vplot(worker_table, task_table, progress_plot, resource_plot)
doc.add_root(vbox)
