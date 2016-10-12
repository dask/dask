#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from distributed.bokeh.status_monitor import (progress_plot, task_stream_plot,
        nbytes_plot)
from distributed.bokeh.worker_monitor import resource_profile_plot
from distributed.diagnostics.progress_stream import progress_quads, nbytes_bar
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()


resource_index = [0]
resource_source, resource_plot, network_plot, combo_toolbar = resource_profile_plot(sizing_mode=SIZING_MODE, width=WIDTH, height=80)
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


nbytes_task_source, nbytes_task_plot = nbytes_plot(sizing_mode=SIZING_MODE,
        width=WIDTH, height=60)

progress_source, progress_plot = progress_plot(sizing_mode=SIZING_MODE,
        width=WIDTH, height=160)
def progress_update():
    with log_errors():
        msg = messages['progress']
        if not msg:
            return
        d = progress_quads(msg)
        progress_source.data.update(d)
        if messages['tasks']['deque']:
            progress_plot.title.text = ("Progress -- total: %(total)s, "
                "in-memory: %(in-memory)s, processing: %(processing)s, "
                "ready: %(ready)s, waiting: %(waiting)s, failed: %(failed)s"
                % messages['tasks']['deque'][-1])

        nb = nbytes_bar(msg['nbytes'])
        nbytes_task_source.data.update(nb)
        nbytes_task_plot.title.text = \
                "Memory Use: %0.2f MB" % (sum(msg['nbytes'].values()) / 1e6)
doc.add_periodic_callback(progress_update, 50)


task_stream_index = [0]
task_stream_source, task_stream_plot = task_stream_plot(
        sizing_mode=SIZING_MODE, width=WIDTH, height=300)
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
    row(nbytes_task_plot, sizing_mode=SIZING_MODE),
    row(task_stream_plot, sizing_mode=SIZING_MODE),
    row(progress_plot, sizing_mode=SIZING_MODE),
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
