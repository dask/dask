#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect
from toolz import valmap

from bokeh.plotting import curdoc, vplot
from distributed.diagnostics.status_monitor import (task_stream_plot,
        progress_plot)
from distributed.utils import log_errors
import distributed.diagnostics.bokeh

messages = distributed.diagnostics.bokeh.messages  # global message store
doc = curdoc()
width = 800


task_stream_source, task_stream_plot = task_stream_plot(height=800, width=width)
task_stream_plot.min_border_top = 0
task_stream_plot.min_border_bottom = 0
task_stream_plot.min_border_left = 0
task_stream_plot.min_border_right = 0
task_stream_plot.xaxis.axis_label = None
task_stream_index = [0]
def task_stream_update():
    with log_errors():
        index = messages['task-events']['index']
        if not index:
            return
        if index[-1] == task_stream_index[0]:
            return

        ind = bisect(index, task_stream_index[0])
        rectangles = messages['task-events']['rectangles']
        if task_stream_index == [0]:
            rectangles = valmap(list, rectangles)
        rectangles = {k: [v[i] for i in range(ind, len(index))]
                      for k, v in rectangles.items()}
        task_stream_index[0] = index[-1]

        workers = messages['task-events']['workers']
        workers = {w: i for i, w in enumerate(sorted(workers, reverse=True))}
        rectangles['y'] = [workers[wt] for wt in rectangles['worker_thread']]
        task_stream_source.stream(rectangles, 20000)

doc.add_periodic_callback(task_stream_update, messages['task-events']['interval'])

doc.add_root(task_stream_plot)
