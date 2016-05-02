#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect
from toolz import valmap

from bokeh.plotting import curdoc, vplot
from distributed.bokeh.status_monitor import (task_stream_plot,
        progress_plot)
from distributed.utils import log_errors
import distributed.bokeh

messages = distributed.bokeh.messages  # global message store
doc = curdoc()
width = 800


task_stream_source, task_stream_plot = task_stream_plot(height=600,
        width=width, follow_interval=20000)
task_stream_plot.min_border_top = 0
task_stream_plot.min_border_bottom = 0
task_stream_plot.min_border_left = 0
task_stream_plot.min_border_right = 10
task_stream_plot.xaxis.axis_label = None
task_stream_index = [0]

rectangles = messages['task-events']['rectangles']
rectangles = valmap(list, rectangles)
task_stream_source.data.update(rectangles)

doc.add_root(task_stream_plot)
