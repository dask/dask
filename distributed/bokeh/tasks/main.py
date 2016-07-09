#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from toolz import valmap
from bokeh.io import curdoc

from distributed.bokeh.status_monitor import task_stream_plot
import distributed.bokeh

messages = distributed.bokeh.messages  # global message store
doc = curdoc()
task_stream_source, task_stream_plot = task_stream_plot(sizing_mode='stretch_both')
task_stream_index = [0]

rectangles = messages['task-events']['rectangles']
rectangles = valmap(list, rectangles)
task_stream_source.data.update(rectangles)

doc.add_root(task_stream_plot)
