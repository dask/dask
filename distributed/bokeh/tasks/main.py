#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh.components import TaskStream
import distributed.bokeh

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

task_stream = TaskStream(100000, sizing_mode='stretch_both',
                         clear_interval=60*1000)
task_stream.update(messages)

doc.add_periodic_callback(lambda: task_stream.update(messages), 2000)

doc.add_root(task_stream.root)
