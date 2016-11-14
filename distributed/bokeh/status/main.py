#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc
from bokeh.layouts import column, row

from distributed.bokeh.components import (
    TaskStream, TaskProgress, MemoryUsage, ResourceProfiles
    )
import distributed.bokeh

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store

doc = curdoc()

task_stream = TaskStream(1000, sizing_mode=SIZING_MODE, width=WIDTH, height=300, clear_interval=10000)
doc.add_periodic_callback(lambda: task_stream.update(messages), messages['task-events']['interval'])

task_progress = TaskProgress(sizing_mode=SIZING_MODE, width=WIDTH, height=160)
doc.add_periodic_callback(lambda: task_progress.update(messages), 50)

memory_usage = MemoryUsage(sizing_mode=SIZING_MODE, width=WIDTH, height=60)
doc.add_periodic_callback(lambda: memory_usage.update(messages), 50)

resource_profiles = ResourceProfiles(sizing_mode=SIZING_MODE, width=WIDTH, height=80)
doc.add_periodic_callback(lambda: resource_profiles.update(messages), messages['task-events']['interval'])

layout = column(
    row(resource_profiles.root, sizing_mode=SIZING_MODE),
    row(memory_usage.root, sizing_mode=SIZING_MODE),
    row(task_stream.root, sizing_mode=SIZING_MODE),
    row(task_progress.root, sizing_mode=SIZING_MODE),
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
