#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc
from bokeh.layouts import column, row

import distributed.bokeh

from distributed.bokeh.components import ProcessingStacks, WorkerTable

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

processing_stacks = ProcessingStacks(sizing_mode=SIZING_MODE, width=WIDTH, height=150)
doc.add_periodic_callback(lambda: processing_stacks.update(messages), 200)

worker_table = WorkerTable(sizing_mode=SIZING_MODE, width=WIDTH)
doc.add_periodic_callback(lambda: worker_table.update(messages), messages['workers']['interval'])

layout = column(
    row(processing_stacks.root, sizing_mode=SIZING_MODE),
    row(worker_table.root, sizing_mode=SIZING_MODE),
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
