#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc
from bokeh.layouts import column, row

import distributed.bokeh

from distributed.bokeh.components import Processing, WorkerTable

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

processing = Processing(sizing_mode=SIZING_MODE, width=WIDTH, height=150)
doc.add_periodic_callback(lambda: processing.update(messages), 200)

worker_table = WorkerTable(sizing_mode=SIZING_MODE, width=WIDTH, plot_height=80)
doc.add_periodic_callback(lambda: worker_table.update(messages), messages['workers']['interval'])

layout = column(
    row(processing.root, sizing_mode=SIZING_MODE),
    row(worker_table.root, sizing_mode=SIZING_MODE),
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
