#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import json
import os
import sys

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from tornado import gen

from distributed.core import rpc
from distributed.bokeh.worker_monitor import (worker_table_plot,
        worker_table_update, processing_plot, processing_update)
from distributed.utils import log_errors
from distributed.bokeh.utils import parse_args
import distributed.bokeh

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store

options = parse_args(sys.argv[1:])

doc = curdoc()

scheduler = rpc(ip=options['host'], port=options['tcp-port'])


worker_source, [mem, table] = worker_table_plot(width=WIDTH)
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(worker_source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


"""
def f(_, old, new):
    host = worker_source.data['host']
    hosts = [host[i] for i in new['1d']['indices']]

    @gen.coroutine
    def _():
        results = yield scheduler.broadcast(hosts=hosts, msg={'op': 'health'})
        text = json.dumps(results, indent=2)
        paragraph.text = text

    doc.add_next_tick_callback(_)

worker_source.on_change('selected', f)
"""

processing_source, processing_plot = processing_plot(width=WIDTH, height=150)
def processing_plot_update():
    with log_errors():
        msg = messages['processing']
        if not msg['ncores']:
            return
        data = processing_update(msg)
        x_range = processing_plot.x_range
        max_right = max(data['right'])
        min_left = min(data['left'][:-1])
        cores = max(data['ncores'])
        if min_left < x_range.start:  # not out there enough, jump ahead
            x_range.start = min_left - 2
        elif x_range.start < 2 * min_left - cores:  # way out there, walk back
            x_range.start = x_range.start * 0.95 + min_left * 0.05
        if x_range.end < max_right:
            x_range.end = max_right + 2
        elif x_range.end > 2 * max_right + cores:  # way out there, walk back
            x_range.end = x_range.end * 0.95 + max_right * 0.05

        processing_source.data.update(data)

doc.add_periodic_callback(processing_plot_update, 200)

layout = column(
    processing_plot,
    mem,
    table,
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
