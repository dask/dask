#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import json
import os

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from tornado import gen

from distributed.core import rpc
from distributed.bokeh.worker_monitor import worker_table_plot, worker_table_update
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'scale_width'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()


dask_dir = os.path.join(os.path.expanduser('~'), '.dask')
options_path = os.path.join(dask_dir, '.dask-web-ui.json')
if os.path.exists(options_path):
    with open(options_path, 'r') as f:
        options = json.load(f)
else:
    options = {'host': '127.0.0.1',
               'tcp-port': 8786,
               'http-port': 9786}

scheduler = rpc(ip=options['host'], port=options['tcp-port'])


source, [mem, table] = worker_table_plot(width=WIDTH)
def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(source, msg)
doc.add_periodic_callback(worker_update, messages['workers']['interval'])


"""
def f(_, old, new):
    host = source.data['host']
    hosts = [host[i] for i in new['1d']['indices']]

    @gen.coroutine
    def _():
        results = yield scheduler.broadcast(hosts=hosts, msg={'op': 'health'})
        text = json.dumps(results, indent=2)
        paragraph.text = text

    doc.add_next_tick_callback(_)

source.on_change('selected', f)
"""

layout = column(
    mem,
    table,
    sizing_mode=SIZING_MODE
)
doc.add_root(layout)
