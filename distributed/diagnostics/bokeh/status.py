#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import json

from bokeh.plotting import curdoc, show
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.utils import log_errors
from distributed.diagnostics.status_monitor import (Status_Monitor,
        worker_table_plot, worker_table_update)

doc = curdoc()
client = AsyncHTTPClient()

worker_source, worker_table = worker_table_plot()
@gen.coroutine
def worker_update():
    with log_errors():
        response = yield client.fetch('http://localhost:9786/workers.json')
        d = json.loads(response.body.decode())
        worker_table_update(worker_source, d)
        print(worker_source.data)
doc.add_root(worker_table)


doc.add_periodic_callback(worker_update, 1000)
