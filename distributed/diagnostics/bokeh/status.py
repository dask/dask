#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import json

from bokeh.plotting import curdoc, vplot
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.diagnostics.status_monitor import (Status_Monitor,
        worker_table_plot, worker_table_update, task_table_plot,
        task_table_update)

doc = curdoc()
client = AsyncHTTPClient()

worker_source, worker_table = worker_table_plot()
@gen.coroutine
def worker_update():
    response = yield client.fetch('http://localhost:9786/workers.json')
    d = json.loads(response.body.decode())
    worker_table_update(worker_source, d)
doc.add_periodic_callback(worker_update, 1000)

task_source, task_table = task_table_plot()
@gen.coroutine
def task_update():
    response = yield client.fetch('http://localhost:9786/tasks.json')
    d = json.loads(response.body.decode())
    task_table_update(task_source, d)
doc.add_periodic_callback(task_update, 100)

doc.add_root(vplot(worker_table, task_table))
