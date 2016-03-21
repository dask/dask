#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from distributed.diagnostics.status_monitor import Status_Monitor

from bokeh.plotting import curdoc

monitor = Status_Monitor(('127.0.0.1', 9786))

doc = curdoc()

cb = lambda: monitor.update()
# doc.add_periodic_callback(cb, 1000)
doc.add_periodic_callback(monitor.update, monitor.interval)
