#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from collections import deque
import json
import os
from time import time

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.iostream import StreamClosedError
from tornado.ioloop import IOLoop

from distributed.core import read
from distributed.diagnostics.eventstream import eventstream
from distributed.bokeh.status_monitor import task_stream_append
import distributed.bokeh
from distributed.utils import log_errors

client = AsyncHTTPClient()

messages = distributed.bokeh.messages  # monkey-patching


if os.path.exists('.dask-web-ui.json'):
    with open('.dask-web-ui.json', 'r') as f:
        options = json.load(f)
else:
    options = {'host': '127.0.0.1',
               'tcp-port': 8786,
               'http-port': 9786}



@gen.coroutine
def task_events(interval, deque, times, index, rectangles, workers, last_seen):
    i = 0
    with log_errors():
        stream = yield eventstream('%(host)s:%(tcp-port)d' % options, 0.100)
        while True:
            try:
                msgs = yield read(stream)
            except StreamClosedError:
                break
            else:
                if not msgs:
                    continue

                last_seen[0] = time()
                for msg in msgs:
                    if 'compute_start' in msg:
                        deque.append(msg)
                        times.append(msg['compute_start'])
                        index.append(i); i += 1
                        if msg.get('transfer_start') is not None:
                            index.append(i); i += 1
                        task_stream_append(rectangles, msg, workers)


n = 100000

def on_server_loaded(server_context):
    messages['task-events'] = {'interval': 200,
                               'deque': deque(maxlen=n),
                               'times': deque(maxlen=n),
                               'index': deque(maxlen=n),
                               'rectangles':{name: deque(maxlen=n) for name in
                                            'start duration key name color worker worker_thread y alpha'.split()},
                               'workers': dict(),
                               'last_seen': [time()]}
    IOLoop.current().add_callback(task_events, **messages['task-events'])
