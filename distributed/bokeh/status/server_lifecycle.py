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
from distributed.diagnostics.progress_stream import progress_stream
from distributed.bokeh.worker_monitor import resource_append
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
def http_get(route):
    """ Get data from JSON route, store in messages deques """
    with log_errors():
        try:
            response = yield client.fetch(
                    'http://%(host)s:%(http-port)d/' % options
                     + route + '.json')
        except ConnectionRefusedError:
            import sys; sys.exit(0)
        msg = json.loads(response.body.decode())
        messages[route]['deque'].append(msg)
        messages[route]['times'].append(time())


last_index = [0]
@gen.coroutine
def workers():
    """ Get data from JSON route, store in messages deques """
    with log_errors():
        response = yield client.fetch(
                'http://%(host)s:%(http-port)d/workers.json' % options)
        msg = json.loads(response.body.decode())
        if msg:
            messages['workers']['deque'].append(msg)
            messages['workers']['times'].append(time())
            resource_append(messages['workers']['plot-data'], msg)
            index = messages['workers']['index']
            index.append(last_index[0] + 1)
            last_index[0] += 1


@gen.coroutine
def progress():
    with log_errors():
        stream = yield progress_stream('%(host)s:%(tcp-port)d' % options, 0.050)
        while True:
            try:
                msg = yield read(stream)
            except StreamClosedError:
                break
            else:
                messages['progress'] = msg


def on_server_loaded(server_context):
    n = 60
    messages['workers'] = {'interval': 500,
                           'deque': deque(maxlen=n),
                           'times': deque(maxlen=n),
                           'index': deque(maxlen=n),
                           'plot-data': {'time': deque(maxlen=n),
                                         'cpu': deque(maxlen=n),
                                         'memory-percent': deque(maxlen=n),
                                         'network-send': deque(maxlen=n),
                                         'network-recv': deque(maxlen=n)}}
    server_context.add_periodic_callback(workers, 500)

    messages['tasks'] = {'interval': 100,
                         'deque': deque(maxlen=100),
                         'times': deque(maxlen=100)}
    server_context.add_periodic_callback(lambda: http_get('tasks'), 100)

    messages['progress'] = {'all': {}, 'in_memory': {},
                            'erred': {}, 'released': {}}

    IOLoop.current().add_callback(progress)
