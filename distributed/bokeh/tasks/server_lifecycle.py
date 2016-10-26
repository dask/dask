#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from collections import deque
import sys
import json
import os
import logging
import sys
from time import time

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.iostream import StreamClosedError
from tornado.ioloop import IOLoop

from distributed.core import read
from distributed.diagnostics.eventstream import eventstream
from distributed.diagnostics.progress_stream import task_stream_append
import distributed.bokeh
from distributed.bokeh.utils import parse_args


logger = logging.getLogger(__name__)

client = AsyncHTTPClient()

messages = distributed.bokeh.messages  # monkey-patching

options = parse_args(sys.argv[1:])


@gen.coroutine
def task_events(interval, deque, times, index, rectangles, workers, last_seen):
    i = 0
    try:
        stream = yield eventstream('%(host)s:%(tcp-port)d' % options, 0.100)
        while True:
            msgs = yield read(stream)
            if not msgs:
                continue

            last_seen[0] = time()
            for msg in msgs:
                if 'compute_start' in msg:
                    deque.append(msg)
                    times.append(msg['compute_start'])
                    index.append(i)
                    i += 1
                    if msg.get('transfer_start') is not None:
                        index.append(i)
                        i += 1
                    if msg.get('disk_load_start') is not None:
                        index.append(i)
                        i += 1
                    task_stream_append(rectangles, msg, workers)
    except StreamClosedError:
        pass  # don't log StreamClosedErrors
    except Exception as e:
        logger.exception(e)
    finally:
        try:
            sys.exit(0)
        except:
            pass


n = 100000

def on_server_loaded(server_context):
    IOLoop.current().add_callback(task_events, **messages['task-events'])
