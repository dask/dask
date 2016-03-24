from __future__ import print_function, division, absolute_import

from collections import defaultdict
import json
from operator import sub
from time import time

from toolz import memoize
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

from ..core import rpc
from ..utils import ignoring, is_kernel, log_errors, key_split
from ..executor import default_executor
from ..scheduler import Scheduler

try:
    from cytoolz import pluck
except ImportError:
    from toolz import pluck

with ignoring(ImportError):
    from bokeh.palettes import Spectral11
    from bokeh.models import (ColumnDataSource, HoverTool, BoxZoomTool,
            PanTool, ResetTool, ResizeTool, FactorRange)
    from bokeh.models.widgets import DataTable, TableColumn
    from bokeh.plotting import vplot, output_notebook, show, figure
    from bokeh.io import curstate, push_notebook

class Status_Monitor(object):
    """ Display the tasks running and waiting on each worker

    Parameters
    ----------
    addr: tuple, optional
        (ip, port) of scheduler.  Defaults to scheduler of recent Executor
    interval: Number, optional
        Interval between updates.  Defaults to 1s
    """
    def __init__(self, addr=None, interval=1000.00, loop=None):
        if addr is None:
            scheduler = default_executor().scheduler
            if isinstance(scheduler, rpc):
                addr = (scheduler.ip, 9786)
            elif isinstance(scheduler, Scheduler):
                addr = ('127.0.0.1', scheduler.services['http'].port)
        self.addr = addr
        self.interval = interval

        self.display_notebook = False

        if is_kernel() and not curstate().notebook:
            output_notebook()
            assert curstate().notebook

        self.task_source, self.task_table = task_table_plot()
        self.worker_source, self.worker_table = worker_table_plot()

        self.output = vplot(self.worker_table, self.task_table)

        self.client = AsyncHTTPClient()

        self.loop = loop or IOLoop.current()
        self.loop.add_callback(self.update)
        self._pc = PeriodicCallback(self.update, self.interval, io_loop=self.loop)
        self._pc.start()

    def _ipython_display_(self, **kwargs):
        show(self.output)
        self.display_notebook = True

    @gen.coroutine
    def update(self):
        """ Query the Scheduler, update the figure

        This opens a connection to the scheduler, sends it a function to run
        periodically, streams the results back and uses those results to update
        the bokeh figure
        """
        with log_errors():
            tasks, workers = yield [
                    self.client.fetch('http://%s:%d/tasks.json' % self.addr),
                    self.client.fetch('http://%s:%d/workers.json' % self.addr)]

            tasks = json.loads(tasks.body.decode())
            workers = json.loads(workers.body.decode())

            task_table_update(self.task_source, tasks)
            worker_table_update(self.worker_source, workers)

            if self.display_notebook:
                push_notebook()


def task_table_plot(row_headers=False, width=600, height=100):
    names = ['waiting', 'ready', 'failed', 'processing', 'in-memory', 'total']
    source = ColumnDataSource({k: [] for k in names})

    columns = [TableColumn(field=name, title=name) for name in names]
    table = DataTable(source=source, columns=columns,
                      row_headers=row_headers, width=width, height=height)
    return source, table


def task_table_update(source, d):
    d = {k: [v] for k, v in d.items()}
    source.data = d


def worker_table_plot(width=600, height=100, **kwargs):
    """ Column data source and plot for host table """
    names = ['workers', 'cpu', 'memory-percent', 'memory', 'cores', 'processes',
             'processing', 'latency', 'last-seen', 'disk-read', 'disk-write',
             'network-send', 'network-recv']
    source = ColumnDataSource({k: [] for k in names})

    columns = {name: TableColumn(field=name, title=name) for name in names}

    slow_names = ['workers', 'cores', 'processes', 'memory',
                  'latency', 'last-seen']
    slow = DataTable(source=source, columns=[columns[n] for n in slow_names],
                     width=width, height=height, **kwargs)

    fast_names = ['workers', 'cpu', 'memory-percent', 'processing',
            'disk-read', 'disk-write', 'network-send', 'network-recv']
    fast = DataTable(source=source, columns=[columns[n] for n in fast_names],
                     width=width, height=height, **kwargs)

    table = vplot(slow, fast)
    return source, table


def worker_table_update(source, d):
    """ Update host table source """
    workers = sorted(d, reverse=True)

    data = {}
    data['workers'] = workers
    for name in ['cores', 'cpu', 'memory-percent', 'latency', 'last-seen',
                 'memory', 'disk-read', 'disk-write', 'network-send',
                 'network-recv']:
        try:
            data[name] = [d[w][name] for w in workers]
        except KeyError:
            pass

    data['processing'] = [sorted(d[w]['processing']) for w in workers]
    data['processes'] = [len(d[w]['ports']) for w in workers]
    source.data.update(data)


def task_stream_plot(height=400, width=800, **kwargs):
    data = {'start': [], 'duration': [],
            'transfer-start': [], 'transfer-duration': [],
            'key': [], 'name': [], 'color': [],
            'worker': [], 'thread': [], 'y': []}

    source = ColumnDataSource(data)
    hover = HoverTool()
    tools = [hover, BoxZoomTool(), PanTool(), ResetTool(), ResizeTool()]
    fig = figure(width=width, height=height, x_axis_type='datetime',
                 tools=tools, **kwargs)
    fig.rect(x='start', width='duration',
             y='y', height=0.9,
             fill_color='color', line_color='gray', source=source)
    fig.xaxis.axis_label = 'Time'
    fig.yaxis.axis_label = 'Worker Core'

    hover = fig.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Key:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Duration:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@duration</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    return source, fig


import itertools
counter = itertools.count()
@memoize
def incrementing_index(o):
    return next(counter)


def task_stream_append(lists, msg, worker_threads, palette=Spectral11):
    with log_errors():
        lists['start'].append(msg['compute-start'] * 1000)
        lists['duration'].append(1000 * (msg['compute-stop']-msg['compute-start']))
        key = msg['key']
        name = key_split(key)
        color = palette[incrementing_index(name) % len(palette)]
        lists['key'].append(key)
        lists['name'].append(name)
        lists['color'].append(color)
        lists['worker'].append(msg['worker'])

        worker_thread = '%s-%d' % (msg['worker'], msg['thread'])
        lists['worker_thread'].append(worker_thread)
        worker_threads.add(worker_thread)

        if 'transfer-start' in msg:
            lists['start'].append(msg['transfer-start'] * 1000)
            lists['duration'].append(1000 * (msg['transfer-stop'] -
                                            msg['transfer-start']))

            lists['key'].append(key)
            lists['name'].append('transfer-to-' + name)
            lists['worker'].append(msg['worker'])
            lists['color'].append('red')
            lists['worker_thread'].append(worker_thread)
