from __future__ import print_function, division, absolute_import

from collections import defaultdict
import json
from operator import sub
from time import time
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
    from bokeh.models import ColumnDataSource
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


def task_stream_plot(height=400, width=1000, **kwargs):
    data = {'compute-start': [], 'compute-duration': [],
            'transfer-start': [], 'transfer-duration': [],
            'key': [], 'key-prefix': [],
            'worker': [], 'thread': [], 'worker-position': []}

    source = ColumnDataSource(data)
    fig = figure(width=width, height=height, x_axis_type='datetime', **kwargs)
    fig.rect(x='compute-start', width='compute-duration',
             y='worker-position', height=0.9, line_color='gray', source=source)
    return source, fig


def task_stream_update(source, plot, msgs):
    if not msgs:
        return
    if source.data['key'] and msgs[-1]['key'] == source.data['key'][-1]:  # no change
        return

    data = dict()
    data['compute-start'] = [msg['compute-start'] * 1000 for msg in msgs]
    data['compute-duration'] = [1000 * (msg['compute-stop']
                                      - msg['compute-start']) for msg in msgs]
    data['key'] = list(pluck('key', msgs))
    data['key-prefix'] = list(map(key_split, data['key']))
    data['worker'] = list(pluck('worker', msgs))

    workers = list(pluck('worker', msgs))
    threads = list(pluck('thread', msgs))
    sorted_workers = sorted(set(zip(workers, threads)))
    data['worker'] = workers
    data['worker-position'] = list(map(sorted_workers.index,
                                        zip(workers, threads)))
    source.data.update(data)

    plot.y_range.end = len(sorted_workers)
