from __future__ import print_function, division, absolute_import

from collections import defaultdict
import json
import time
from tornado import gen

from ..core import rpc
from ..utils import ignoring, is_kernel
from ..executor import default_executor
from ..scheduler import Scheduler

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

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
    def __init__(self, addr=None, interval=1000.00):
        if addr is None:
            scheduler = default_executor().scheduler
            if isinstance(scheduler, rpc):
                addr = (scheduler.ip, scheduler.port) # doesn't work
            elif isinstance(scheduler, Scheduler):
                addr = ('127.0.0.1', scheduler.services['http'].port)
        self.addr = addr
        self.interval = interval

        self.display_notebook = False

        if is_kernel() and not curstate().notebook:
            output_notebook()
            assert curstate().notebook

        self.task_source, self.task_table = task_table_plot()

        ts_props = ['time','workers', 'nbytes']
        # self.ts_source = ColumnDataSource({k: [[0,1],[0,1]] for k in ts_props})
        self.ts_source = ColumnDataSource({k: [] for k in ts_props})
        self.plot = figure(height=400, width=600, x_axis_type='datetime')
        self.plot.multi_line(xs='time', ys='nbytes',
                             color=Spectral11,
                             source=self.ts_source)

        # For managing monitor state
        self.nbytes = defaultdict(list)
        self.times = []
        self.start = time.time()

        self.output = vplot(self.task_table, self.plot)

        self.client = AsyncHTTPClient()

        loop = IOLoop.current()
        loop.add_callback(self.update)
        pc = PeriodicCallback(self.update, self.interval, io_loop=loop)
        pc.start()

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

        response = yield self.client.fetch('http://%s:%d/tasks.json' % self.addr)
        d = json.loads(response.body.decode())

        task_table_update(self.task_source, d)

        for k,v in d['bytes'].items():
            self.nbytes[k].append(v)
        self.times.append(time.time() * 1000 - self.start * 1000) #bokeh uses msse

        self.ts_source.data['time'] = [self.times for k in self.nbytes.keys()]
        self.ts_source.data['nbytes'] = [v for k, v in sorted(self.nbytes.items(),
                                         key=lambda x: x[0], reverse=True)]

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
    names = ['workers', 'cpu', 'available-memory', 'cores', 'processes',
             'processing', 'latency', 'last-seen']
    source = ColumnDataSource({k: [] for k in names})

    columns = [TableColumn(field=name, title=name) for name in names]
    table = DataTable(source=source, columns=columns, width=width,
                      height=height, **kwargs)

    return source, table


def worker_table_update(source, d):
    """ Update host table source """
    source.data['time'] = [time.time()]
    workers = sorted(d, reverse=True)

    source.data['workers'] = workers
    for name in ['cores', 'cpu', 'available-memory', 'latency', 'last-seen',
                 'total-memory']:
        source.data[name] = [d[w][name] for w in workers]

    source.data['processing'] = [sorted(d[w]['processing']) for w in workers]
    source.data['processes'] = [len(d[w]['ports']) for w in workers]
