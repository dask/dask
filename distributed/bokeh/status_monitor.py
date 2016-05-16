from __future__ import print_function, division, absolute_import

import json

from toolz import memoize
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop, PeriodicCallback

from ..core import rpc
from ..utils import is_kernel, log_errors, key_split
from ..executor import default_executor
from ..scheduler import Scheduler

try:
    from bokeh.palettes import Spectral11
    from bokeh.models import ColumnDataSource, DataRange1d, HoverTool
    from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
    from bokeh.plotting import vplot, output_notebook, show, figure
    from bokeh.io import curstate, push_notebook
except ImportError:
    Spectral11 = None

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


def task_table_plot(row_headers=False, width=600, height="auto"):
    names = ['waiting', 'ready', 'failed', 'processing', 'in-memory', 'total']
    source = ColumnDataSource({k: [] for k in names})

    columns = [TableColumn(field=name, title=name) for name in names]
    table = DataTable(source=source, columns=columns,
                      row_headers=row_headers, width=width, height=height)
    return source, table


def task_table_update(source, d):
    d = {k: [v] for k, v in d.items()}
    source.data = d


def worker_table_plot(width=600, height="auto", **kwargs):
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
    slow.columns[3].formatter = NumberFormatter(format='0.0 b')
    slow.columns[4].formatter = NumberFormatter(format='0.00000')
    slow.columns[5].formatter = NumberFormatter(format='0.000')

    fast_names = ['workers', 'cpu', 'memory-percent', 'processing',
            'disk-read', 'disk-write', 'network-send', 'network-recv']
    fast = DataTable(source=source, columns=[columns[n] for n in fast_names],
                     width=width, height=height, **kwargs)
    fast.columns[1].formatter = NumberFormatter(format='0.0 %')
    fast.columns[2].formatter = NumberFormatter(format='0.0 %')
    fast.columns[4].formatter = NumberFormatter(format='0 b')
    fast.columns[5].formatter = NumberFormatter(format='0 b')
    fast.columns[6].formatter = NumberFormatter(format='0 b')
    fast.columns[7].formatter = NumberFormatter(format='0 b')

    table = vplot(slow, fast)
    return source, table


def worker_table_update(source, d):
    """ Update host table source """
    workers = sorted(d)

    data = {}
    data['workers'] = workers
    for name in ['cores', 'cpu', 'memory-percent', 'latency', 'last-seen',
                 'memory', 'disk-read', 'disk-write', 'network-send',
                 'network-recv']:
        try:
            if name in ('cpu', 'memory-percent'):
                data[name] = [d[w][name] / 100 for w in workers]
            else:
                data[name] = [d[w][name] for w in workers]
        except KeyError:
            pass

    data['processing'] = [sorted(d[w]['processing']) for w in workers]
    data['processes'] = [len(d[w]['ports']) for w in workers]
    source.data.update(data)


def task_stream_plot(height=400, width=800, follow_interval=5000, **kwargs):
    data = {'start': [], 'duration': [],
            'key': [], 'name': [], 'color': [],
            'worker': [], 'y': [], 'worker_thread': [], 'alpha': []}

    source = ColumnDataSource(data)
    if follow_interval:
        x_range = DataRange1d(follow='end', follow_interval=follow_interval,
                              range_padding=0)
    else:
        x_range = None

    fig = figure(width=width, height=height, x_axis_type='datetime',
                 tools=['xwheel_zoom', 'xpan', 'reset', 'resize', 'box_zoom'],
                 responsive=True, x_range=x_range, **kwargs)
    fig.rect(x='start', y='y', width='duration', height=0.9,
             fill_color='color', line_color='gray', alpha='alpha',
             source=source)
    if x_range:
        fig.circle(x=[1, 2], y=[1, 2], alpha=0.0)
    fig.xaxis.axis_label = 'Time'
    fig.yaxis.axis_label = 'Worker Core'
    fig.min_border_right = 10
    fig.ygrid.grid_line_alpha = 0.4
    fig.xgrid.grid_line_alpha = 0.0

    hover = HoverTool()
    fig.add_tools(hover)
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


def task_stream_append(lists, msg, workers, palette=Spectral11):
    start, stop = msg['compute_start'], msg['compute_stop']
    lists['start'].append((start + stop) / 2 * 1000)
    lists['duration'].append(1000 * (stop - start))
    key = msg['key']
    name = key_split(key)
    if msg['status'] == 'OK':
        color = palette[incrementing_index(name) % len(palette)]
    else:
        color = 'black'
    lists['key'].append(key)
    lists['name'].append(name)
    lists['color'].append(color)
    lists['alpha'].append(1)
    lists['worker'].append(msg['worker'])

    worker_thread = '%s-%d' % (msg['worker'], msg['thread'])
    lists['worker_thread'].append(worker_thread)
    if worker_thread not in workers:
        workers[worker_thread] = len(workers)
    lists['y'].append(workers[worker_thread])

    if msg.get('transfer_start') is not None:
        start, stop = msg['transfer_start'], msg['transfer_stop']
        lists['start'].append((start + stop) / 2 * 1000)
        lists['duration'].append(1000 * (stop - start))

        lists['key'].append(key)
        lists['name'].append('transfer-to-' + name)
        lists['worker'].append(msg['worker'])
        lists['color'].append('red')
        lists['alpha'].append('0.8')
        lists['worker_thread'].append(worker_thread)
        lists['y'].append(workers[worker_thread])


def progress_plot(height=300, width=800, **kwargs):
    from ..diagnostics.progress_stream import progress_quads
    data = progress_quads({'all': {}, 'in_memory': {},
                           'erred': {}, 'released': {}})

    source = ColumnDataSource(data)
    fig = figure(width=width, height=height, tools=['resize'],
                 responsive=True, **kwargs)
    fig.quad(source=source, top='top', bottom='bottom',
             left=0, right=1, color='#aaaaaa', alpha=0.2)
    fig.quad(source=source, top='top', bottom='bottom',
             left=0, right='released_right', color='#0000FF', alpha=0.4)
    fig.quad(source=source, top='top', bottom='bottom',
             left='released_right', right='in_memory_right',
             color='#0000FF', alpha=0.8)
    fig.quad(source=source, top='top', bottom='bottom',
             left='erred_left', right=1,
             color='#000000', alpha=0.3)
    fig.text(source=source, text='fraction', y='center', x=-0.01,
             text_align='right', text_baseline='middle')
    fig.text(source=source, text='name', y='center', x=1.01,
             text_align='left', text_baseline='middle')
    fig.scatter(x=[-0.2, 1.4], y=[0, 5], alpha=0)
    fig.xgrid.grid_line_color = None
    fig.ygrid.grid_line_color = None
    fig.axis.visible = None
    fig.min_border_left = 0
    fig.min_border_right = 10
    fig.min_border_top = 0
    fig.min_border_bottom = 0
    fig.outline_line_color = None

    hover = HoverTool()
    fig.add_tools(hover)
    hover = fig.select(HoverTool)
    hover.tooltips = """
    <div>
        <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">All:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@all</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">In Memory:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@in_memory</span>
    </div>
    <div>
        <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
        <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
    </div>
    """
    hover.point_policy = 'follow_mouse'

    return source, fig
