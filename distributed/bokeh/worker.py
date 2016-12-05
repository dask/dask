from __future__ import print_function, division, absolute_import

from functools import partial
import logging

from bokeh.layouts import row, column, widgetbox
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem, BoxSelectTool,
    Circle, CategoricalAxis, Select
)
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
from bokeh.palettes import Spectral9, RdBu11
from bokeh.plotting import figure
from toolz import frequencies

from .components import DashboardComponent
from ..compatibility import WINDOWS
from ..diagnostics.progress_stream import color_of
from ..metrics import time
from ..utils import log_errors, key_split


red_blue = sorted(RdBu11)[::-1]


logger = logging.getLogger(__name__)


class StateTable(DashboardComponent):
    """ Currently running tasks """
    def __init__(self, worker):
        self.worker = worker

        names = ['Stored', 'Executing', 'Ready', 'Waiting', 'Connections', 'Serving']
        self.source = ColumnDataSource({name: [] for name in names})

        columns = {name: TableColumn(field=name, title=name)
                   for name in names}

        table = DataTable(
            source=self.source, columns=[columns[n] for n in names],
            height=70,
        )
        self.root = table

    def update(self):
        with log_errors():
            w = self.worker
            d = {'Stored': [len(w.data)],
                 'Executing': ['%d / %d' % (len(w.executing), w.ncores)],
                 'Ready': [len(w.ready)],
                 'Waiting': [len(w.waiting_for_data)],
                 'Connections': [len(w.connections)],
                 'Serving': [len(w._listen_streams)]}
            self.source.data.update(d)


class CommunicatingStream(DashboardComponent):
    def __init__(self, worker, height=300, **kwargs):
        with log_errors():
            self.worker = worker
            names = ['start', 'stop', 'middle', 'duration', 'who', 'y',
                    'hover', 'alpha', 'bandwidth', 'total' ]

            self.incoming = ColumnDataSource({name: [] for name in names})
            self.outgoing = ColumnDataSource({name: [] for name in names})

            x_range = DataRange1d(range_padding=0)
            y_range = DataRange1d(range_padding=0)

            fig= figure(title='Peer Communications',
                    x_axis_type='datetime', x_range=x_range, y_range=y_range,
                    height=height, tools='', **kwargs)

            fig.rect(source=self.incoming, x='middle', y='y', width='duration',
                     height=0.9, color='red', alpha='alpha')
            fig.rect(source=self.outgoing, x='middle', y='y', width='duration',
                     height=0.9, color='blue', alpha='alpha')

            hover = HoverTool(
                point_policy="follow_mouse",
                tooltips="""@hover"""
            )
            fig.add_tools(
                hover,
                ResetTool(reset_size=False),
                PanTool(dimensions="width"),
                WheelZoomTool(dimensions="width")
            )

            self.root = fig

            self.last_incoming = 0
            self.last_outgoing = 0
            self.who = dict()

    def update(self):
        with log_errors():
            outgoing = self.worker.outgoing_transfer_log
            n = self.worker.outgoing_count - self.last_outgoing
            outgoing = [outgoing[-i].copy() for i in range(1, n + 1)]
            self.last_outgoing = self.worker.outgoing_count

            incoming = self.worker.incoming_transfer_log
            n = self.worker.incoming_count - self.last_incoming
            incoming = [incoming[-i].copy() for i in range(1, n + 1)]
            self.last_incoming = self.worker.incoming_count

            for [msgs, source] in [[incoming, self.incoming],
                                   [outgoing, self.outgoing]]:

                for msg in msgs:
                    if 'compressed' in msg:
                        del msg['compressed']
                    del msg['keys']

                    bandwidth = msg['total'] / (msg['duration'] or 0.5)
                    bw = max(min(bandwidth / 500e6, 1), 0.3)
                    msg['alpha'] = bw
                    try:
                        msg['y'] = self.who[msg['who']]
                    except KeyError:
                        self.who[msg['who']] = len(self.who)
                        msg['y'] = self.who[msg['who']]

                    msg['hover'] = '%s / %s = %s/s' % (
                                format_bytes(msg['total']),
                                format_time(msg['duration']),
                                format_bytes(msg['total'] / msg['duration']))

                    for k in ['middle', 'duration', 'start', 'stop']:
                        msg[k] = msg[k] * 1000

                if msgs:
                    msgs = transpose(msgs)
                    if (len(source.data['stop']) and
                        min(msgs['start']) > source.data['stop'][-1] + 10000):
                        source.data.update(msgs)
                    else:
                        source.stream(msgs, rollover=1000)


class CommunicatingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'in': [], 'out': []})

        x_range = DataRange1d(follow='end', follow_interval=10000, range_padding=0)

        fig = figure(title="Communication History",
                     x_axis_type='datetime',
                     y_range=[-0.1, worker.total_connections + 0.5],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='in', color='red')
        fig.line(source=self.source, x='x', y='out', color='blue')

        fig.add_tools(
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'out': [len(self.worker._listen_streams)],
                                'in': [len(self.worker.connections)]}, 1000)


class ExecutingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'y': []})

        x_range = DataRange1d(follow='end', follow_interval=10000, range_padding=0)

        fig = figure(title="Executing History",
                     x_axis_type='datetime', y_range=[-0.1, worker.ncores + 0.1],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='y')

        fig.add_tools(
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'y': [len(self.worker.executing)]}, 1000)


class CrossFilter(DashboardComponent):
    def __init__(self, worker, **kwargs):
        with log_errors():
            self.worker = worker

            names = ['nbytes', 'duration', 'bandwidth', 'count', 'type',
                     'inout-color', 'type-color', 'key', 'key-color', 'start',
                     'stop']
            quantities = ['nbytes', 'duration', 'bandwidth', 'count',
                          'start', 'stop']
            colors = ['inout-color', 'type-color', 'key-color']

            # self.source = ColumnDataSource({name: [] for name in names})
            self.source = ColumnDataSource({
                'nbytes': [1, 2],
                'duration': [0.01, 0.02],
                'bandwidth': [0.01, 0.02],
                'count': [1, 2],
                'type': ['int', 'str'],
                'inout-color': ['blue', 'red'],
                'type-color': ['blue', 'red'],
                'key': ['add', 'inc'],
                'start': [1, 2],
                'stop': [1, 2]
                })



            self.x = Select(title='X-Axis', value='nbytes', options=quantities)
            self.x.on_change('value', self.update_figure)

            self.y = Select(title='Y-Axis', value='bandwidth', options=quantities)
            self.y.on_change('value', self.update_figure)

            self.size = Select(title='Size', value='None',
                               options=['None'] + quantities)
            self.size.on_change('value', self.update_figure)

            self.color = Select(title='Color', value='type-color',
                                options=['black'] + colors)
            self.color.on_change('value', self.update_figure)

            if 'sizing_mode' in kwargs:
                kw = {'sizing_mode': kwargs['sizing_mode']}
            else:
                kw = {}

            self.control = widgetbox([self.x, self.y, self.size, self.color],
                                     width=200, **kw)

            self.last_outgoing = 0
            self.last_incoming = 0
            self.kwargs = kwargs

            self.layout = row(self.control, self.create_figure(**self.kwargs),
                              **kw)

            self.root = self.layout

    def update(self):
        with log_errors():
            outgoing = self.worker.outgoing_transfer_log
            n = self.worker.outgoing_count - self.last_outgoing
            n = min(n, 1000)
            outgoing = [outgoing[-i].copy() for i in range(1, n)]
            self.last_outgoing = self.worker.outgoing_count

            incoming = self.worker.incoming_transfer_log
            n = self.worker.incoming_count - self.last_incoming
            n = min(n, 1000)
            incoming = [incoming[-i].copy() for i in range(1, n)]
            self.last_incoming = self.worker.incoming_count

            out = []

            for msg in incoming:
                if msg['keys']:
                    d = self.process_msg(msg)
                    d['inout-color'] = 'red'
                    out.append(d)

            for msg in outgoing:
                if msg['keys']:
                    d = self.process_msg(msg)
                    d['inout-color'] = 'blue'
                    out.append(d)

            if out:
                out = transpose(out)
                if (len(self.source.data['stop']) and
                    min(out['start']) > self.source.data['stop'][-1] + 10):
                    self.source.data.update(out)
                else:
                    self.source.stream(out, rollover=1000)

    def create_figure(self, **kwargs):
        with log_errors():
            fig = figure(title='', tools='', **kwargs)

            size = self.size.value
            if size == 'None':
                size = 1

            fig.circle(source=self.source, x=self.x.value, y=self.y.value,
                       color=self.color.value, size=10, alpha=0.5,
                       hover_alpha=1)
            fig.xaxis.axis_label = self.x.value
            fig.yaxis.axis_label = self.y.value

            fig.add_tools(
                # self.hover,
                ResetTool(reset_size=False),
                PanTool(),
                WheelZoomTool(),
                BoxZoomTool(),
            )
            return fig

    def update_figure(self, attr, old, new):
        with log_errors():
            fig = self.create_figure(**self.kwargs)
            self.layout.children[1] = fig

    def process_msg(self, msg):
        try:
            func = lambda k: msg['keys'].get(k, 0)
            main_key = max(msg['keys'], key=func)
            typ = self.worker.types.get(main_key, object).__name__
            keyname = key_split(main_key)
            d = {
                  'nbytes': msg['total'],
                  'duration': msg['duration'],
                  'bandwidth': msg['bandwidth'],
                  'count': len(msg['keys']),
                  'type': typ,
                  'type-color': color_of(typ),
                  'key': keyname,
                  'key-color': color_of(keyname),
                  'start': msg['start'],
                  'stop': msg['stop']
                 }
            return d
        except Exception as e:
            logger.exception(e)
            raise


class SystemMonitor(DashboardComponent):
    def __init__(self, worker, height=150, **kwargs):
        self.worker = worker

        self.names = ['cpu', 'memory', 'read_bytes', 'write_bytes', 'time']
        if not WINDOWS:
            self.names.append('num_fds')
        self.source = ColumnDataSource({name: [] for name in self.names})

        x_range = DataRange1d(follow='end', follow_interval=10000,
                              range_padding=0)

        tools = 'reset,pan,wheel_zoom'

        self.cpu = figure(title="CPU", x_axis_type='datetime',
                          y_range=[-0.1, 100 * self.worker.ncores],
                          height=height, tools=tools, x_range=x_range, **kwargs)
        self.cpu.line(source=self.source, x='time', y='cpu')
        self.cpu.yaxis.axis_label = 'Percentage'
        self.mem = figure(title="Memory", x_axis_type='datetime',
                          height=height, tools=tools, x_range=x_range, **kwargs)
        self.mem.line(source=self.source, x='time', y='memory')
        self.mem.yaxis.axis_label = 'Bytes'
        self.bandwidth = figure(title='Bandwidth', x_axis_type='datetime',
                                height=height,
                                x_range=x_range, tools=tools, **kwargs)
        self.bandwidth.line(source=self.source, x='time', y='read_bytes',
                            color='red')
        self.bandwidth.line(source=self.source, x='time', y='write_bytes',
                            color='blue')
        plots = [self.cpu, self.mem, self.bandwidth]

        if not WINDOWS:
            self.num_fds = figure(title='Number of File Descriptors',
                                  x_axis_type='datetime', height=height,
                                  x_range=x_range, tools=tools, **kwargs)

            self.num_fds.line(source=self.source, x='time', y='num_fds')
            plots.append(self.num_fds)

        if 'sizing_mode' in kwargs:
            kw = {'sizing_mode': kwargs['sizing_mode']}
        else:
            kw = {}

        self.last = 0
        self.root = column(*plots, **kw)
        self.worker.monitor.update()

    def update(self):
        with log_errors():
            monitor = self.worker.monitor

            n = monitor.count - self.last
            if not n:
                return

            seq = [-i for i in range(1, n + 1)][::-1]

            d = {attr: [getattr(monitor, attr)[i] for i in seq]
                 for attr in self.names}

            d['time'] = [monitor.time[i] * 1000 for i in seq]

            self.source.stream(d, 1000)
            if not WINDOWS:
                self.num_fds.y_range.start = 0
            self.mem.y_range.start = 0
            self.last = monitor.count


from bokeh.server.server import Server
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application


def main_doc(worker, doc):
    with log_errors():
        statetable = StateTable(worker)
        executing_ts = ExecutingTimeSeries(worker, sizing_mode='scale_width')
        communicating_ts = CommunicatingTimeSeries(worker,
                sizing_mode='scale_width')
        communicating_stream = CommunicatingStream(worker,
                sizing_mode='scale_width')

        xr = executing_ts.root.x_range
        communicating_ts.root.x_range = xr
        communicating_stream.root.x_range = xr

        doc.add_periodic_callback(statetable.update, 200)
        doc.add_periodic_callback(executing_ts.update, 200)
        doc.add_periodic_callback(communicating_ts.update, 200)
        doc.add_periodic_callback(communicating_stream.update, 200)
        doc.add_root(column(statetable.root,
                            executing_ts.root,
                            communicating_ts.root,
                            communicating_stream.root,
                            sizing_mode='scale_width'))


def crossfilter_doc(worker, doc):
    with log_errors():
        statetable = StateTable(worker)
        crossfilter = CrossFilter(worker)

        doc.add_periodic_callback(statetable.update, 200)
        doc.add_periodic_callback(crossfilter.update, 500)

        doc.add_root(column(statetable.root, crossfilter.root))


def systemmonitor_doc(worker, doc):
    with log_errors():
        sysmon = SystemMonitor(worker, sizing_mode='scale_width')
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(sysmon.root)


class BokehWorker(object):
    def __init__(self, worker, io_loop=None):
        self.worker = worker
        main = Application(FunctionHandler(partial(main_doc, worker)))
        crossfilter = Application(FunctionHandler(partial(crossfilter_doc, worker)))
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc, worker)))
        self.apps = {'/main': main,
                     '/crossfilter': crossfilter,
                     '/system': systemmonitor}

        self.loop = io_loop or worker.loop
        self.server = None

    def listen(self, port):
        if self.server:
            return
        for i in range(5):
            try:
                self.server = Server(self.apps, io_loop=self.loop, port=port,
                                     host=['*'])
                self.server.start(start_loop=False)
                break
            except (SystemExit, EnvironmentError):
                port = 0
                if i == 4:
                    raise

    @property
    def port(self):
        return (self.server.port or
                list(self.server._http._sockets.values())[0].getsockname()[1])

    def stop(self):
        for context in self.server._tornado._applications.values():
            context.run_unload_hook()

        self.server._tornado._stats_job.stop()
        self.server._tornado._cleanup_job.stop()
        if self.server._tornado._ping_job is not None:
            self.server._tornado._ping_job.stop()

        # self.server.stop()
        # https://github.com/bokeh/bokeh/issues/5494


def transpose(lod):
    keys = list(lod[0].keys())
    return {k: [d[k] for d in lod] for k in keys}


def format_bytes(n):
    """ Format bytes as text

    >>> format_bytes(1)
    '1 B'
    >>> format_bytes(1234)
    '1.23 kB'
    >>> format_bytes(12345678)
    '12.35 MB'
    >>> format_bytes(1234567890)
    '1.23 GB'
    """
    if n > 1e9:
        return '%0.2f GB' % (n / 1e9)
    if n > 1e6:
        return '%0.2f MB' % (n / 1e6)
    if n > 1e3:
        return '%0.2f kB' % (n / 1000)
    return '%d B' % n


def format_time(n):
    """ format integers as time

    >>> format_time(1)
    '1.00 s'
    >>> format_time(0.001234)
    '1.23 ms'
    >>> format_time(0.00012345)
    '123.45 us'
    >>> format_time(123.456)
    '123.46 s'
    """
    if n >= 1:
        return '%.2f s' % n
    if n >= 1e-3:
        return '%.2f ms' % (n * 1e3)
    return '%.2f us' % (n * 1e6)
