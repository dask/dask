from __future__ import print_function, division, absolute_import

from functools import partial
import logging
from math import sqrt
from operator import add
import os

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import column
from bokeh.models import ( ColumnDataSource, DataRange1d, HoverTool, ResetTool,
        PanTool, WheelZoomTool, TapTool, OpenURL, Range1d, Plot, Quad, Text,
        value, LinearAxis)
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure
from bokeh.palettes import Viridis11
from bokeh.io import curdoc
from toolz import pipe
try:
    import numpy as np
except ImportError:
    np = False

from . import components
from .components import DashboardComponent
from .core import BokehServer
from .worker import SystemMonitor, format_time, counters_doc
from .utils import transpose
from ..metrics import time
from ..utils import log_errors
from ..diagnostics.progress_stream import color_of, progress_quads, nbytes_bar
from ..diagnostics.progress import AllProgress
from .task_stream import TaskStreamPlugin

try:
    from cytoolz.curried import map, concat, groupby, valmap
except ImportError:
    from toolz.curried import map, concat, groupby, valmap

logger = logging.getLogger(__name__)


PROFILING = True

import jinja2

with open(os.path.join(os.path.dirname(__file__), 'template.html')) as f:
    template_source = f.read()

template = jinja2.Template(template_source)


class StateTable(DashboardComponent):
    """ Currently running tasks """
    def __init__(self, scheduler):
        self.scheduler = scheduler

        names = ['Tasks', 'Stored', 'Processing', 'Waiting', 'No Worker',
                 'Erred', 'Released']
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
            s = self.scheduler
            d = {'Tasks': [len(s.tasks)],
                 'Stored': [len(s.who_has)],
                 'Processing': ['%d / %d' % (len(s.rprocessing), s.total_ncores)],
                 'Waiting': [len(s.waiting)],
                 'No Worker': [len(s.unrunnable)],
                 'Erred': [len(s.exceptions)],
                 'Released': [len(s.released)]}

            if PROFILING:
                curdoc().add_next_tick_callback(lambda: self.source.data.update(d))
            else:
                self.source.data.update(d)


class Occupancy(DashboardComponent):
    """ Occupancy (in time) per worker """
    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.scheduler = scheduler
            self.source = ColumnDataSource({'occupancy': [0, 0],
                                            'worker': ['a', 'b'],
                                            'x': [0.0, 0.1],
                                            'y': [1, 2],
                                            'ms': [1, 2],
                                            'color': ['red', 'blue'],
                                            'bokeh_address': ['', '']})

            fig = figure(title='Occupancy', tools='resize', id='bk-occupancy-plot',
                         x_axis_type='datetime', **kwargs)
            fig.rect(source=self.source, x='x', width='ms', y='y', height=1,
                     color='color')

            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            # fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0s')
            fig.x_range.start = 0

            tap = TapTool(callback=OpenURL(url='http://@bokeh_address/'))

            hover = HoverTool()
            hover.tooltips = "@worker : @occupancy s.  Click for worker page"
            hover.point_policy = 'follow_mouse'
            fig.add_tools(hover, tap)

            self.root = fig

    def update(self):
        with log_errors():
            o = self.scheduler.occupancy
            workers = list(self.scheduler.workers)

            bokeh_addresses = []
            for worker in workers:
                addr = self.scheduler.get_worker_service_addr(worker, 'bokeh')
                bokeh_addresses.append('%s:%d' % addr if addr is not None else '')

            y = list(range(len(workers)))
            occupancy = [o[w] for w in workers]
            ms = [occ * 1000 for occ in occupancy]
            x = [occ / 500 for occ in occupancy]
            total = sum(occupancy)
            color = []
            for w in workers:
                if w in self.scheduler.idle:
                    color.append('red')
                elif w in self.scheduler.saturated:
                    color.append('green')
                else:
                    color.append('blue')

            if total:
                self.root.title.text = ('Occupancy -- total time: %s  wall time: %s' %
                                  (format_time(total),
                                  format_time(total / self.scheduler.total_ncores)))
            else:
                self.root.title.text = 'Occupancy'

            if occupancy:
                result = {'occupancy': occupancy,
                          'worker': workers,
                          'ms': ms,
                          'color': color,
                          'bokeh_address': bokeh_addresses,
                          'x': x, 'y': y}

                if PROFILING:
                    curdoc().add_next_tick_callback(lambda: self.source.data.update(result))
                else:
                    self.source.data.update(result)


class NProcessing(DashboardComponent):
    """ How many tasks are on each worker """
    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.scheduler = scheduler
            self.source = ColumnDataSource({'nprocessing': [1, 2],
                                            'worker': ['a', 'b'],
                                            'x': [0.0, 0.1],
                                            'y': [1, 2],
                                            'color': ['red', 'blue'],
                                            'bokeh_address': ['', '']})

            fig = figure(title='Occupancy', tools='resize', id='bk-nprocessing-plot',
                          **kwargs)
            fig.rect(source=self.source, x='x', width='nprocessing', y='y', height=1,
                     color='color')

            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            fig.x_range.start = 0

            tap = TapTool(callback=OpenURL(url='http://@bokeh_address/'))

            hover = HoverTool()
            hover.tooltips = "@worker : @nprocessing tasks.  Click for worker page"
            hover.point_policy = 'follow_mouse'
            fig.add_tools(hover, tap)

            self.root = fig

    def update(self):
        with log_errors():
            p = valmap(len, self.scheduler.processing)
            workers = list(self.scheduler.workers)

            bokeh_addresses = []
            for worker in workers:
                addr = self.scheduler.get_worker_service_addr(worker, 'bokeh')
                bokeh_addresses.append('%s:%d' % addr if addr is not None else '')

            y = list(range(len(workers)))
            nprocessing = [p[w] for w in workers]
            x = [np / 2 for np in nprocessing]
            total = sum(nprocessing)
            color = []
            for w in workers:
                if w in self.scheduler.idle:
                    color.append('red')
                elif w in self.scheduler.saturated:
                    color.append('green')
                else:
                    color.append('blue')

            if total:
                self.root.title.text = ('Processing Count-- total: %6d  avg: %8.2f' %
                                        (total, total / len(workers)))
            else:
                self.root.title.text = 'Processing Count'

            if nprocessing:
                result = {'nprocessing': nprocessing,
                          'worker': workers,
                          'color': color,
                          'bokeh_address': bokeh_addresses,
                          'x': x, 'y': y}

                if PROFILING:
                    curdoc().add_next_tick_callback(lambda: self.source.data.update(result))
                else:
                    self.source.data.update(result)


class StealingTimeSeries(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource({'time': [], 'idle': [], 'saturated': []})

        x_range = DataRange1d(follow='end', follow_interval=20000, range_padding=0)

        fig = figure(title="Idle and Saturated Workers Over Time",
                     x_axis_type='datetime', y_range=[-0.1, len(scheduler.workers) + 0.1],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='time', y='idle', color='red')
        fig.line(source=self.source, x='time', y='saturated', color='green')
        fig.yaxis.minor_tick_line_color = None

        fig.add_tools(
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            result = {'time': [time() * 1000],
                      'idle': [len(self.scheduler.idle)],
                      'saturated': [len(self.scheduler.saturated)]}
            if PROFILING:
                curdoc().add_next_tick_callback(lambda: self.source.stream(result, 10000))
            else:
                self.source.stream(result, 10000)


class StealingEvents(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.steal = scheduler.extensions['stealing']
        self.last = 0
        self.source = ColumnDataSource({'time': [time() - 20, time()],
                                        'level': [0, 15],
                                        'color': ['white', 'white'],
                                        'duration': [0, 0], 'radius': [1, 1],
                                        'cost_factor': [0, 10], 'count': [1, 1]})

        x_range = DataRange1d(follow='end', follow_interval=20000, range_padding=0)

        fig = figure(title="Stealing Events",
                     x_axis_type='datetime', y_axis_type='log',
                     height=250, tools='', x_range=x_range, **kwargs)

        fig.circle(source=self.source, x='time', y='cost_factor', color='color',
                   size='radius', alpha=0.5)
        fig.yaxis.axis_label = "Cost Multiplier"

        hover = HoverTool()
        hover.tooltips = "Level: @level, Duration: @duration, Count: @count, Cost factor: @cost_factor"
        hover.point_policy = 'follow_mouse'

        fig.add_tools(
            hover,
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def convert(self, msgs):
        """ Convert a log message to a glyph """
        total_duration = 0
        for msg in msgs:
            time, level, key, duration, sat, occ_sat, idl, occ_idl = msg
            total_duration += duration

        try:
            color = Viridis11[level]
        except (KeyError, IndexError):
            color = 'black'

        radius = sqrt(min(total_duration, 10)) * 30 + 2

        d = {'time': time * 1000, 'level': level, 'count': len(msgs),
             'color': color, 'duration': total_duration, 'radius': radius,
             'cost_factor': min(10, self.steal.cost_multipliers[level])}

        return d

    def update(self):
        with log_errors():
            log = self.steal.log
            n = self.steal.count - self.last
            log = [log[-i] for i in range(1, n + 1)]
            self.last = self.steal.count

            if log:
                new = pipe(log, map(groupby(1)), map(dict.values), concat,
                           map(self.convert), list, transpose)
                if PROFILING:
                    curdoc().add_next_tick_callback(
                            lambda: self.source.stream(new, 10000))
                else:
                    self.source.stream(new, 10000)


class Events(DashboardComponent):
    def __init__(self, scheduler, name, height=150, **kwargs):
        self.scheduler = scheduler
        self.action_ys = dict()
        self.last = 0
        self.name = name
        self.source = ColumnDataSource({'time': [], 'action': [], 'hover': [],
                                        'y': [], 'color': []})

        x_range = DataRange1d(follow='end', follow_interval=200000)

        fig = figure(title=name, x_axis_type='datetime',
                     height=height, tools='', x_range=x_range, **kwargs)

        fig.circle(source=self.source, x='time', y='y', color='color',
                   size=50, alpha=0.5, legend='action')
        fig.yaxis.axis_label = "Action"
        fig.legend.location = 'top_left'

        hover = HoverTool()
        hover.tooltips = "@action<br>@hover"
        hover.point_policy = 'follow_mouse'

        fig.add_tools(
            hover,
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            log = self.scheduler.events[self.name]
            n = self.scheduler.event_counts[self.name] - self.last
            log = [log[-i] for i in range(1, n + 1)]
            self.last = self.scheduler.event_counts[self.name]

            if log:
                actions = []
                times = []
                hovers = []
                ys = []
                colors = []
                for msg in log:
                    times.append(msg['time'] * 1000)
                    action = msg['action']
                    actions.append(action)
                    try:
                        ys.append(self.action_ys[action])
                    except KeyError:
                        self.action_ys[action] = len(self.action_ys)
                        ys.append(self.action_ys[action])
                    colors.append(color_of(action))
                    hovers.append('TODO')

                new = {'time': times,
                       'action': actions,
                       'hover': hovers,
                       'y': ys,
                       'color': colors}

                if PROFILING:
                    curdoc().add_next_tick_callback(lambda: self.source.stream(new, 10000))
                else:
                    self.source.stream(new, 10000)


class TaskStream(components.TaskStream):
    def __init__(self, scheduler, n_rectangles=1000, clear_interval=20000, **kwargs):
        self.scheduler = scheduler
        es = [p for p in self.scheduler.plugins if isinstance(p, TaskStreamPlugin)]
        if not es:
            self.plugin = TaskStreamPlugin(self.scheduler)
        else:
            self.plugin = es[0]
        self.index = max(0, self.plugin.index - n_rectangles)
        self.workers = dict()

        components.TaskStream.__init__(self, n_rectangles=n_rectangles,
                                       clear_interval=clear_interval, **kwargs)

    def update(self):
        with log_errors():
            rectangles = self.plugin.rectangles(istart=self.index,
                                                workers=self.workers)
            n = len(rectangles['name'])
            self.index += n

            # If there has been a significant delay then clear old rectangles
            if rectangles['start']:
                m = min(map(add, rectangles['start'], rectangles['duration']))
                if m > self.last:
                    self.last, last = m, self.last
                    if m > last + self.clear_interval:
                        if PROFILING:
                            curdoc().add_next_tick_callback(lambda:
                                    self.source.data.update(rectangles))
                        else:
                            self.source.data.update(rectangles)
                        return

            if len(set(map(len, rectangles.values()))) != 1:
                import pdb; pdb.set_trace()
            if not n:
                return
            if n > 10 and np:
                rectangles = valmap(np.array, rectangles)
            if PROFILING:
                curdoc().add_next_tick_callback(lambda:
                        self.source.stream(rectangles, self.n_rectangles))
            else:
                self.source.stream(rectangles, self.n_rectangles)


class TaskProgress(DashboardComponent):
    """ Progress bars per task type """

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        ps = [p for p in scheduler.plugins if isinstance(p, AllProgress)]
        if ps:
            self.plugin = ps[0]
        else:
            self.plugin = AllProgress(scheduler)

        data = progress_quads(dict(all={}, memory={}, erred={}, released={}))
        self.source = ColumnDataSource(data=data)

        x_range = DataRange1d()
        y_range = Range1d(-8, 0)

        self.root = Plot(
            id='bk-task-progress-plot',
            x_range=x_range, y_range=y_range, toolbar_location=None, **kwargs
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='right',
                 fill_color="#aaaaaa", line_color="#aaaaaa", fill_alpha=0.2)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='released-loc',
                 fill_color="color", line_color="color", fill_alpha=0.6)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='released-loc',
                 right='memory-loc', fill_color="color", line_color="color",
                 fill_alpha=1.0)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='erred-loc',
                 right='erred-loc', fill_color='#000000', line_color='#000000',
                 fill_alpha=0.3)
        )
        self.root.add_glyph(
            self.source,
            Text(text='show-name', y='bottom', x='left', x_offset=5,
                 text_font_size=value('10pt'))
        )
        self.root.add_glyph(
            self.source,
            Text(text='done', y='bottom', x='right', x_offset=-5,
                 text_align='right', text_font_size=value('10pt'))
        )

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">All:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@all</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self):
        with log_errors():
            state = {'all': valmap(len, self.plugin.all),
                     'nbytes': self.plugin.nbytes}
            for k in ['memory', 'erred', 'released']:
                state[k] = valmap(len, self.plugin.state[k])
            if not state['all']:
                return

            d = progress_quads(state)

            if PROFILING:
                curdoc().add_next_tick_callback(lambda: self.source.data.update(d))
            else:
                self.source.data.update(d)

            totals = {k: sum(state[k].values())
                      for k in ['all', 'memory', 'erred', 'released']}
            totals['processing'] = totals['all'] - sum(v for k, v in
                    totals.items() if k != 'all')

            # self.root.title.text = ("Progress -- total: %(all)s, "
            #     "in-memory: %(memory)s, processing: %(processing)s, "
            #     "erred: %(erred)s" % totals)


class MemoryUse(DashboardComponent):
    """ The memory usage across the cluster, grouped by task type """
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        ps = [p for p in scheduler.plugins if isinstance(p, AllProgress)]
        if ps:
            self.plugin = ps[0]
        else:
            self.plugin = AllProgress(scheduler)

        self.source = ColumnDataSource(data=dict(
            name=[], left=[], right=[], center=[], color=[],
            percent=[], MB=[], text=[])
        )

        self.root = Plot(
            id='bk-nbytes-plot', x_range=DataRange1d(), y_range=DataRange1d(),
            toolbar_location=None, outline_line_color=None, **kwargs
        )

        self.root.add_glyph(
            self.source,
            Quad(top=1, bottom=0, left='left', right='right',
                 fill_color='color', fill_alpha=1)
        )

        self.root.add_layout(LinearAxis(), 'left')
        self.root.add_layout(LinearAxis(), 'below')

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Percent:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@percent</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">MB:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@MB</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self):
        with log_errors():
            nb = nbytes_bar(self.plugin.nbytes)
            if PROFILING:
                curdoc().add_next_tick_callback(lambda:
                        self.source.data.update(nb))
            else:
                self.source.data.update(nb)
            self.root.title.text = \
                    "Memory Use: %0.2f MB" % (sum(self.plugin.nbytes.values()) / 1e6)


def systemmonitor_doc(scheduler, doc):
    with log_errors():
        table = StateTable(scheduler)
        sysmon = SystemMonitor(scheduler, sizing_mode='scale_width')
        doc.title = "Dask Scheduler Internal Monitor"
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(column(table.root, sysmon.root,
                            sizing_mode='scale_width'))
        doc.template = template


def workers_doc(scheduler, doc):
    with log_errors():
        table = StateTable(scheduler)
        occupancy = Occupancy(scheduler, height=200, sizing_mode='scale_width')
        stealing_ts = StealingTimeSeries(scheduler, sizing_mode='scale_width')
        stealing_events = StealingEvents(scheduler, sizing_mode='scale_width')
        stealing_events.root.x_range = stealing_ts.root.x_range
        doc.title = "Dask Workers Monitor"
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(occupancy.update, 500)
        doc.add_periodic_callback(stealing_ts.update, 500)
        doc.add_periodic_callback(stealing_events.update, 500)

        doc.add_root(column(table.root, occupancy.root, stealing_ts.root,
                            stealing_events.root,
                            sizing_mode='scale_width'))

        doc.template = template


def events_doc(scheduler, doc):
    with log_errors():
        events = Events(scheduler, 'all', height=250)
        events.update()
        doc.add_periodic_callback(events.update, 500)
        doc.title = "Dask Scheduler Events"
        doc.add_root(column(events.root, sizing_mode='scale_width'))
        doc.template = template


def tasks_doc(scheduler, doc):
    with log_errors():
        ts = TaskStream(scheduler, n_rectangles=100000, clear_interval=60000)
        ts.update()
        doc.add_periodic_callback(ts.update, 5000)
        doc.title = "Dask Task Stream"
        doc.add_root(column(ts.root, sizing_mode='scale_width'))
        doc.template = template


def status_doc(scheduler, doc):
    with log_errors():
        ts = TaskStream(scheduler, n_rectangles=1000, clear_interval=10000, height=350)
        ts.update()
        tp = TaskProgress(scheduler, height=160)
        tp.update()
        mu = MemoryUse(scheduler, height=60)
        mu.update()
        pp = NProcessing(scheduler, height=160)
        pp.update()
        doc.add_periodic_callback(ts.update, 100)
        doc.add_periodic_callback(tp.update, 100)
        doc.add_periodic_callback(mu.update, 100)
        doc.add_periodic_callback(pp.update, 100)
        doc.title = "Dask Status"
        doc.add_root(column(pp.root, ts.root, tp.root, mu.root, sizing_mode='scale_width'))
        doc.template = template


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None):
        self.scheduler = scheduler
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc,
                                                            scheduler)))
        workers = Application(FunctionHandler(partial(workers_doc, scheduler)))
        counters = Application(FunctionHandler(partial(counters_doc, scheduler)))
        events = Application(FunctionHandler(partial(events_doc, scheduler)))
        tasks = Application(FunctionHandler(partial(tasks_doc, scheduler)))
        status = Application(FunctionHandler(partial(status_doc, scheduler)))

        self.apps = {
                '/system': systemmonitor,
                '/workers': workers,
                '/events': events,
                '/counters': counters,
                '/tasks': tasks,
                '/status': status
        }

        self.loop = io_loop or scheduler.loop
        self.server = None
