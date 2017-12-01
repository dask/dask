from __future__ import print_function, division, absolute_import

from functools import partial
import logging
import math
from math import sqrt
from numbers import Number
from operator import add
import os

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import column, row
from bokeh.models import (ColumnDataSource, DataRange1d, HoverTool, ResetTool,
                          PanTool, WheelZoomTool, TapTool, OpenURL, Range1d, Plot, Quad,
                          value, LinearAxis, NumeralTickFormatter, BasicTicker, NumberFormatter,
                          BoxSelectTool)
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
from .components import DashboardComponent, ProfileTimePlot
from .core import BokehServer
from .worker import SystemMonitor, counters_doc
from .utils import transpose
from ..metrics import time
from ..utils import log_errors, format_bytes, format_time
from ..diagnostics.progress_stream import color_of, progress_quads, nbytes_bar
from ..diagnostics.progress import AllProgress
from .task_stream import TaskStreamPlugin

try:
    from cytoolz.curried import map, concat, groupby, valmap, first
except ImportError:
    from toolz.curried import map, concat, groupby, valmap, first

logger = logging.getLogger(__name__)


PROFILING = False

import jinja2

with open(os.path.join(os.path.dirname(__file__), 'template.html')) as f:
    template_source = f.read()

template = jinja2.Template(template_source)

template_variables = {'pages': ['status', 'workers', 'tasks', 'system',
                                'profile', 'counters']}


def update(source, data):
    """ Update source with data

    This checks a few things first

    1.  If the data is the same, then don't update
    2.  If numpy is available and the data is numeric, then convert to numpy
        arrays
    3.  If profiling then perform the update in another callback
    """
    if (not np or not any(isinstance(v, np.ndarray)
                          for v in source.data.values())):
        if source.data == data:
            return
    if np and len(data[first(data)]) > 10:
        d = {}
        for k, v in data.items():
            if type(v) is not np.ndarray and isinstance(v[0], Number):
                d[k] = np.array(v)
            else:
                d[k] = v
    else:
        d = data

    if PROFILING:
        curdoc().add_next_tick_callback(lambda: source.data.update(d))
    else:
        source.data.update(d)


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

            update(self.source, d)


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

            fig = figure(title='Occupancy', tools='', id='bk-occupancy-plot',
                         x_axis_type='datetime', **kwargs)
            rect = fig.rect(source=self.source, x='x', width='ms', y='y', height=1,
                            color='color')
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            # fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0s')
            fig.x_range.start = 0

            tap = TapTool(callback=OpenURL(url='http://@bokeh_address/'))

            hover = HoverTool()
            hover.tooltips = "@worker : @occupancy s."
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

                update(self.source, result)


class ProcessingHistogram(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource({'left': [1, 2],
                                            'right': [10, 10],
                                            'top': [0, 0]})

            self.root = figure(title='Tasks Processing',
                               id='bk-nprocessing-histogram-plot',
                               **kwargs)

            self.root.xaxis.minor_tick_line_alpha = 0
            self.root.ygrid.visible = False

            self.root.toolbar.logo = None
            self.root.toolbar_location = None

            self.root.quad(source=self.source,
                           left='left', right='right', bottom=0, top='top',
                           color='blue')

    def update(self):
        L = list(map(len, self.scheduler.processing.values()))
        counts, x = np.histogram(L, bins=40)
        self.source.data.update({'left': x[:-1],
                                 'right': x[1:],
                                 'top': counts})


class NBytesHistogram(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource({'left': [1, 2],
                                            'right': [10, 10],
                                            'top': [0, 0]})

            self.root = figure(title='NBytes Stored',
                               id='bk-nbytes-histogram-plot',
                               **kwargs)
            self.root.xaxis[0].formatter = NumeralTickFormatter(format='0.0 b')
            self.root.xaxis.major_label_orientation = -math.pi / 12

            self.root.xaxis.minor_tick_line_alpha = 0
            self.root.ygrid.visible = False

            self.root.toolbar.logo = None
            self.root.toolbar_location = None

            self.root.quad(source=self.source,
                           left='left', right='right', bottom=0, top='top',
                           color='blue')

    def update(self):
        nbytes = np.asarray(list(self.scheduler.worker_bytes.values()))
        counts, x = np.histogram(nbytes, bins=40)
        d = {'left': x[:-1], 'right': x[1:], 'top': counts}
        self.source.data.update(d)

        self.root.title.text = 'Bytes stored: ' + format_bytes(nbytes.sum())


class CurrentLoad(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, width=600, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource({'nprocessing': [1, 2],
                                            'nprocessing-half': [0.5, 1],
                                            'nprocessing-color': ['red', 'blue'],
                                            'nbytes': [1, 2],
                                            'nbytes-half': [0.5, 1],
                                            'nbytes_text': ['1B', '2B'],
                                            'worker': ['a', 'b'],
                                            'y': [1, 2],
                                            'nbytes-color': ['blue', 'blue'],
                                            'bokeh_address': ['', '']})

            processing = figure(title='Tasks Processing', tools='', id='bk-nprocessing-plot',
                                width=int(width / 2), **kwargs)
            rect = processing.rect(source=self.source,
                                   x='nprocessing-half', y='y',
                                   width='nprocessing', height=1,
                                   color='nprocessing-color')
            processing.x_range.start = 0
            rect.nonselection_glyph = None

            nbytes = figure(title='Bytes stored', tools='',
                            id='bk-nbytes-worker-plot', width=int(width / 2),
                            **kwargs)
            rect = nbytes.rect(source=self.source,
                               x='nbytes-half', y='y',
                               width='nbytes', height=1,
                               color='nbytes-color')
            rect.nonselection_glyph = None

            nbytes.axis[0].ticker = BasicTicker(mantissas=[1, 256, 512], base=1024)
            nbytes.xaxis[0].formatter = NumeralTickFormatter(format='0.0 b')
            nbytes.xaxis.major_label_orientation = -math.pi / 12
            nbytes.x_range.start = 0

            for fig in [processing, nbytes]:
                fig.xaxis.minor_tick_line_alpha = 0
                fig.yaxis.visible = False
                fig.ygrid.visible = False

                tap = TapTool(callback=OpenURL(url='http://@bokeh_address/'))
                fig.add_tools(tap)

                fig.toolbar.logo = None
                fig.toolbar_location = None
                fig.yaxis.visible = False

            hover = HoverTool()
            hover.tooltips = "@worker : @nprocessing tasks"
            hover.point_policy = 'follow_mouse'
            processing.add_tools(hover)

            hover = HoverTool()
            hover.tooltips = "@worker : @nbytes_text bytes"
            hover.point_policy = 'follow_mouse'
            nbytes.add_tools(hover)

            self.processing_figure = processing
            self.nbytes_figure = nbytes

            processing.y_range = nbytes.y_range
            self.root = row(nbytes, processing, sizing_mode='scale_width')

    def update(self):
        with log_errors():
            processing = valmap(len, self.scheduler.processing)
            workers = list(self.scheduler.workers)

            bokeh_addresses = []
            for worker in workers:
                addr = self.scheduler.get_worker_service_addr(worker, 'bokeh')
                bokeh_addresses.append('%s:%d' % addr if addr is not None else '')

            y = list(range(len(workers)))
            nprocessing = [processing[w] for w in workers]
            processing_color = []
            for w in workers:
                if w in self.scheduler.idle:
                    processing_color.append('red')
                elif w in self.scheduler.saturated:
                    processing_color.append('green')
                else:
                    processing_color.append('blue')

            nbytes = [self.scheduler.worker_bytes[w] for w in workers]
            nbytes_text = [format_bytes(nb) for nb in nbytes]
            nbytes_color = []
            max_limit = 0
            for w, nb in zip(workers, nbytes):
                try:
                    limit = self.scheduler.worker_info[w]['memory_limit']
                except KeyError:
                    limit = 16e9
                if limit > max_limit:
                    max_limit = limit

                if nb > limit:
                    nbytes_color.append('red')
                elif nb > limit / 2:
                    nbytes_color.append('orange')
                else:
                    nbytes_color.append('blue')

            now = time()
            if any(nprocessing) or self.last + 1 < now:
                self.last = now
                result = {'nprocessing': nprocessing,
                          'nprocessing-half': [np / 2 for np in nprocessing],
                          'nprocessing-color': processing_color,
                          'nbytes': nbytes,
                          'nbytes-half': [nb / 2 for nb in nbytes],
                          'nbytes-color': nbytes_color,
                          'nbytes_text': nbytes_text,
                          'bokeh_address': bokeh_addresses,
                          'worker': workers,
                          'y': y}

                self.nbytes_figure.title.text = 'Bytes stored: ' + format_bytes(sum(nbytes))

                update(self.source, result)


class StealingTimeSeries(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource({'time': [time(), time() + 1],
                                        'idle': [0, 0.1],
                                        'saturated': [0, 0.1]})

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
            log = [log[-i] for i in range(1, n + 1) if isinstance(log[-i], list)]
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
            if log:
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
        self.offset = 0
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
        if self.index == self.plugin.index:
            return
        with log_errors():
            if self.index and self.source.data['start']:
                start = min(self.source.data['start'])
                duration = max(self.source.data['duration'])
                boundary = (self.offset + start - duration) / 1000
            else:
                boundary = self.offset
            rectangles = self.plugin.rectangles(istart=self.index,
                                                workers=self.workers,
                                                start_boundary=boundary)
            n = len(rectangles['name'])
            self.index = self.plugin.index

            if not rectangles['start']:
                return

            # If there has been a significant delay then clear old rectangles
            first_end = min(map(add, rectangles['start'], rectangles['duration']))
            if first_end > self.last:
                last = self.last
                self.last = first_end
                if first_end > last + self.clear_interval:
                    self.offset = min(rectangles['start'])
                    self.source.data.update({k: [] for k in rectangles})

            rectangles['start'] = [x - self.offset for x in rectangles['start']]

            # Convert to numpy for serialization speed
            if n >= 10 and np:
                for k, v in rectangles.items():
                    if isinstance(v[0], Number):
                        rectangles[k] = np.array(v)

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

        x_range = DataRange1d(range_padding=0)
        y_range = Range1d(-8, 0)

        self.root = figure(
            id='bk-task-progress-plot', title='Progress',
            x_range=x_range, y_range=y_range, toolbar_location=None, **kwargs
        )
        self.root.line(  # just to define early ranges
            x=[0, 1], y=[-1, 0], line_color="#FFFFFF", alpha=0.0)
        self.root.quad(
            source=self.source,
            top='top', bottom='bottom', left='left', right='right',
            fill_color="#aaaaaa", line_color="#aaaaaa", fill_alpha=0.2
        )
        self.root.quad(
            source=self.source,
            top='top', bottom='bottom', left='left', right='released-loc',
            fill_color="color", line_color="color", fill_alpha=0.6
        )
        self.root.quad(
            source=self.source,
            top='top', bottom='bottom', left='released-loc',
            right='memory-loc', fill_color="color", line_color="color",
            fill_alpha=1.0
        )
        self.root.quad(
            source=self.source,
            top='top', bottom='bottom', left='released-loc',
            right='erred-loc', fill_color='black', line_color='#000000',
            fill_alpha=0.5
        )
        self.root.text(
            source=self.source,
            text='show-name', y='bottom', x='left', x_offset=5,
            text_font_size=value('10pt')
        )
        self.root.text(
            source=self.source,
            text='done', y='bottom', x='right', x_offset=-5,
            text_align='right', text_font_size=value('10pt')
        )
        self.root.ygrid.visible = False
        self.root.yaxis.minor_tick_line_alpha = 0
        self.root.yaxis.visible = False
        self.root.xgrid.visible = False
        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.xaxis.visible = False

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
            if not state['all'] and not len(self.source.data['all']):
                return

            d = progress_quads(state)

            update(self.source, d)

            totals = {k: sum(state[k].values())
                      for k in ['all', 'memory', 'erred', 'released']}
            totals['processing'] = totals['all'] - sum(v for k, v in
                                                       totals.items() if k != 'all')

            self.root.title.text = ("Progress -- total: %(all)s, "
                                    "in-memory: %(memory)s, processing: %(processing)s, "
                                    "erred: %(erred)s" % totals)


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
            update(self.source, nb)
            self.root.title.text = \
                "Memory Use: %0.2f MB" % (sum(self.plugin.nbytes.values()) / 1e6)


class WorkerTable(DashboardComponent):
    """ Status of the current workers

    This is two plots, a text-based table for each host and a thin horizontal
    plot laying out hosts by their current memory use.
    """

    def __init__(self, scheduler, width=800, **kwargs):
        self.scheduler = scheduler
        self.names = ['worker', 'ncores', 'cpu', 'memory', 'memory_limit',
                      'memory_percent', 'num_fds', 'read_bytes', 'write_bytes']

        self.source = ColumnDataSource({k: [] for k in self.names})

        columns = {name: TableColumn(field=name,
                                     title=name.replace('_percent', ' %'))
                   for name in self.names}

        formatters = {'cpu': NumberFormatter(format='0.0 %'),
                      'memory_percent': NumberFormatter(format='0.0 %'),
                      'memory': NumberFormatter(format='0 b'),
                      'memory_limit': NumberFormatter(format='0 b'),
                      'read_bytes': NumberFormatter(format='0 b'),
                      'write_bytes': NumberFormatter(format='0 b'),
                      'num_fds': NumberFormatter(format='0'),
                      'ncores': NumberFormatter(format='0')}

        table = DataTable(
            source=self.source, columns=[columns[n] for n in self.names],
            row_headers=False, reorderable=True, sortable=True, width=width,
        )

        for name in self.names:
            if name in formatters:
                table.columns[self.names.index(name)].formatter = formatters[name]

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@host: </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@memory_percent</span>
                </div>
                """
        )

        mem_plot = figure(title='Memory Use (%)', toolbar_location=None,
                          x_range=(0, 1), y_range=(-0.1, 0.1), height=80,
                          width=width, tools='', **kwargs)
        mem_plot.circle(source=self.source, x='memory_percent', y=0,
                        size=10, fill_alpha=0.5)
        mem_plot.ygrid.visible = False
        mem_plot.yaxis.minor_tick_line_alpha = 0
        mem_plot.yaxis.visible = False
        mem_plot.add_tools(hover, BoxSelectTool())

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@worker: </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@cpu</span>
                </div>
                """
        )

        cpu_plot = figure(title='CPU Use (%)', toolbar_location=None,
                          x_range=(0, 1), y_range=(-0.1, 0.1), height=80,
                          width=width, tools='', **kwargs)
        cpu_plot.circle(source=self.source, x='cpu', y=0,
                        size=10, fill_alpha=0.5)
        cpu_plot.ygrid.visible = False
        cpu_plot.yaxis.minor_tick_line_alpha = 0
        cpu_plot.yaxis.visible = False
        cpu_plot.add_tools(hover, BoxSelectTool())

        if 'sizing_mode' in kwargs:
            sizing_mode = {'sizing_mode': kwargs['sizing_mode']}
        else:
            sizing_mode = {}

        self.root = column(cpu_plot, mem_plot, table, id='bk-worker-table', **sizing_mode)

    def update(self):
        data = {name: [] for name in self.names}
        for worker, info in sorted(self.scheduler.worker_info.items()):
            for name in self.names:
                data[name].append(info.get(name, None))
            data['worker'][-1] = worker
            data['memory_percent'][-1] = info['memory'] / info['memory_limit']
            data['cpu'][-1] = info['cpu'] / 100.0

        self.source.data.update(data)


def systemmonitor_doc(scheduler, extra, doc):
    with log_errors():
        table = StateTable(scheduler)
        sysmon = SystemMonitor(scheduler, sizing_mode='scale_width')
        doc.title = "Dask Scheduler Internal Monitor"
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(column(table.root, sysmon.root,
                            sizing_mode='scale_width'))
        doc.template = template
        doc.template_variables['active_page'] = 'system'
        doc.template_variables.update(extra)


def stealing_doc(scheduler, extra, doc):
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
        doc.template_variables['active_page'] = 'stealing'
        doc.template_variables.update(extra)


def events_doc(scheduler, extra, doc):
    with log_errors():
        events = Events(scheduler, 'all', height=250)
        events.update()
        doc.add_periodic_callback(events.update, 500)
        doc.title = "Dask Scheduler Events"
        doc.add_root(column(events.root, sizing_mode='scale_width'))
        doc.template = template
        doc.template_variables['active_page'] = 'events'
        doc.template_variables.update(extra)


def workers_doc(scheduler, extra, doc):
    with log_errors():
        table = WorkerTable(scheduler)
        table.update()
        doc.add_periodic_callback(table.update, 500)
        doc.title = "Dask Workers"
        doc.add_root(table.root)
        doc.template = template
        doc.template_variables['active_page'] = 'workers'
        doc.template_variables.update(extra)


def tasks_doc(scheduler, extra, doc):
    with log_errors():
        ts = TaskStream(scheduler, n_rectangles=100000, clear_interval=60000,
                        sizing_mode='stretch_both')
        ts.update()
        doc.add_periodic_callback(ts.update, 5000)
        doc.title = "Dask Task Stream"
        doc.add_root(ts.root)
        doc.template = template
        doc.template_variables['active_page'] = 'tasks'
        doc.template_variables.update(extra)


def status_doc(scheduler, extra, doc):
    with log_errors():
        task_stream = TaskStream(scheduler, n_rectangles=1000, clear_interval=10000, height=350)
        task_stream.update()
        doc.add_periodic_callback(task_stream.update, 100)

        task_progress = TaskProgress(scheduler, height=160)
        task_progress.update()
        doc.add_periodic_callback(task_progress.update, 100)

        if len(scheduler.workers) < 50:
            current_load = CurrentLoad(scheduler, height=160)
            current_load.update()
            doc.add_periodic_callback(current_load.update, 100)
            current_load_fig = current_load.root
        else:
            nbytes_hist = NBytesHistogram(scheduler, width=300, height=160)
            nbytes_hist.update()
            processing_hist = ProcessingHistogram(scheduler, width=300,
                                                  height=160)
            processing_hist.update()
            doc.add_periodic_callback(nbytes_hist.update, 100)
            doc.add_periodic_callback(processing_hist.update, 100)
            current_load_fig = row(nbytes_hist.root, processing_hist.root,
                                   sizing_mode='scale_width')

        doc.title = "Dask Status"
        doc.add_root(column(current_load_fig,
                            task_stream.root,
                            task_progress.root,
                            sizing_mode='scale_width'))
        doc.template = template
        doc.template_variables['active_page'] = 'status'
        doc.template_variables.update(extra)


def profile_doc(scheduler, extra, doc):
    with log_errors():
        doc.title = "Dask Profile"
        prof = ProfileTimePlot(scheduler, sizing_mode='scale_width', doc=doc)
        doc.add_root(prof.root)
        doc.template = template
        doc.template_variables['active_page'] = 'profile'
        doc.template_variables.update(extra)

        prof.trigger_update()


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None, prefix='', **kwargs):
        self.scheduler = scheduler
        self.server_kwargs = kwargs
        self.server_kwargs['prefix'] = prefix or None
        prefix = prefix or ''
        prefix = prefix.rstrip('/')
        if prefix and not prefix.startswith('/'):
            prefix = '/' + prefix

        extra = {'prefix': prefix}
        extra.update(template_variables)

        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc, scheduler, extra)))
        workers = Application(FunctionHandler(partial(workers_doc, scheduler, extra)))
        stealing = Application(FunctionHandler(partial(stealing_doc, scheduler, extra)))
        counters = Application(FunctionHandler(partial(counters_doc, scheduler, extra)))
        events = Application(FunctionHandler(partial(events_doc, scheduler, extra)))
        tasks = Application(FunctionHandler(partial(tasks_doc, scheduler, extra)))
        status = Application(FunctionHandler(partial(status_doc, scheduler, extra)))
        profile = Application(FunctionHandler(partial(profile_doc, scheduler, extra)))

        self.apps = {
            '/system': systemmonitor,
            '/stealing': stealing,
            '/workers': workers,
            '/events': events,
            '/counters': counters,
            '/tasks': tasks,
            '/status': status,
            '/profile': profile,
        }

        self.loop = io_loop or scheduler.loop
        self.server = None

    @property
    def my_server(self):
        return self.scheduler

    def listen(self, *args, **kwargs):
        super(BokehScheduler, self).listen(*args, **kwargs)

        from .scheduler_html import get_handlers
        self.server._tornado.add_handlers(r'.*', get_handlers(self.my_server))
