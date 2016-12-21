from __future__ import print_function, division, absolute_import

from functools import partial
import logging
from math import sqrt

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.layouts import row, column, widgetbox
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem, BoxSelectTool,
    Circle, CategoricalAxis, Select
)
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
from bokeh.plotting import figure
from bokeh.palettes import Viridis11
from toolz import frequencies
from toolz.curried import map, concat, groupby, pipe

from .components import DashboardComponent
from .core import BokehServer
from .worker import SystemMonitor, format_time, counters_doc
from .utils import transpose
from ..compatibility import WINDOWS
from ..diagnostics.progress_stream import color_of
from ..metrics import time
from ..utils import log_errors, key_split, ignoring

with ignoring(ImportError):
    from cytoolz.curried import map, concat, groupby

logger = logging.getLogger(__name__)


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
                                            'color': ['red', 'blue']})

            fig = figure(title='Occupancy', tools='resize', id='bk-occupancy-plot',
                         x_axis_type='datetime', **kwargs)
            fig.rect(source=self.source, x='x', width='ms', y='y', height=1,
                     color='color')

            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            # fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0s')
            fig.x_range.start = 0

            hover = HoverTool()
            hover.tooltips = "@worker : @occupancy s"
            hover.point_policy = 'follow_mouse'
            fig.add_tools(hover)

            self.root = fig

    def update(self):
        with log_errors():
            o = self.scheduler.occupancy
            workers = list(self.scheduler.workers)
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
            self.source.data.update({'occupancy': occupancy,
                                     'worker': workers,
                                     'ms': ms,
                                     'color': color,
                                     'x': x, 'y': y})


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
            self.source.stream({'time': [time() * 1000],
                                'idle': [len(self.scheduler.idle)],
                                'saturated': [len(self.scheduler.saturated)]},
                                10000)


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
                self.source.stream(new, 10000)


def systemmonitor_doc(scheduler, doc):
    with log_errors():
        table = StateTable(scheduler)
        sysmon = SystemMonitor(scheduler, sizing_mode='scale_width')
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(column(table.root, sysmon.root,
                            sizing_mode='scale_width'))


def workers_doc(scheduler, doc):
    with log_errors():
        table = StateTable(scheduler)
        occupancy = Occupancy(scheduler, height=200, sizing_mode='scale_width')
        stealing_ts = StealingTimeSeries(scheduler, sizing_mode='scale_width')
        stealing_events = StealingEvents(scheduler, sizing_mode='scale_width')
        stealing_events.root.x_range = stealing_ts.root.x_range
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(occupancy.update, 500)
        doc.add_periodic_callback(stealing_ts.update, 500)
        doc.add_periodic_callback(stealing_events.update, 500)

        doc.add_root(column(table.root, occupancy.root, stealing_ts.root,
                            stealing_events.root,
                            sizing_mode='scale_width'))


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None):
        self.scheduler = scheduler
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc,
                                                            scheduler)))
        workers = Application(FunctionHandler(partial(workers_doc, scheduler)))
        counters = Application(FunctionHandler(partial(counters_doc, scheduler)))

        self.apps = {'/system': systemmonitor,
                     '/workers': workers,
                     '/counters': counters}

        self.loop = io_loop or scheduler.loop
        self.server = None
