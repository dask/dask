from __future__ import print_function, division, absolute_import

from functools import partial
import logging

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
from toolz import frequencies

from .components import DashboardComponent
from .core import BokehServer
from .worker import SystemMonitor, format_time
from ..compatibility import WINDOWS
from ..diagnostics.progress_stream import color_of
from ..metrics import time
from ..utils import log_errors, key_split

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
                                            'ms': [1, 2]})

            fig = figure(title='Occupancy', tools='resize', id='bk-occupancy-plot',
                         x_axis_type='datetime', **kwargs)
            fig.rect(source=self.source, x='x', width='ms', y='y', height=1,
                     color='blue')

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
            workers = sorted(o)
            y = list(range(len(workers)))
            occupancy = [o[w] for w in workers]
            ms = [occ * 1000 for occ in occupancy]
            x = [occ / 500 for occ in occupancy]
            total = sum(occupancy)
            if total:
                self.root.title.text = ('Occupancy -- total time: %s  wall time: %s' %
                                  (format_time(total),
                                  format_time(total / self.scheduler.total_ncores)))
            else:
                self.root.title.text = 'Occupancy'
            self.source.data.update({'occupancy': occupancy,
                                     'worker': workers,
                                     'ms': ms,
                                     'x': x, 'y': y})


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
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(occupancy.update, 500)

        doc.add_root(column(table.root, occupancy.root,
                            sizing_mode='scale_width'))


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None):
        self.scheduler = scheduler
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc,
                                                            scheduler)))
        workers = Application(FunctionHandler(partial(workers_doc, scheduler)))

        self.apps = {'/system': systemmonitor,
                     '/workers': workers}

        self.loop = io_loop or scheduler.loop
        self.server = None
