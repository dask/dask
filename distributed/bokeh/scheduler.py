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
from .worker import SystemMonitor
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


def systemmonitor_doc(scheduler, doc):
    with log_errors():
        table = StateTable(scheduler)
        sysmon = SystemMonitor(scheduler, sizing_mode='scale_width')
        doc.add_periodic_callback(table.update, 500)
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(column(table.root, sysmon.root,
                            sizing_mode='scale_width'))


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None):
        self.scheduler = scheduler
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc,
                                                            scheduler)))
        self.apps = {'/system': systemmonitor}

        self.loop = io_loop or scheduler.loop
        self.server = None
