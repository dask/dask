from __future__ import annotations

import logging
import math
import operator
import os
from collections import OrderedDict, defaultdict
from collections.abc import Iterable
from datetime import datetime
from numbers import Number
from typing import Any, TypeVar

import numpy as np
from bokeh.core.properties import value, without_property_validation
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import (
    AdaptiveTicker,
    Arrow,
    BasicTicker,
    BoxSelectTool,
    BoxZoomTool,
    CDSView,
    ColorBar,
    ColumnDataSource,
    CustomJSHover,
    DataRange1d,
    DatetimeTickFormatter,
    FactorRange,
    GroupFilter,
    HelpTool,
    HoverTool,
    HTMLTemplateFormatter,
    MultiChoice,
    NumberFormatter,
    NumeralTickFormatter,
    OpenURL,
    PanTool,
    Range1d,
    ResetTool,
    Select,
    TabPanel,
    Tabs,
    TapTool,
    Title,
    VeeHead,
    WheelZoomTool,
)
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.models.widgets.markups import Div
from bokeh.palettes import Viridis11
from bokeh.plotting import figure
from bokeh.themes import Theme
from bokeh.transform import cumsum, factor_cmap, linear_cmap, stack
from jinja2 import Environment, FileSystemLoader
from packaging.version import parse as parse_version
from tlz import curry, pipe, second, valmap
from tlz.curried import concat, groupby, map

import dask
from dask import config
from dask.utils import (
    format_bytes,
    format_time,
    funcname,
    key_split,
    parse_bytes,
    parse_timedelta,
)

from distributed.core import Status
from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (
    DashboardComponent,
    ProfileServer,
    ProfileTimePlot,
    SystemMonitor,
)
from distributed.dashboard.utils import (
    _DATATABLE_STYLESHEETS_KWARGS,
    BOKEH_VERSION,
    PROFILING,
    transpose,
    update,
)
from distributed.diagnostics.graph_layout import GraphLayout
from distributed.diagnostics.progress import GroupTiming
from distributed.diagnostics.progress_stream import color_of, progress_quads
from distributed.diagnostics.task_stream import TaskStreamPlugin
from distributed.diagnostics.task_stream import color_of as ts_color_of
from distributed.diagnostics.task_stream import colors as ts_color_lookup
from distributed.metrics import time
from distributed.scheduler import Scheduler
from distributed.spans import SpansSchedulerExtension
from distributed.utils import Log, log_errors, url_escape

if dask.config.get("distributed.dashboard.export-tool"):
    from distributed.dashboard.export_tool import ExportTool
else:
    ExportTool = None  # type: ignore

T = TypeVar("T")

logger = logging.getLogger(__name__)

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "..", "http", "templates")
    )
)

BOKEH_THEME = Theme(
    filename=os.path.join(os.path.dirname(__file__), "..", "theme.yaml")
)
TICKS_1024 = {"base": 1024, "mantissas": [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]}
XLABEL_ORIENTATION = -math.pi / 9  # slanted downwards 20 degrees


logos_dict = {
    "numpy": "statics/images/numpy.png",
    "pandas": "statics/images/pandas.png",
    "builtins": "statics/images/python.png",
}


class Occupancy(DashboardComponent):
    """Occupancy (in time) per worker"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "occupancy": [0, 0],
                "worker": ["a", "b"],
                "x": [0.0, 0.1],
                "y": [1, 2],
                "ms": [1, 2],
                "color": ["red", "blue"],
                "escaped_worker": ["a", "b"],
            }
        )

        self.root = figure(
            title="Occupancy",
            tools="",
            toolbar_location="above",
            x_axis_type="datetime",
            min_border_bottom=50,
            **kwargs,
        )
        rect = self.root.rect(
            source=self.source, x="x", width="ms", y="y", height=0.9, color="color"
        )
        rect.nonselection_glyph = None

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.yaxis.visible = False
        self.root.ygrid.visible = False
        # fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0s')
        self.root.x_range.start = 0

        tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))

        hover = HoverTool()
        hover.tooltips = "@worker : @occupancy s."
        hover.point_policy = "follow_mouse"
        self.root.add_tools(hover, tap)

    @without_property_validation
    @log_errors
    def update(self):
        workers = self.scheduler.workers.values()

        y = list(range(len(workers)))
        occupancy = [ws.occupancy for ws in workers]
        ms = [occ * 1000 for occ in occupancy]
        x = [occ / 500 for occ in occupancy]
        total = sum(occupancy)
        color = []
        for ws in workers:
            if ws in self.scheduler.idle:
                color.append("red")
            elif ws in self.scheduler.saturated:
                color.append("green")
            else:
                color.append("blue")

        if total:
            self.root.title.text = (
                f"Occupancy -- total time: {format_time(total)} "
                f"wall time: {format_time(total / self.scheduler.total_nthreads)}"
            )
        else:
            self.root.title.text = "Occupancy"

        if occupancy:
            result = {
                "occupancy": occupancy,
                "worker": [ws.address for ws in workers],
                "ms": ms,
                "color": color,
                "escaped_worker": [url_escape(ws.address) for ws in workers],
                "x": x,
                "y": y,
            }

            update(self.source, result)


class ProcessingHistogram(DashboardComponent):
    """How many tasks are on each worker"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {"left": [1, 2], "right": [10, 10], "top": [0, 0]}
        )

        self.root = figure(
            title="Tasks Processing (count)",
            name="processing",
            y_axis_label="frequency",
            tools="",
            **kwargs,
        )

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.ygrid.visible = False

        self.root.toolbar_location = None

        self.root.quad(
            source=self.source,
            left="left",
            right="right",
            bottom=0,
            top="top",
            color="deepskyblue",
            fill_alpha=0.5,
        )

    @without_property_validation
    def update(self):
        L = [len(ws.processing) for ws in self.scheduler.workers.values()]
        counts, x = np.histogram(L, bins=40)
        self.source.data.update({"left": x[:-1], "right": x[1:], "top": counts})


class MemoryColor:
    """Change the color of the memory bars from blue to orange when process memory goes
    above the ``target`` threshold and to red when the worker pauses.
    Workers in ``closing_gracefully`` state will also be orange.

    If ``target`` is disabled, change to orange on ``spill`` instead.
    If spilling is completely disabled, never turn orange.

    If pausing is disabled, change to red when passing the ``terminate`` threshold
    instead. If both pause and terminate are disabled, turn red when passing
    ``memory_limit``.

    Note
    ----
    A worker will start spilling when managed memory alone passes the target threshold.
    However, here we're switching to orange when the process memory goes beyond target,
    which is usually earlier.
    This is deliberate for the sake of simplicity and also because, when the process
    memory passes the spill threshold, it will keep spilling until it falls below the
    target threshold - so it's not completely wrong. Again, we don't want to track
    the hysteresis cycle of the spill system here for the sake of simplicity.

    In short, orange should be treated as "the worker *may* be spilling".
    """

    orange: float
    red: float

    def __init__(
        self, neutral_color="blue", target_color="orange", terminated_color="red"
    ):
        self.neutral_color = neutral_color
        self.target_color = target_color
        self.terminated_color = terminated_color

        target = dask.config.get("distributed.worker.memory.target")
        spill = dask.config.get("distributed.worker.memory.spill")
        terminate = dask.config.get("distributed.worker.memory.terminate")

        # These values can be False. It's also common to configure them to impossibly
        # high values to achieve the same effect.
        self.orange = min(target or math.inf, spill or math.inf)
        self.red = min(terminate or math.inf, 1.0)

    def _memory_color(self, current: int, limit: int, status: Status) -> str:
        if status != Status.running:
            return self.terminated_color
        if not limit:
            return self.neutral_color
        if current >= limit * self.red:
            return self.terminated_color
        if current >= limit * self.orange:
            return self.target_color
        return self.neutral_color


class ClusterMemory(DashboardComponent, MemoryColor):
    """Total memory usage on the cluster"""

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        DashboardComponent.__init__(self)
        MemoryColor.__init__(self)

        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "width": [0] * 4,
                "x": [0] * 4,
                "y": [0] * 4,
                "color": ["blue", "blue", "blue", "grey"],
                "alpha": [1, 0.7, 0.4, 1],
                "proc_memory": [0] * 4,
                "managed": [0] * 4,
                "unmanaged_old": [0] * 4,
                "unmanaged_recent": [0] * 4,
                "spilled": [0] * 4,
            }
        )

        self.root = figure(
            title="Bytes stored on cluster",
            tools="",
            width=int(width / 2),
            name="cluster_memory",
            min_border_bottom=50,
            **kwargs,
        )
        rect = self.root.rect(
            source=self.source,
            x="x",
            y="y",
            width="width",
            height=0.9,
            color="color",
            alpha="alpha",
        )
        rect.nonselection_glyph = None

        self.root.axis[0].ticker = BasicTicker(**TICKS_1024)
        self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.x_range = Range1d(start=0)
        self.root.yaxis.visible = False
        self.root.ygrid.visible = False

        self.root.toolbar_location = "above"
        self.root.yaxis.visible = False

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                            <div>
                                <span style="font-size: 12px; font-weight: bold;">Process memory (RSS):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@proc_memory{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Managed:</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@managed{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Unmanaged (old):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@unmanaged_old{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Unmanaged (recent):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@unmanaged_recent{0.00 b}</span>
                            </div>
                            <div>
                                <span style="font-size: 12px; font-weight: bold;">Spilled to disk:</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@spilled{0.00 b}</span>
                            </div>
                            """,
        )
        help_ = HelpTool(
            redirect="https://docs.dask.org/en/stable/dashboard.html#bytes-stored-and-bytes-per-worker",
            description="Description of bytes stored plots",
        )
        self.root.add_tools(hover, help_)

    def _cluster_memory_color(self) -> str:
        colors = {
            self._memory_color(
                current=ws.memory.process,
                limit=getattr(ws, "memory_limit", 0),
                status=ws.status,
            )
            for ws in self.scheduler.workers.values()
        }

        assert colors.issubset({"red", "orange", "blue"})
        if "red" in colors:
            return "red"
        elif "orange" in colors:
            return "orange"
        else:
            return "blue"

    @without_property_validation
    @log_errors
    def update(self):
        limit = sum(ws.memory_limit for ws in self.scheduler.workers.values())
        meminfo = self.scheduler.memory
        color = self._cluster_memory_color()

        width = [
            meminfo.managed,
            meminfo.unmanaged_old,
            meminfo.unmanaged_recent,
            meminfo.spilled,
        ]

        result = {
            "width": width,
            "x": [sum(width[:i]) + w / 2 for i, w in enumerate(width)],
            "color": [color, color, color, "grey"],
            "proc_memory": [meminfo.process] * 4,
            "managed": [meminfo.managed] * 4,
            "unmanaged_old": [meminfo.unmanaged_old] * 4,
            "unmanaged_recent": [meminfo.unmanaged_recent] * 4,
            "spilled": [meminfo.spilled] * 4,
        }

        x_end = max(limit, meminfo.process + meminfo.spilled)
        self.root.x_range.end = x_end

        title = f"Bytes stored: {format_bytes(meminfo.process)}"
        if meminfo.spilled:
            title += f" + {format_bytes(meminfo.spilled)} spilled to disk"
        self.root.title.text = title

        update(self.source, result)


class WorkersMemory(DashboardComponent, MemoryColor):
    """Memory usage for single workers"""

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        DashboardComponent.__init__(self)
        MemoryColor.__init__(self)

        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "width": [],
                "x": [],
                "y": [],
                "color": [],
                "alpha": [],
                "worker": [],
                "escaped_worker": [],
                "proc_memory": [],
                "managed": [],
                "unmanaged_old": [],
                "unmanaged_recent": [],
                "spilled": [],
            }
        )

        self.root = figure(
            title="Bytes stored per worker",
            tools="",
            width=int(width / 2),
            name="workers_memory",
            min_border_bottom=50,
            **kwargs,
        )
        rect = self.root.rect(
            source=self.source,
            x="x",
            y="y",
            width="width",
            height=0.9,
            color="color",
            fill_alpha="alpha",
            line_width=0,
        )
        rect.nonselection_glyph = None

        self.root.axis[0].ticker = BasicTicker(**TICKS_1024)
        self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.x_range = Range1d(start=0)
        self.root.yaxis.visible = False
        self.root.ygrid.visible = False
        self.root.toolbar_location = None

        tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))
        self.root.add_tools(tap)

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                            <div>
                                <span style="font-size: 12px; font-weight: bold;">Worker:</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@worker</span>
                            </div>
                            <div>
                                <span style="font-size: 12px; font-weight: bold;">Process memory (RSS):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@proc_memory{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Managed:</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@managed{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Unmanaged (old):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@unmanaged_old{0.00 b}</span>
                            </div>
                            <div style="margin-left: 1em;">
                                <span style="font-size: 12px; font-weight: bold;">Unmanaged (recent):</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@unmanaged_recent{0.00 b}</span>
                            </div>
                            <div>
                                <span style="font-size: 12px; font-weight: bold;">Spilled to disk:</span>&nbsp;
                                <span style="font-size: 10px; font-family: Monaco, monospace;">@spilled{0.00 b}</span>
                            </div>
                            """,
        )
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        def quadlist(i: Iterable[T]) -> list[T]:
            out = []
            for ii in i:
                out += [ii, ii, ii, ii]
            return out

        workers = self.scheduler.workers.values()

        width = []
        x = []
        color = []
        max_limit = 0
        procmemory = []
        managed = []
        spilled = []
        unmanaged_old = []
        unmanaged_recent = []

        for ws in workers:
            meminfo = ws.memory
            limit = getattr(ws, "memory_limit", 0)
            max_limit = max(max_limit, limit, meminfo.process + meminfo.spilled)
            color_i = self._memory_color(meminfo.process, limit, ws.status)

            width += [
                meminfo.managed,
                meminfo.unmanaged_old,
                meminfo.unmanaged_recent,
                meminfo.spilled,
            ]
            x += [sum(width[-4:i]) + width[i] / 2 for i in range(-4, 0)]
            color += [color_i, color_i, color_i, "grey"]

            # memory info
            procmemory.append(meminfo.process)
            managed.append(meminfo.managed)
            unmanaged_old.append(meminfo.unmanaged_old)
            unmanaged_recent.append(meminfo.unmanaged_recent)
            spilled.append(meminfo.spilled)

        result = {
            "width": width,
            "x": x,
            "color": color,
            "alpha": [1, 0.7, 0.4, 1] * len(workers),
            "worker": quadlist(ws.address for ws in workers),
            "escaped_worker": quadlist(url_escape(ws.address) for ws in workers),
            "y": quadlist(range(len(workers))),
            "proc_memory": quadlist(procmemory),
            "managed": quadlist(managed),
            "unmanaged_old": quadlist(unmanaged_old),
            "unmanaged_recent": quadlist(unmanaged_recent),
            "spilled": quadlist(spilled),
        }
        # Remove rectangles with width=0
        result = {k: [vi for vi, w in zip(v, width) if w] for k, v in result.items()}

        self.root.x_range.end = max_limit
        update(self.source, result)


class WorkersMemoryHistogram(DashboardComponent):
    """Histogram of memory usage, showing how many workers there are in each bucket of
    usage. Replaces the per-worker graph when there are >= 50 workers.
    """

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {"left": [1, 2], "right": [10, 10], "top": [0, 0]}
        )

        self.root = figure(
            title="Bytes stored per worker",
            name="workers_memory",
            y_axis_label="frequency",
            tools="",
            **kwargs,
        )

        self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.xaxis.ticker = AdaptiveTicker(**TICKS_1024)
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.ygrid.visible = False

        self.root.toolbar_location = None

        self.root.quad(
            source=self.source,
            left="left",
            right="right",
            bottom=0,
            top="top",
            color="deepskyblue",
            fill_alpha=0.5,
        )

    @without_property_validation
    def update(self):
        nbytes = np.asarray(
            [ws.metrics["memory"] for ws in self.scheduler.workers.values()]
        )
        counts, x = np.histogram(nbytes, bins=40)
        d = {"left": x[:-1], "right": x[1:], "top": counts}
        update(self.source, d)


class WorkersTransferBytes(DashboardComponent):
    """Size of open data transfers from/to other workers per worker"""

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "escaped_worker": [],
                "transfer_incoming_bytes": [],
                "transfer_outgoing_bytes": [],
                "worker": [],
                "y_incoming": [],
                "y_outgoing": [],
            }
        )

        self.root = figure(
            title=f"Bytes transferring: {format_bytes(0)}",
            tools="",
            width=int(width / 2),
            name="workers_transfer_bytes",
            min_border_bottom=50,
            **kwargs,
        )

        # transfer_incoming_bytes
        self.root.hbar(
            name="transfer_incoming_bytes",
            y="y_incoming",
            right="transfer_incoming_bytes",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="red",
            source=self.source,
        )

        # transfer_outgoing_bytes
        self.root.hbar(
            name="transfer_outgoing_bytes",
            y="y_outgoing",
            right="transfer_outgoing_bytes",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="blue",
            source=self.source,
        )

        self.root.axis[0].ticker = BasicTicker(**TICKS_1024)
        self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.x_range = Range1d(start=0)
        self.root.yaxis.visible = False
        self.root.ygrid.visible = False
        self.root.toolbar_location = None

        tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))
        hover = HoverTool(
            tooltips=[
                ("Worker", "@worker"),
                ("Incoming", "@transfer_incoming_bytes{0.00 b}"),
                ("Outgoing", "@transfer_outgoing_bytes{0.00 b}"),
            ],
            point_policy="follow_mouse",
        )
        self.root.add_tools(hover, tap)

    @without_property_validation
    @log_errors
    def update(self):
        wss = self.scheduler.workers.values()

        h = 0.1
        y_incoming = [i + 0.75 + i * h for i in range(len(wss))]
        y_outgoing = [i + 0.25 + i * h for i in range(len(wss))]

        transfer_incoming_bytes = [
            ws.metrics["transfer"]["incoming_bytes"] for ws in wss
        ]
        transfer_outgoing_bytes = [
            ws.metrics["transfer"]["outgoing_bytes"] for ws in wss
        ]
        workers = [ws.address for ws in wss]
        escaped_workers = [url_escape(worker) for worker in workers]

        if wss:
            x_limit = max(
                max(transfer_incoming_bytes),
                max(transfer_outgoing_bytes),
                max(ws.memory_limit for ws in wss),
            )
        else:
            x_limit = 0
        self.root.x_range.end = x_limit

        result = {
            "escaped_worker": escaped_workers,
            "transfer_incoming_bytes": transfer_incoming_bytes,
            "transfer_outgoing_bytes": transfer_outgoing_bytes,
            "worker": workers,
            "y_incoming": y_incoming,
            "y_outgoing": y_outgoing,
        }
        self.root.title.text = (
            f"Bytes transferring: {format_bytes(sum(transfer_incoming_bytes))}"
        )
        update(self.source, result)


class Hardware(DashboardComponent):
    """Occupancy (in time) per worker"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        # Disk
        self.disk_source = ColumnDataSource(
            {
                "size": [],
                "bandwidth": [],
            }
        )

        self.disk_figure = figure(
            title="Disk Bandwidth -- Computing ...",
            tools="",
            toolbar_location="above",
            x_range=FactorRange(factors=[]),
            **kwargs,
        )
        self.disk_figure.vbar(
            x="size", top="bandwidth", width=0.9, source=self.disk_source
        )
        hover = HoverTool(
            mode="vline", tooltips=[("Bandwidth", "@bandwidth{0.00 b}/s")]
        )
        self.disk_figure.add_tools(hover)
        self.disk_figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.disk_figure.xgrid.visible = False

        # Memory
        self.memory_source = ColumnDataSource(
            {
                "size": [],
                "bandwidth": [],
            }
        )

        self.memory_figure = figure(
            title="Memory Bandwidth -- Computing ...",
            tools="",
            toolbar_location="above",
            x_range=FactorRange(factors=[]),
            **kwargs,
        )

        self.memory_figure.vbar(
            x="size", top="bandwidth", width=0.9, source=self.memory_source
        )
        hover = HoverTool(
            mode="vline", tooltips=[("Bandwidth", "@bandwidth{0.00 b}/s")]
        )
        self.memory_figure.add_tools(hover)
        self.memory_figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.memory_figure.xgrid.visible = False

        # Network
        self.network_source = ColumnDataSource(
            {
                "size": [],
                "bandwidth": [],
            }
        )

        self.network_figure = figure(
            title="Network Bandwidth -- Computing ...",
            tools="",
            toolbar_location="above",
            x_range=FactorRange(factors=[]),
            **kwargs,
        )

        self.network_figure.vbar(
            x="size", top="bandwidth", width=0.9, source=self.network_source
        )
        hover = HoverTool(
            mode="vline", tooltips=[("Bandwidth", "@bandwidth{0.00 b}/s")]
        )
        self.network_figure.add_tools(hover)
        self.network_figure.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.network_figure.xgrid.visible = False

        self.root = row(
            self.memory_figure,
            self.disk_figure,
            self.network_figure,
        )

        self.memory_data = {
            "size": [],
            "bandwidth": [],
        }
        self.disk_data = {
            "size": [],
            "bandwidth": [],
        }
        self.network_data = {
            "size": [],
            "bandwidth": [],
        }

        async def f():
            result = await self.scheduler.benchmark_hardware()

            for size in sorted(result["disk"], key=parse_bytes):
                bandwidth = result["disk"][size]
                self.disk_data["size"].append(size)
                self.disk_data["bandwidth"].append(bandwidth)

            for size in sorted(result["memory"], key=parse_bytes):
                bandwidth = result["memory"][size]
                self.memory_data["size"].append(size)
                self.memory_data["bandwidth"].append(bandwidth)

            for size in sorted(result["network"], key=parse_bytes):
                bandwidth = result["network"][size]
                self.network_data["size"].append(size)
                self.network_data["bandwidth"].append(bandwidth)

        self.scheduler.loop.add_callback(f)

    def update(self):
        if (
            not self.disk_data["size"]
            or self.disk_figure.title.text == "Disk Bandwidth"
        ):
            return

        self.network_figure.x_range.factors = self.network_data["size"]
        self.disk_figure.x_range.factors = self.disk_data["size"]
        self.memory_figure.x_range.factors = self.memory_data["size"]
        update(self.disk_source, self.disk_data)
        update(self.memory_source, self.memory_data)
        update(self.network_source, self.network_data)
        self.memory_figure.title.text = "Memory Bandwidth"
        self.disk_figure.title.text = "Disk Bandwidth"
        self.network_figure.title.text = "Network Bandwidth"


class BandwidthTypes(DashboardComponent):
    """Bar chart showing bandwidth per type"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "bandwidth": [1, 2],
                "bandwidth-half": [0.5, 1],
                "type": ["a", "b"],
                "bandwidth_text": ["1", "2"],
            }
        )

        self.root = figure(
            title="Bandwidth by Type",
            tools="",
            name="bandwidth_type_histogram",
            y_range=["a", "b"],
            **kwargs,
        )
        self.root.xaxis.major_label_orientation = -0.5
        rect = self.root.rect(
            source=self.source,
            x="bandwidth-half",
            y="type",
            width="bandwidth",
            height=0.9,
            color="blue",
        )
        self.root.x_range.start = 0
        self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.xaxis.ticker = AdaptiveTicker(**TICKS_1024)
        rect.nonselection_glyph = None

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.ygrid.visible = False

        self.root.toolbar_location = None

        hover = HoverTool()
        hover.tooltips = "@type: @bandwidth_text / s"
        hover.point_policy = "follow_mouse"
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        bw = self.scheduler.bandwidth_types
        self.root.y_range.factors = list(sorted(bw))
        result = {
            "bandwidth": list(bw.values()),
            "bandwidth-half": [b / 2 for b in bw.values()],
            "type": list(bw.keys()),
            "bandwidth_text": [format_bytes(x) for x in bw.values()],
        }
        self.root.title.text = "Bandwidth: " + format_bytes(self.scheduler.bandwidth)
        update(self.source, result)


class BandwidthWorkers(DashboardComponent):
    """How many tasks are on each worker"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "bandwidth": [1, 2],
                "source": ["a", "b"],
                "destination": ["a", "b"],
                "bandwidth_text": ["1", "2"],
            }
        )

        values = [hex(x)[2:] for x in range(64, 256)][::-1]
        mapper = linear_cmap(
            field_name="bandwidth",
            palette=["#" + x + x + "FF" for x in values],
            low=0,
            high=1,
        )

        self.root = figure(
            title="Bandwidth by Worker",
            tools="",
            name="bandwidth_worker_heatmap",
            x_range=["a", "b"],
            y_range=["a", "b"],
            **kwargs,
        )
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.root.rect(
            source=self.source,
            x="source",
            y="destination",
            color=mapper,
            height=1,
            width=1,
        )

        self.color_map = mapper["transform"]
        color_bar = ColorBar(
            color_mapper=self.color_map,
            label_standoff=12,
            border_line_color=None,
            location=(0, 0),
        )
        color_bar.formatter = NumeralTickFormatter(format="0.0 b")
        color_bar.ticker = AdaptiveTicker(**TICKS_1024)
        self.root.add_layout(color_bar, "right")

        self.root.toolbar_location = None

        hover = HoverTool()
        hover.tooltips = """
            <div>
                <p><b>Source:</b> @source </p>
                <p><b>Destination:</b> @destination </p>
                <p><b>Bandwidth:</b> @bandwidth_text / s</p>
            </div>
            """
        hover.point_policy = "follow_mouse"
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        bw = self.scheduler.bandwidth_workers
        if not bw:
            return

        def name(address):
            try:
                ws = self.scheduler.workers[address]
            except KeyError:
                return address
            if ws.name is not None:
                return str(ws.name)
            return address

        x, y, value = zip(*((name(a), name(b), c) for (a, b), c in bw.items()))

        self.color_map.high = max(value)

        factors = list(sorted(set(x + y)))
        self.root.x_range.factors = factors
        self.root.y_range.factors = factors[::-1]

        result = {
            "source": x,
            "destination": y,
            "bandwidth": value,
            "bandwidth_text": list(map(format_bytes, value)),
        }
        self.root.title.text = "Bandwidth: " + format_bytes(self.scheduler.bandwidth)
        update(self.source, result)


class WorkerNetworkBandwidth(DashboardComponent):
    """Worker network bandwidth chart

    Plots horizontal bars with the host_net_io.read_bps and host_net_io.write_bps worker
    state
    """

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "y_read": [],
                "y_write": [],
                "x_read": [],
                "x_write": [],
                "x_read_disk": [],
                "x_write_disk": [],
            }
        )

        self.bandwidth = figure(
            title="Worker Network Bandwidth",
            tools="",
            name="worker_network_bandwidth",
            **kwargs,
        )

        # host_net_io.read_bps
        self.bandwidth.hbar(
            y="y_read",
            right="x_read",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="red",
            legend_label="read",
            source=self.source,
        )

        # host_net_io.write_bps
        self.bandwidth.hbar(
            y="y_write",
            right="x_write",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="blue",
            legend_label="write",
            source=self.source,
        )

        self.bandwidth.axis[0].ticker = BasicTicker(**TICKS_1024)
        self.bandwidth.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.bandwidth.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.bandwidth.xaxis.minor_tick_line_alpha = 0
        self.bandwidth.x_range = Range1d(start=0)
        self.bandwidth.yaxis.visible = False
        self.bandwidth.ygrid.visible = False
        self.bandwidth.toolbar_location = None

        self.disk = figure(
            title="Workers Disk",
            tools="",
            name="worker_disk",
            **kwargs,
        )

        # host_disk_io.read_bps
        self.disk.hbar(
            y="y_read",
            right="x_read_disk",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="red",
            legend_label="read",
            source=self.source,
        )

        # host_disk_io.write_bps
        self.disk.hbar(
            y="y_write",
            right="x_write_disk",
            line_color=None,
            left=0,
            height=0.5,
            fill_color="blue",
            legend_label="write",
            source=self.source,
        )

        self.disk.axis[0].ticker = BasicTicker(**TICKS_1024)
        self.disk.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.disk.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.disk.xaxis.minor_tick_line_alpha = 0
        self.disk.x_range = Range1d(start=0)
        self.disk.yaxis.visible = False
        self.disk.ygrid.visible = False
        self.disk.toolbar_location = None

    @without_property_validation
    @log_errors
    def update(self):
        workers = self.scheduler.workers.values()

        h = 0.1
        y_read = [i + 0.75 + i * h for i in range(len(workers))]
        y_write = [i + 0.25 + i * h for i in range(len(workers))]

        x_read = []
        x_write = []
        x_read_disk = []
        x_write_disk = []

        for ws in workers:
            x_read.append(ws.metrics["host_net_io"]["read_bps"])
            x_write.append(ws.metrics["host_net_io"]["write_bps"])
            x_read_disk.append(ws.metrics.get("host_disk_io", {}).get("read_bps", 0))
            x_write_disk.append(ws.metrics.get("host_disk_io", {}).get("write_bps", 0))

        if self.scheduler.workers:
            self.bandwidth.x_range.end = max(
                max(x_read),
                max(x_write),
                100_000_000,
                0.95 * self.bandwidth.x_range.end,
            )

            self.disk.x_range.end = max(
                max(x_read_disk),
                max(x_write_disk),
                100_000_000,
                0.95 * self.disk.x_range.end,
            )
        else:
            self.bandwidth.x_range.end = 100_000_000
            self.disk.x_range.end = 100_000_000

        result = {
            "y_read": y_read,
            "y_write": y_write,
            "x_read": x_read,
            "x_write": x_write,
            "x_read_disk": x_read_disk,
            "x_write_disk": x_write_disk,
        }

        update(self.source, result)


class SystemTimeseries(DashboardComponent):
    """Timeseries for worker network bandwidth, cpu, memory and disk.

    bandwidth
        Plots the average of host_net_io.read_bps and host_net_io.write_bps for the
        workers as a function of time
    cpu
        Plots the average of cpu for the workers as a function of time
    memory
        Plots the average of memory for the workers as a function of time
    disk
        Plots the average of host_disk_io.read_bps and host_disk_io.write_bps for the
        workers as a function of time

    The metrics plotted come from the aggregation of from ws.metrics[key] for ws in
    scheduler.workers.values() divided by number of workers.
    """

    @log_errors
    def __init__(self, scheduler, follow_interval=20000, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "time": [],
                "host_net_io.read_bps": [],
                "host_net_io.write_bps": [],
                "cpu": [],
                "memory": [],
                "host_disk_io.read_bps": [],
                "host_disk_io.write_bps": [],
            }
        )

        update(self.source, self.get_data())

        x_range = DataRange1d(
            follow="end", follow_interval=follow_interval, range_padding=0
        )
        tools = "reset, xpan, xwheel_zoom"

        self.bandwidth = figure(
            title="Worker Network Bandwidth (average)",
            x_axis_type="datetime",
            tools=tools,
            x_range=x_range,
            name="worker_network_bandwidth-timeseries",
            **kwargs,
        )

        self.bandwidth.line(
            source=self.source,
            x="time",
            y="host_net_io.read_bps",
            color="red",
            legend_label="read (mean)",
        )
        self.bandwidth.line(
            source=self.source,
            x="time",
            y="host_net_io.write_bps",
            color="blue",
            legend_label="write (mean)",
        )

        self.bandwidth.legend.location = "top_left"
        self.bandwidth.yaxis.axis_label = "bytes / second"
        self.bandwidth.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        self.bandwidth.y_range.start = 0
        self.bandwidth.yaxis.minor_tick_line_alpha = 0
        self.bandwidth.xgrid.visible = False

        self.cpu = figure(
            title="Worker CPU Utilization (average)",
            x_axis_type="datetime",
            tools=tools,
            x_range=x_range,
            name="worker_cpu-timeseries",
            **kwargs,
        )

        self.cpu.line(
            source=self.source,
            x="time",
            y="cpu",
        )
        self.cpu.yaxis.axis_label = "Utilization"
        self.cpu.y_range.start = 0
        self.cpu.yaxis.minor_tick_line_alpha = 0
        self.cpu.xgrid.visible = False

        self.memory = figure(
            title="Worker Memory Use (average)",
            x_axis_type="datetime",
            tools=tools,
            x_range=x_range,
            name="worker_memory-timeseries",
            **kwargs,
        )

        self.memory.line(
            source=self.source,
            x="time",
            y="memory",
        )
        self.memory.yaxis.axis_label = "Bytes"
        self.memory.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        self.memory.y_range.start = 0
        self.memory.yaxis.minor_tick_line_alpha = 0
        self.memory.xgrid.visible = False

        self.disk = figure(
            title="Worker Disk Bandwidth (average)",
            x_axis_type="datetime",
            tools=tools,
            x_range=x_range,
            name="worker_disk-timeseries",
            **kwargs,
        )

        self.disk.line(
            source=self.source,
            x="time",
            y="host_disk_io.read_bps",
            color="red",
            legend_label="read (mean)",
        )
        self.disk.line(
            source=self.source,
            x="time",
            y="host_disk_io.write_bps",
            color="blue",
            legend_label="write (mean)",
        )

        self.disk.legend.location = "top_left"
        self.disk.yaxis.axis_label = "bytes / second"
        self.disk.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        self.disk.y_range.start = 0
        self.disk.yaxis.minor_tick_line_alpha = 0
        self.disk.xgrid.visible = False

    def get_data(self):
        workers = self.scheduler.workers.values()

        net_read_bps = 0
        net_write_bps = 0
        cpu = 0
        memory = 0
        disk_read_bps = 0
        disk_write_bps = 0
        time = 0
        for ws in workers:
            net_read_bps += ws.metrics["host_net_io"]["read_bps"]
            net_write_bps += ws.metrics["host_net_io"]["write_bps"]
            cpu += ws.metrics["cpu"]
            memory += ws.metrics["memory"]
            disk_read_bps += ws.metrics.get("host_disk_io", {}).get("read_bps", 0)
            disk_write_bps += ws.metrics.get("host_disk_io", {}).get("write_bps", 0)
            time += ws.metrics["time"]

        result = {
            # use `or` to avoid ZeroDivision when no workers
            "time": [time / (len(workers) or 1) * 1000],
            "host_net_io.read_bps": [net_read_bps / (len(workers) or 1)],
            "host_net_io.write_bps": [net_write_bps / (len(workers) or 1)],
            "cpu": [cpu / (len(workers) or 1)],
            "memory": [memory / (len(workers) or 1)],
            "host_disk_io.read_bps": [disk_read_bps / (len(workers) or 1)],
            "host_disk_io.write_bps": [disk_write_bps / (len(workers) or 1)],
        }
        return result

    @without_property_validation
    @log_errors
    def update(self):
        self.source.stream(self.get_data(), 1000)

        if self.scheduler.workers:
            y_end_cpu = sum(
                ws.nthreads or 1 for ws in self.scheduler.workers.values()
            ) / len(self.scheduler.workers.values())
            y_end_mem = sum(
                ws.memory_limit for ws in self.scheduler.workers.values()
            ) / len(self.scheduler.workers.values())
        else:
            y_end_cpu = 1
            y_end_mem = 100_000_000

        self.cpu.y_range.end = y_end_cpu * 100
        self.memory.y_range.end = y_end_mem


class ComputePerKey(DashboardComponent):
    """Bar chart showing time spend in action by key prefix"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler

        if TaskStreamPlugin.name not in self.scheduler.plugins:
            self.scheduler.add_plugin(TaskStreamPlugin(self.scheduler))

        compute_data = {
            "times": [0.2, 0.1],
            "formatted_time": ["0.2 ms", "2.8 us"],
            "angles": [3.14, 0.785],
            "color": [ts_color_lookup["transfer"], ts_color_lookup["compute"]],
            "names": ["sum", "sum_partial"],
        }

        self.compute_source = ColumnDataSource(data=compute_data)

        fig = figure(
            title="Compute Time Per Task",
            tools="",
            name="compute_time_per_key",
            x_range=["a", "b"],
            **kwargs,
        )

        rect = fig.vbar(
            source=self.compute_source,
            x="names",
            top="times",
            width=0.7,
            color="color",
        )

        fig.y_range.start = 0
        fig.yaxis.axis_label = "Time (s)"
        fig.yaxis[0].formatter = NumeralTickFormatter(format="0")
        fig.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
        fig.xaxis.major_label_orientation = XLABEL_ORIENTATION
        rect.nonselection_glyph = None

        fig.xaxis.minor_tick_line_alpha = 0
        fig.xgrid.visible = False

        fig.toolbar_location = None

        hover = HoverTool()
        hover.tooltips = """
            <div>
                <p><b>Name:</b> @names</p>
                <p><b>Time:</b> @formatted_time</p>
            </div>
            """
        hover.point_policy = "follow_mouse"
        fig.add_tools(hover)

        fig.add_layout(
            Title(
                text="Note: tasks less than 2% of max are not displayed",
                text_font_style="italic",
            ),
            "below",
        )

        self.fig = fig
        tab1 = TabPanel(child=fig, title="Bar Chart")

        fig2 = figure(
            title="Compute Time Per Task",
            tools="",
            name="compute_time_per_key-pie",
            x_range=(-0.5, 1.0),
            **kwargs,
        )

        fig2.wedge(
            x=0,
            y=1,
            radius=0.4,
            start_angle=cumsum("angles", include_zero=True),
            end_angle=cumsum("angles"),
            line_color="white",
            fill_color="color",
            legend_field="names",
            source=self.compute_source,
        )

        fig2.axis.axis_label = None
        fig2.axis.visible = False
        fig2.grid.grid_line_color = None
        fig2.add_layout(
            Title(
                text="Note: tasks less than 2% of max are not displayed",
                text_font_style="italic",
            ),
            "below",
        )

        hover = HoverTool()
        hover.tooltips = """
            <div>
                <p><b>Name:</b> @names</p>
                <p><b>Time:</b> @formatted_time</p>
            </div>
            """
        hover.point_policy = "follow_mouse"
        fig2.add_tools(hover)
        self.wedge_fig = fig2
        tab2 = TabPanel(child=fig2, title="Pie Chart")

        self.root = Tabs(tabs=[tab1, tab2], sizing_mode="stretch_both")

    @without_property_validation
    @log_errors
    def update(self):
        compute_times = defaultdict(float)

        for name, tp in self.scheduler.task_prefixes.items():
            for action, t in tp.all_durations.items():
                if action == "compute":
                    compute_times[name] += t

        if not compute_times:
            return

        # order by largest time first
        compute_times = sorted(compute_times.items(), key=second, reverse=True)

        # Keep only times which are 2% of max or greater
        max_time = compute_times[0][1] * 0.02
        compute_colors = []
        compute_names = []
        compute_time = []
        total_time = 0
        for name, t in compute_times:
            if t < max_time:
                break
            compute_names.append(name)
            compute_colors.append(ts_color_of(name))
            compute_time.append(t)
            total_time += t

        angles = [t / total_time * 2 * math.pi for t in compute_time]

        self.fig.x_range.factors = compute_names

        compute_result = dict(
            angles=angles,
            times=compute_time,
            color=compute_colors,
            names=compute_names,
            formatted_time=[format_time(t) for t in compute_time],
        )

        update(self.compute_source, compute_result)


class AggregateAction(DashboardComponent):
    """Bar chart showing time spend in action by key prefix"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler

        if TaskStreamPlugin.name not in self.scheduler.plugins:
            self.scheduler.add_plugin(TaskStreamPlugin(self.scheduler))

        action_data = {
            "times": [0.2, 0.1],
            "formatted_time": ["0.2 ms", "2.8 us"],
            "color": [ts_color_lookup["transfer"], ts_color_lookup["compute"]],
            "names": ["transfer", "compute"],
        }

        self.action_source = ColumnDataSource(data=action_data)

        self.root = figure(
            title="Aggregate Per Action",
            tools="",
            name="aggregate_per_action",
            x_range=["a", "b"],
            **kwargs,
        )

        rect = self.root.vbar(
            source=self.action_source,
            x="names",
            top="times",
            width=0.7,
            color="color",
        )

        self.root.y_range.start = 0
        self.root.yaxis[0].formatter = NumeralTickFormatter(format="0")
        self.root.yaxis.axis_label = "Time (s)"
        self.root.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        self.root.xaxis.major_label_text_font_size = "16px"
        rect.nonselection_glyph = None

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.xgrid.visible = False

        self.root.toolbar_location = None

        hover = HoverTool()
        hover.tooltips = """
            <div>
                <p><b>Name:</b> @names</p>
                <p><b>Time:</b> @formatted_time</p>
            </div>
            """
        hover.point_policy = "follow_mouse"
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        agg_times = defaultdict(float)

        for ts in self.scheduler.task_prefixes.values():
            for action, t in ts.all_durations.items():
                agg_times[action] += t

        # order by largest time first
        agg_times = sorted(agg_times.items(), key=lambda x: x[1], reverse=True)

        agg_colors = list()
        agg_names = list()
        agg_time = list()
        for action, t in agg_times:
            agg_names.append(action)
            if action == "compute":
                agg_colors.append("purple")
            else:
                agg_colors.append(ts_color_lookup[action])
            agg_time.append(t)

        self.root.x_range.factors = agg_names
        self.root.title.text = "Aggregate Time Per Action"

        action_result = dict(
            times=agg_time,
            color=agg_colors,
            names=agg_names,
            formatted_time=[format_time(t) for t in agg_time],
        )

        update(self.action_source, action_result)


class MemoryByKey(DashboardComponent):
    """Bar chart showing memory use by key prefix"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "name": ["a", "b"],
                "nbytes": [100, 1000],
                "count": [1, 2],
                "color": ["blue", "blue"],
            }
        )

        self.root = figure(
            title="Memory Use",
            tools="",
            name="memory_by_key",
            x_range=["a", "b"],
            **kwargs,
        )
        rect = self.root.vbar(
            source=self.source, x="name", top="nbytes", width=0.9, color="color"
        )
        self.root.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        self.root.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
        self.root.xaxis.major_label_orientation = XLABEL_ORIENTATION
        rect.nonselection_glyph = None

        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.ygrid.visible = False

        self.root.toolbar_location = None

        hover = HoverTool()
        hover.tooltips = "@name: @nbytes_text"
        hover.tooltips = """
            <div>
                <p><b>Name:</b> @name</p>
                <p><b>Bytes:</b> @nbytes_text </p>
                <p><b>Count:</b> @count objects </p>
            </div>
            """
        hover.point_policy = "follow_mouse"
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        counts = defaultdict(int)
        nbytes = defaultdict(int)
        for ws in self.scheduler.workers.values():
            for ts in ws.has_what:
                ks = key_split(ts.key)
                counts[ks] += 1
                nbytes[ks] += ts.nbytes

        names = list(sorted(counts))
        self.root.x_range.factors = names
        result = {
            "name": names,
            "count": [counts[name] for name in names],
            "nbytes": [nbytes[name] for name in names],
            "nbytes_text": [format_bytes(nbytes[name]) for name in names],
            "color": [color_of(name) for name in names],
        }
        self.root.title.text = "Total Use: " + format_bytes(sum(nbytes.values()))

        update(self.source, result)


class CurrentLoad(DashboardComponent):
    """Tasks and CPU usage on each worker"""

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "nprocessing": [],
                "nprocessing-half": [],
                "nprocessing-color": [],
                "cpu": [],
                "cpu-half": [],
                "y": [],
                "worker": [],
                "escaped_worker": [],
            }
        )
        processing = figure(
            title="Tasks Processing",
            tools="",
            name="processing",
            width=int(width / 2),
            min_border_bottom=50,
            **kwargs,
        )
        rect = processing.rect(
            source=self.source,
            x="nprocessing-half",
            y="y",
            width="nprocessing",
            height=0.9,
            color="nprocessing-color",
        )
        processing.x_range.start = 0
        rect.nonselection_glyph = None

        cpu = figure(
            title="CPU Utilization",
            tools="",
            width=int(width / 2),
            name="cpu_hist",
            x_range=(0, 100),
            min_border_bottom=50,
            **kwargs,
        )
        rect = cpu.rect(
            source=self.source,
            x="cpu-half",
            y="y",
            width="cpu",
            height=0.9,
            color="blue",
        )
        rect.nonselection_glyph = None

        for fig in (processing, cpu):
            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False

            tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))
            fig.add_tools(tap)

            fig.toolbar_location = None
            fig.yaxis.visible = False

        hover = HoverTool()
        hover.tooltips = "@worker : @nprocessing tasks"
        hover.point_policy = "follow_mouse"
        processing.add_tools(hover)

        hover = HoverTool()
        hover.tooltips = "@worker : @cpu %"
        hover.point_policy = "follow_mouse"
        cpu.add_tools(hover)

        self.processing_figure = processing
        self.cpu_figure = cpu

    @without_property_validation
    @log_errors
    def update(self):
        workers = self.scheduler.workers.values()
        now = time()
        if not any(ws.processing for ws in workers) and now < self.last + 1:
            return
        self.last = now

        cpu = [int(ws.metrics["cpu"]) for ws in workers]
        nprocessing = [len(ws.processing) for ws in workers]

        nprocessing_color = []
        for ws in workers:
            if ws in self.scheduler.idle:
                nprocessing_color.append("red")
            elif ws in self.scheduler.saturated:
                nprocessing_color.append("green")
            else:
                nprocessing_color.append("blue")

        result = {
            "cpu": cpu,
            "cpu-half": [c / 2 for c in cpu],
            "nprocessing": nprocessing,
            "nprocessing-half": [np / 2 for np in nprocessing],
            "nprocessing-color": nprocessing_color,
            "worker": [ws.address for ws in workers],
            "escaped_worker": [url_escape(ws.address) for ws in workers],
            "y": list(range(len(workers))),
        }

        if self.scheduler.workers:
            xrange = max(ws.nthreads or 1 for ws in workers)
        else:
            xrange = 1
        self.cpu_figure.x_range.end = xrange * 100

        update(self.source, result)


class StealingTimeSeries(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "time": [time() * 1000, time() * 1000 + 1],
                "idle": [0, 0],
                "saturated": [0, 0],
            }
        )

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        self.root = figure(
            title="Idle and Saturated Workers Over Time",
            x_axis_type="datetime",
            tools="",
            x_range=x_range,
            **kwargs,
        )
        self.root.line(source=self.source, x="time", y="idle", color="red")
        self.root.line(source=self.source, x="time", y="saturated", color="green")
        self.root.yaxis.minor_tick_line_color = None

        self.root.add_tools(
            ResetTool(), PanTool(dimensions="width"), WheelZoomTool(dimensions="width")
        )

    @without_property_validation
    @log_errors
    def update(self):
        result = {
            "time": [time() * 1000],
            "idle": [len(self.scheduler.idle)],
            "saturated": [len(self.scheduler.saturated)],
        }
        if PROFILING:
            curdoc().add_next_tick_callback(lambda: self.source.stream(result, 10000))
        else:
            self.source.stream(result, 10000)


class StealingEvents(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.steal = scheduler.extensions["stealing"]
        self.last = 0
        self.source = ColumnDataSource(
            {
                "time": [time() - 60, time()],
                "level": [0, 15],
                "color": ["white", "white"],
                "duration": [0, 0],
                "radius": [1, 1],
                "cost_factor": [0, 10],
                "count": [1, 1],
            }
        )

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        self.root = figure(
            title="Stealing Events",
            x_axis_type="datetime",
            tools="",
            x_range=x_range,
            **kwargs,
        )

        self.root.scatter(
            source=self.source,
            x="time",
            y="level",
            color="color",
            size="radius",
            alpha=0.5,
        )
        self.root.yaxis.axis_label = "Level"

        hover = HoverTool()
        hover.tooltips = "Level: @level, Duration: @duration, Count: @count, Cost factor: @cost_factor"
        hover.point_policy = "follow_mouse"

        self.root.add_tools(
            hover,
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
        )

    def convert(self, msgs):
        """Convert a log message to a glyph"""
        total_duration = 0
        for msg in msgs:
            time, level, key, duration, sat, occ_sat, idl, occ_idl = msg[:8]
            total_duration += duration

        try:
            color = Viridis11[level]
        except (KeyError, IndexError):
            color = "black"

        radius = math.sqrt(min(total_duration, 10)) * 30 + 2

        d = {
            "time": time * 1000,
            "level": level,
            "count": len(msgs),
            "color": color,
            "duration": total_duration,
            "radius": radius,
            "cost_factor": self.steal.cost_multipliers[level],
        }

        return d

    @without_property_validation
    @log_errors
    def update(self):
        topic = self.scheduler._broker._topics["stealing"]
        log = log = topic.events
        n = min(topic.count - self.last, len(log))
        if log:
            log = [log[-i][1][1] for i in range(1, n + 1) if log[-i][1][0] == "request"]
        self.last = topic.count

        if log:
            new = pipe(
                log,
                map(groupby(1)),
                map(dict.values),
                concat,
                map(self.convert),
                list,
                transpose,
            )
            if PROFILING:
                curdoc().add_next_tick_callback(lambda: self.source.stream(new, 10000))
            else:
                self.source.stream(new, 10000)


class Events(DashboardComponent):
    def __init__(self, scheduler, name, height=150, **kwargs):
        self.scheduler = scheduler
        self.action_ys = dict()
        self.last = 0
        self.name = name
        self.source = ColumnDataSource(
            {"time": [], "action": [], "hover": [], "y": [], "color": []}
        )

        x_range = DataRange1d(follow="end", follow_interval=200000)

        self.root = figure(
            title=name,
            x_axis_type="datetime",
            height=height,
            tools="",
            x_range=x_range,
            **kwargs,
        )

        self.root.scatter(
            source=self.source,
            x="time",
            y="y",
            color="color",
            size=50,
            alpha=0.5,
            legend_field="action",
        )
        self.root.yaxis.axis_label = "Action"
        self.root.legend.location = "top_left"

        hover = HoverTool()
        hover.tooltips = "@action<br>@hover"
        hover.point_policy = "follow_mouse"

        self.root.add_tools(
            hover,
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
        )

    @without_property_validation
    @log_errors
    def update(self):
        topic = self.scheduler._broker._topics[self.name]
        log = topic.events
        n = min(topic.count - self.last, len(log))
        if log:
            log = [log[-i] for i in range(1, n + 1)]
        self.last = topic.count

        if log:
            actions = []
            times = []
            hovers = []
            ys = []
            colors = []
            for msg_time, msg in log:
                times.append(msg_time * 1000)
                action = msg["action"]
                actions.append(action)
                try:
                    ys.append(self.action_ys[action])
                except KeyError:
                    self.action_ys[action] = len(self.action_ys)
                    ys.append(self.action_ys[action])
                colors.append(color_of(action))
                hovers.append("TODO")

            new = {
                "time": times,
                "action": actions,
                "hover": hovers,
                "y": ys,
                "color": colors,
            }

            if PROFILING:
                curdoc().add_next_tick_callback(lambda: self.source.stream(new, 10000))
            else:
                self.source.stream(new, 10000)


class TaskStream(DashboardComponent):
    def __init__(self, scheduler, n_rectangles=1000, clear_interval="20s", **kwargs):
        self.scheduler = scheduler
        self.offset = 0

        if TaskStreamPlugin.name not in self.scheduler.plugins:
            self.scheduler.add_plugin(TaskStreamPlugin(self.scheduler))
        self.plugin = self.scheduler.plugins[TaskStreamPlugin.name]

        self.index = max(0, self.plugin.index - n_rectangles)
        self.workers = dict()
        self.n_rectangles = n_rectangles
        clear_interval = parse_timedelta(clear_interval, default="ms")
        self.clear_interval = clear_interval
        self.last = 0
        self.last_seen = 0

        self.source, self.root = task_stream_figure(clear_interval, **kwargs)

        # Required for update callback
        self.task_stream_index = [0]

    @without_property_validation
    @log_errors
    def update(self):
        if self.index == self.plugin.index:
            return

        if self.index and len(self.source.data["start"]):
            start = min(self.source.data["start"])
            duration = max(self.source.data["duration"])
            boundary = (self.offset + start - duration) / 1000
        else:
            boundary = self.offset
        rectangles = self.plugin.rectangles(
            istart=self.index, workers=self.workers, start_boundary=boundary
        )
        n = len(rectangles["name"])
        self.index = self.plugin.index

        if not rectangles["start"]:
            return

        # If it has been a while since we've updated the plot
        if time() > self.last_seen + self.clear_interval:
            new_start = min(rectangles["start"]) - self.offset
            old_start = min(self.source.data["start"])
            old_end = max(
                map(
                    operator.add,
                    self.source.data["start"],
                    self.source.data["duration"],
                )
            )

            density = (
                sum(self.source.data["duration"])
                / len(self.workers)
                / (old_end - old_start)
            )

            # If whitespace is more than 3x the old width
            if (new_start - old_end) > (old_end - old_start) * 2 or density < 0.05:
                self.source.data.update({k: [] for k in rectangles})  # clear
                self.offset = min(rectangles["start"])  # redefine offset

        rectangles["start"] = [x - self.offset for x in rectangles["start"]]
        self.last_seen = time()

        # Convert to numpy for serialization speed
        if n >= 10 and np:
            for k, v in rectangles.items():
                if isinstance(v[0], Number):
                    rectangles[k] = np.array(v)

        if PROFILING:
            curdoc().add_next_tick_callback(
                lambda: self.source.stream(rectangles, self.n_rectangles)
            )
        else:
            self.source.stream(rectangles, self.n_rectangles)


def task_stream_figure(clear_interval="20s", **kwargs):
    """
    kwargs are applied to the bokeh.models.plots.Plot constructor
    """
    clear_interval = parse_timedelta(clear_interval, default="ms")

    source = ColumnDataSource(
        data=dict(
            start=[time() - clear_interval],
            duration=[0.1],
            key=["start"],
            name=["start"],
            color=["white"],
            duration_text=["100 ms"],
            worker=["foo"],
            y=[0],
            worker_thread=[1],
            alpha=[0.0],
        )
    )

    x_range = DataRange1d(range_padding=0)
    y_range = DataRange1d(range_padding=0)

    root = figure(
        name="task_stream",
        title="Task Stream",
        x_range=x_range,
        y_range=y_range,
        toolbar_location="above",
        x_axis_type=(
            "timedelta" if BOKEH_VERSION >= parse_version("3.8.0") else "datetime"
        ),
        y_axis_location=None,
        tools="",
        min_border_bottom=50,
        **kwargs,
    )

    rect = root.rect(
        source=source,
        x="start",
        y="y",
        width="duration",
        height=0.4,
        fill_color="color",
        line_color="color",
        line_alpha=0.6,
        fill_alpha="alpha",
        line_width=3,
    )
    rect.nonselection_glyph = None

    root.yaxis.major_label_text_alpha = 0
    root.yaxis.minor_tick_line_alpha = 0
    root.yaxis.major_tick_line_alpha = 0
    if BOKEH_VERSION < parse_version("3.8.0") and BOKEH_VERSION >= parse_version(
        "3.5.2"
    ):
        root.xaxis[0].formatter = DatetimeTickFormatter(
            boundary_scaling=False, context=None
        )  # remove full date context misleading for users
    root.xgrid.visible = False

    hover = HoverTool(
        point_policy="follow_mouse",
        tooltips="""
            <div>
                <span style="font-size: 12px; font-weight: bold;">@name:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@duration_text</span>
            </div>
            """,
    )

    tap = TapTool(callback=OpenURL(url="./profile?key=@name"))
    help_ = HelpTool(
        redirect="https://docs.dask.org/en/stable/dashboard.html#task-stream",
        description="A description of the TaskStream and its color palette.",
    )
    root.add_tools(
        hover,
        tap,
        BoxZoomTool(),
        ResetTool(),
        PanTool(dimensions="width"),
        WheelZoomTool(dimensions="width"),
        help_,
    )
    if ExportTool:  # type: ignore
        export = ExportTool()
        export.register_plot(root)
        root.add_tools(export)

    return source, root


class TaskGraph(DashboardComponent):
    """
    A dynamic node-link diagram for the task graph on the scheduler

    See also the GraphLayout diagnostic at
    distributed/diagnostics/graph_layout.py
    """

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.layout = GraphLayout(scheduler)
        scheduler.add_plugin(self.layout)
        self.invisible_count = 0  # number of invisible nodes

        self.node_source = ColumnDataSource(
            {"x": [], "y": [], "name": [], "state": [], "visible": [], "key": []}
        )
        self.edge_source = ColumnDataSource({"x": [], "y": [], "visible": []})

        filter = GroupFilter(column_name="visible", group="True")
        node_view = CDSView(filter=filter)
        edge_view = CDSView(filter=filter)
        node_colors = factor_cmap(
            "state",
            factors=["waiting", "queued", "processing", "memory", "released", "erred"],
            palette=["gray", "yellow", "green", "red", "blue", "black"],
        )

        self.root = figure(title="Task Graph", **kwargs)
        self.subtitle = Title(text=" ", text_font_style="italic")
        self.root.add_layout(self.subtitle, "above")

        self.root.multi_line(
            xs="x",
            ys="y",
            source=self.edge_source,
            line_width=1,
            view=edge_view,
            color="black",
            alpha=0.3,
        )
        rect = self.root.scatter(
            x="x",
            y="y",
            size=10,
            color=node_colors,
            source=self.node_source,
            view=node_view,
            legend_field="state",
            marker="square",
        )
        self.root.xgrid.grid_line_color = None
        self.root.ygrid.grid_line_color = None
        self.root.xaxis.visible = False
        self.root.yaxis.visible = False

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="<b>@name</b>: @state",
            renderers=[rect],
        )
        tap = TapTool(callback=OpenURL(url="info/task/@key.html"), renderers=[rect])
        rect.nonselection_glyph = None
        self.root.add_tools(hover, tap)
        self.max_items = config.get("distributed.dashboard.graph-max-items", 5000)

    @without_property_validation
    @log_errors
    def update(self):
        # If there are too many tasks in the scheduler we'll disable this
        # compoonents to not overload scheduler or client. Once we drop
        # below the threshold, the data is filled up again as usual
        if len(self.scheduler.tasks) > self.max_items:
            self.subtitle.text = "Scheduler has too many tasks to display."
            for container in [self.node_source, self.edge_source]:
                container.data = {col: [] for col in container.column_names}
        else:
            # occasionally reset the column data source to remove old nodes
            if self.invisible_count > len(self.node_source.data["x"]) / 2:
                self.layout.reset_index()
                self.invisible_count = 0
                update = True
            else:
                update = False

            new, self.layout.new = self.layout.new, []
            new_edges = self.layout.new_edges
            self.layout.new_edges = []

            self.add_new_nodes_edges(new, new_edges, update=update)

            self.patch_updates()

            if len(self.scheduler.tasks) == 0:
                self.subtitle.text = "Scheduler is empty."
            else:
                self.subtitle.text = " "

    @without_property_validation
    def add_new_nodes_edges(self, new, new_edges, update=False):
        if new or update:
            node_key = []
            node_x = []
            node_y = []
            node_state = []
            node_name = []
            edge_x = []
            edge_y = []

            x = self.layout.x
            y = self.layout.y

            tasks = self.scheduler.tasks
            for key in new:
                try:
                    task = tasks[key]
                except KeyError:
                    continue
                xx = x[key]
                yy = y[key]
                node_key.append(url_escape(str(key)))
                node_x.append(xx)
                node_y.append(yy)
                node_state.append(task.state)
                node_name.append(task.prefix.name)

            for a, b in new_edges:
                try:
                    edge_x.append([x[a], x[b]])
                    edge_y.append([y[a], y[b]])
                except KeyError:
                    pass

            node = {
                "x": node_x,
                "y": node_y,
                "state": node_state,
                "name": node_name,
                "key": node_key,
                "visible": ["True"] * len(node_x),
            }
            edge = {"x": edge_x, "y": edge_y, "visible": ["True"] * len(edge_x)}

            if update or not len(self.node_source.data["x"]):
                # see https://github.com/bokeh/bokeh/issues/7523
                self.node_source.data.update(node)
                self.edge_source.data.update(edge)
            else:
                self.node_source.stream(node)
                self.edge_source.stream(edge)

    @without_property_validation
    def patch_updates(self):
        """
        Small updates like color changes or lost nodes from task transitions
        """
        n = len(self.node_source.data["x"])
        m = len(self.edge_source.data["x"])

        if self.layout.state_updates:
            state_updates = self.layout.state_updates
            self.layout.state_updates = []
            updates = [(i, c) for i, c in state_updates if i < n]
            self.node_source.patch({"state": updates})

        if self.layout.visible_updates:
            updates = self.layout.visible_updates
            updates = [(i, c) for i, c in updates if i < n]
            self.layout.visible_updates = []
            self.node_source.patch({"visible": updates})
            self.invisible_count += len(updates)

        if self.layout.visible_edge_updates:
            updates = self.layout.visible_edge_updates
            updates = [(i, c) for i, c in updates if i < m]
            self.layout.visible_edge_updates = []
            self.edge_source.patch({"visible": updates})

    def __del__(self):
        self.scheduler.remove_plugin(name=self.layout.name)


class TaskGroupGraph(DashboardComponent):
    """
    Task Group Graph

    Creates a graph layout for TaskGroups on the scheduler.  It assigns
    (x, y) locations to all the TaskGroups and lays them out by according
    to their dependencies. The layout gets updated every time that new
    TaskGroups are added.

    Each task group node incodes information about task progress, memory,
    and output type into glyphs, as well as a hover tooltip with more detailed
    information on name, computation time, memory, and tasks status.
    """

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler

        self.nodes_layout = {}
        self.arrows_layout = {}

        self.old_counter = -1

        self.nodes_source = ColumnDataSource(
            {
                "x": [],
                "y": [],
                "w_box": [],
                "h_box": [],
                "name": [],
                "tot_tasks": [],
                "color": [],
                "x_start": [],
                "x_end": [],
                "y_start": [],
                "y_end": [],
                "x_end_progress": [],
                "mem_alpha": [],
                "node_line_width": [],
                "comp_tasks": [],
                "url_logo": [],
                "x_logo": [],
                "y_logo": [],
                "w_logo": [],
                "h_logo": [],
                "in_processing": [],
                "in_memory": [],
                "in_released": [],
                "in_erred": [],
                "compute_time": [],
                "memory": [],
            }
        )

        self.arrows_source = ColumnDataSource({"xs": [], "ys": [], "xe": [], "ye": []})

        self.root = figure(title="Task Groups Graph", match_aspect=True, **kwargs)
        self.root.axis.visible = False
        self.subtitle = Title(text=" ", text_font_style="italic")
        self.root.add_layout(self.subtitle, "above")

        rect = self.root.rect(
            x="x",
            y="y",
            width="w_box",
            height="h_box",
            color="color",
            fill_alpha="mem_alpha",
            line_color="black",
            line_width="node_line_width",
            source=self.nodes_source,
        )

        # plot tg log
        self.root.image_url(
            url="url_logo",
            x="x_logo",
            y="y_logo",
            w="w_logo",
            h="h_logo",
            anchor="center",
            source=self.nodes_source,
        )

        # progress bar plain box
        self.root.quad(
            left="x_start",
            right="x_end",
            bottom="y_start",
            top="y_end",
            color=None,
            line_color="black",
            source=self.nodes_source,
        )

        # progress bar
        self.root.quad(
            left="x_start",
            right="x_end_progress",
            bottom="y_start",
            top="y_end",
            color="color",
            line_color=None,
            fill_alpha=0.6,
            source=self.nodes_source,
        )

        self.arrows = Arrow(
            end=VeeHead(size=8),
            line_color="black",
            line_alpha=0.5,
            line_width=1,
            x_start="xs",
            y_start="ys",
            x_end="xe",
            y_end="ye",
            source=self.arrows_source,
        )
        self.root.add_layout(self.arrows)

        self.root.xgrid.grid_line_color = None
        self.root.ygrid.grid_line_color = None
        self.root.x_range.range_padding = 0.5
        self.root.y_range.range_padding = 0.5

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 12px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 12px; font-weight: bold;">Compute time:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@compute_time</span>
                </div>
                <div>
                    <span style="font-size: 12px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 12px; font-weight: bold;">Tasks:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@tot_tasks</span>
                </div>
                <div style="margin-left: 2em;">
                    <span style="font-size: 12px; font-weight: bold;">Completed:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@comp_tasks</span>
                </div>
                <div style="margin-left: 2em;">
                    <span style="font-size: 12px; font-weight: bold;">Processing:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@in_processing</span>
                </div>
                <div style="margin-left: 2em;">
                    <span style="font-size: 12px; font-weight: bold;">In memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@in_memory</span>
                </div>
                <div style="margin-left: 2em;">
                    <span style="font-size: 12px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@in_erred</span>
                </div>
                <div style="margin-left: 2em;">
                    <span style="font-size: 12px; font-weight: bold;">Released:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@in_released</span>
                </div>
                """,
            renderers=[rect],
        )

        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update_layout(self):
        # Get dependencies per task group.
        # In some cases there are tg that have themselves as dependencies - we remove those.
        dependencies = {
            k: {ds.name for ds in ts.dependencies if ds.name != k}
            for k, ts in self.scheduler.task_groups.items()
        }

        import dask

        order = dask.order.order(
            dsk={group.name: 1 for k, group in self.scheduler.task_groups.items()},
            dependencies=dependencies,
        )

        ordered = sorted(self.scheduler.task_groups, key=order.get)

        xs = {}
        ys = {}
        locations = set()
        nodes_layout = {}
        arrows_layout = {}
        for tg in ordered:
            if dependencies[tg]:
                x = max(xs[dep] for dep in dependencies[tg]) + 1
                y = max(ys[dep] for dep in dependencies[tg])
                if (
                    len(dependencies[tg]) > 1
                    and len({ys[dep] for dep in dependencies[tg]}) == 1
                ):
                    y += 1
            else:
                x = 0
                y = max(ys.values()) + 1 if ys else 0

            while (x, y) in locations:  # avoid collisions by moving up
                y += 1

            locations.add((x, y))

            xs[tg], ys[tg] = x, y

            # info needed for node layout to column data source
            nodes_layout[tg] = {"x": xs[tg], "y": ys[tg]}

            # info needed for arrow layout
            arrows_layout[tg] = {
                "nstart": dependencies[tg],
                "nend": [tg] * len(dependencies[tg]),
            }

        return nodes_layout, arrows_layout

    def compute_size(self, x, min_box, max_box):
        start = 0.4
        end = 0.8

        y = (end - start) / (max_box - min_box) * (x - min_box) + start

        return y

    @without_property_validation
    def update(self):
        if self.scheduler.transition_counter == self.old_counter:
            return
        self.old_counter = self.scheduler.transition_counter

        if not self.scheduler.task_groups:
            self.subtitle.text = "Scheduler is empty."
        else:
            self.subtitle.text = " "

        if self.nodes_layout.keys() != self.scheduler.task_groups.keys():
            self.nodes_layout, self.arrows_layout = self.update_layout()

        nodes_data = {
            "x": [],
            "y": [],
            "w_box": [],
            "h_box": [],
            "name": [],
            "color": [],
            "tot_tasks": [],
            "x_start": [],
            "x_end": [],
            "y_start": [],
            "y_end": [],
            "x_end_progress": [],
            "mem_alpha": [],
            "node_line_width": [],
            "comp_tasks": [],
            "url_logo": [],
            "x_logo": [],
            "y_logo": [],
            "w_logo": [],
            "h_logo": [],
            "in_processing": [],
            "in_memory": [],
            "in_released": [],
            "in_erred": [],
            "compute_time": [],
            "memory": [],
        }

        arrows_data = {
            "xs": [],
            "ys": [],
            "xe": [],
            "ye": [],
        }

        durations = set()
        nbytes = set()
        for tg in self.scheduler.task_groups.values():
            if tg.duration and tg.nbytes_total:
                durations.add(tg.duration)
                nbytes.add(tg.nbytes_total)

        durations_min = min(durations, default=0)
        durations_max = max(durations, default=0)
        nbytes_min = min(nbytes, default=0)
        nbytes_max = max(nbytes, default=0)

        box_dim = {}
        for key, tg in self.scheduler.task_groups.items():
            comp_tasks = (
                tg.states["released"] + tg.states["memory"] + tg.states["erred"]
            )
            tot_tasks = sum(tg.states.values())

            # compute width and height of boxes
            if (
                tg.duration
                and tg.nbytes_total
                and comp_tasks
                and len(durations) > 1
                and len(nbytes) > 1
            ):
                # scale duration (width)
                width_box = self.compute_size(
                    tg.duration / comp_tasks * tot_tasks,
                    min_box=durations_min / comp_tasks * tot_tasks,
                    max_box=durations_max / comp_tasks * tot_tasks,
                )

                # need to scale memory (height)
                height_box = self.compute_size(
                    tg.nbytes_total / comp_tasks * tot_tasks,
                    min_box=nbytes_min / comp_tasks * tot_tasks,
                    max_box=nbytes_max / comp_tasks * tot_tasks,
                )

            else:
                width_box = 0.6
                height_box = width_box / 2

            box_dim[key] = {"width": width_box, "height": height_box}

        for key, tg in self.scheduler.task_groups.items():
            x = self.nodes_layout[key]["x"]
            y = self.nodes_layout[key]["y"]
            width = box_dim[key]["width"]
            height = box_dim[key]["height"]

            # main boxes layout
            nodes_data["x"].append(x)
            nodes_data["y"].append(y)
            nodes_data["w_box"].append(width)
            nodes_data["h_box"].append(height)

            comp_tasks = (
                tg.states["released"] + tg.states["memory"] + tg.states["erred"]
            )
            tot_tasks = sum(tg.states.values())

            nodes_data["name"].append(tg.prefix.name)

            nodes_data["color"].append(color_of(tg.prefix.name))
            nodes_data["tot_tasks"].append(tot_tasks)

            # memory alpha factor by 0.4 if not get's too dark
            nodes_data["mem_alpha"].append(
                (tg.states["memory"] / sum(tg.states.values())) * 0.4
            )

            # main box line width
            if tg.states["processing"]:
                nodes_data["node_line_width"].append(5)
            else:
                nodes_data["node_line_width"].append(1)

            # progress bar data update
            nodes_data["x_start"].append(x - width / 2)
            nodes_data["x_end"].append(x + width / 2)

            nodes_data["y_start"].append(y - height / 2)
            nodes_data["y_end"].append(y - height / 2 + height * 0.4)

            nodes_data["x_end_progress"].append(
                x - width / 2 + width * comp_tasks / tot_tasks
            )

            # arrows
            arrows_data["xs"] += [
                self.nodes_layout[k]["x"] + box_dim[k]["width"] / 2
                for k in self.arrows_layout[key]["nstart"]
            ]
            arrows_data["ys"] += [
                self.nodes_layout[k]["y"] for k in self.arrows_layout[key]["nstart"]
            ]
            arrows_data["xe"] += [
                self.nodes_layout[k]["x"] - box_dim[k]["width"] / 2
                for k in self.arrows_layout[key]["nend"]
            ]
            arrows_data["ye"] += [
                self.nodes_layout[k]["y"] for k in self.arrows_layout[key]["nend"]
            ]

            # LOGOS
            if len(tg.types) == 1:
                logo_type = next(iter(tg.types)).split(".")[0]
                try:
                    url_logo = logos_dict[logo_type]
                except KeyError:
                    url_logo = ""
            else:
                url_logo = ""

            nodes_data["url_logo"].append(url_logo)

            nodes_data["x_logo"].append(x + width / 3)
            nodes_data["y_logo"].append(y + height / 3)

            ratio = width / height

            if ratio > 1:
                nodes_data["h_logo"].append(height * 0.3)
                nodes_data["w_logo"].append(width * 0.3 / ratio)
            else:
                nodes_data["h_logo"].append(height * 0.3 * ratio)
                nodes_data["w_logo"].append(width * 0.3)

            # compute_time and memory
            nodes_data["compute_time"].append(format_time(tg.duration))
            nodes_data["memory"].append(format_bytes(tg.nbytes_total))

            # Add some status to hover
            tasks_processing = tg.states["processing"]
            tasks_memory = tg.states["memory"]
            tasks_relased = tg.states["released"]
            tasks_erred = tg.states["erred"]

            nodes_data["comp_tasks"].append(
                f"{comp_tasks} ({comp_tasks / tot_tasks * 100:.0f} %)"
            )
            nodes_data["in_processing"].append(
                f"{tasks_processing} ({tasks_processing / tot_tasks * 100:.0f} %)"
            )
            nodes_data["in_memory"].append(
                f"{tasks_memory} ({tasks_memory / tot_tasks * 100:.0f} %)"
            )
            nodes_data["in_released"].append(
                f"{tasks_relased} ({tasks_relased / tot_tasks * 100:.0f} %)"
            )
            nodes_data["in_erred"].append(
                f"{tasks_erred} ({tasks_erred / tot_tasks * 100:.0f} %)"
            )

        self.nodes_source.data.update(nodes_data)
        self.arrows_source.data.update(arrows_data)


class TaskGroupProgress(DashboardComponent):
    """Stacked area chart showing task groups through time"""

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource()
        # The length of timeseries to chart (in units of plugin.dt)
        self.npts = 180

        if GroupTiming.name not in scheduler.plugins:
            scheduler.add_plugin(plugin=GroupTiming(scheduler))

        self.plugin = scheduler.plugins[GroupTiming.name]

        self.source.add(np.array(self.plugin.time) * 1000.0, "time")

        x_range = DataRange1d(range_padding=0)
        y_range = Range1d(0, max(self.plugin.nthreads))

        self.root = figure(
            title="Task Group Progress",
            name="task_group_progress",
            toolbar_location="above",
            min_border_bottom=50,
            x_range=x_range,
            y_range=y_range,
            tools="",
            x_axis_type="datetime",
            y_axis_location=None,
            **kwargs,
        )
        self.root.yaxis.major_label_text_alpha = 0
        self.root.yaxis.minor_tick_line_alpha = 0
        self.root.yaxis.major_tick_line_alpha = 0
        self.root.xgrid.visible = False

        self.root.add_tools(
            BoxZoomTool(),
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
        )
        self._hover = None
        self._last_drawn = None
        self._offset = time()
        self._last_transition_count = scheduler.transition_counter
        # OrderedDict so we can make a reverse iterator later and get the
        # most-recently-added glyphs.
        self._renderers = OrderedDict()
        self._line_renderers = OrderedDict()

    def _should_add_new_renderers(self) -> bool:
        """
        Whether to add new renderers to the chart.

        When a new set of task groups enters the scheduler we'd like to start rendering
        them. But it can be expensive to add new glyps, so we do it deliberately,
        checking whether we have to do it and whether the scheduler seems busy.
        """
        # Always draw if we have not before
        if not self._last_drawn:
            return True
        # Don't draw if there have been no new tasks completed since the last update,
        # or if the scheduler CPU is occupied.
        if (
            self._last_transition_count == self.scheduler.transition_counter
            or self.scheduler.proc.cpu_percent() > 50
        ):
            return False

        # Only return true if there are new task groups that we have not yet added
        # to the ColumnDataSource.
        return not set(self.plugin.compute.keys()) <= set(self.source.data.keys())

    def _should_update(self) -> bool:
        """
        Whether to update the ColumnDataSource. This is cheaper than redrawing,
        but still not free, so we check whether we need it and whether the scheudler
        is busy.
        """
        return (
            self._last_transition_count != self.scheduler.transition_counter
            and self.scheduler.proc.cpu_percent() < 50
        )

    def _get_timeseries(self, restrict_to_existing=False):
        """
        Update the ColumnDataSource with our time series data.

        restrict_to_existing determines whether to add new task groups
        which might have been added since the last time we rendered.
        This is important as we want to add new stackers very deliberately.
        """
        # Get the front/back indices for most recent npts bins out of the timeseries
        front = max(len(self.plugin.time) - self.npts, 0)
        back = None
        # Remove any periods of zero compute at the front or back of the timeseries
        if len(self.plugin.compute):
            agg = sum(np.array(v[front:]) for v in self.plugin.compute.values())
            front2 = len(agg) - len(np.trim_zeros(agg, trim="f"))
            front += front2
            back = len(np.trim_zeros(agg, trim="b")) - len(agg) or None

        prepend = (
            self.plugin.time[front - 1]
            if front >= 1
            else self.plugin.time[front] - self.plugin.dt
        )
        timestamps = np.array(self.plugin.time[front:back])
        dt = np.diff(timestamps, prepend=prepend)

        if restrict_to_existing:
            new_data = {
                k: np.array(v[front:back]) / dt
                for k, v in self.plugin.compute.items()
                if k in self.source.data
            }
        else:
            new_data = valmap(
                lambda x: np.array(x[front:back]) / dt,
                self.plugin.compute,
            )

        new_data["time"] = (
            timestamps - self._offset
        ) * 1000.0  # bokeh likes milliseconds
        new_data["nthreads"] = np.array(self.plugin.nthreads[front:back])

        return new_data

    @without_property_validation
    @log_errors
    def update(self):
        """
        Maybe update the chart. This is somewhat expensive to draw, so we update
        it pretty defensively.
        """
        if self._should_add_new_renderers():
            # Update the chart, allowing for new task groups to be added.
            new_data = self._get_timeseries(restrict_to_existing=False)
            self.source.data = new_data

            # Possibly update the y range if the number of threads has increased.
            max_nthreads = max(self.plugin.nthreads)
            if self.root.y_range.end != max_nthreads:
                self.root.y_range.end = max_nthreads

            stackers = list(self.plugin.compute.keys())
            colors = [color_of(key_split(k)) for k in stackers]

            for i, (group, color) in enumerate(zip(stackers, colors)):
                # If we have already drawn the group, but it is all zero,
                # set it to be invisible.
                if group in self._renderers:
                    if not np.count_nonzero(new_data[group]) > 0:
                        self._renderers[group].visible = False
                        self._line_renderers[group].visible = False
                    else:
                        self._renderers[group].visible = True
                        self._line_renderers[group].visible = True

                    continue

                # Draw the new area and line glyphs.
                renderer = self.root.varea(
                    x="time",
                    y1=stack(*stackers[:i]),
                    y2=stack(*stackers[: i + 1]),
                    color=color,
                    alpha=0.5,
                    source=self.source,
                )
                self._renderers[group] = renderer

                line_renderer = self.root.line(
                    x="time",
                    y=stack(*stackers[: i + 1]),
                    color=color,
                    alpha=1.0,
                    source=self.source,
                )
                self._line_renderers[group] = line_renderer

            # Don't add hover until there is something to show, as bokehjs seems to
            # have trouble with custom hovers when there are no renderers.
            if self.plugin.compute and self._hover is None:
                # Add a hover that will show occupancy for all currently active
                # task groups. This is a little tricky, bokeh doesn't (yet) support
                # hit tests for stacked area charts: https://github.com/bokeh/bokeh/issues/9182
                # Instead, show a single vline hover which lists the currently active task
                # groups. A custom formatter in JS-land pulls the relevant data index and
                # assembles the tooltip.
                formatter = CustomJSHover(code="return '';")
                self._hover = HoverTool(
                    tooltips="""
                        <div>
                          <div style="font-size: 1.2em; font-weight: bold">
                            <b>Worker thread occupancy</b>
                          </div>
                          <div>
                            $index{custom}
                          </div>
                        </div>
                        """,
                    mode="vline",
                    line_policy="nearest",
                    attachment="horizontal",
                    formatters={"$index": formatter},
                )
                self.root.add_tools(self._hover)

            if self._hover:
                # Create a custom tooltip that:
                #   1. Includes nthreads
                #   2. Filters out inactive task groups
                #      (ones without any compute during the relevant dt)
                #   3. Colors the labels appropriately.
                formatter = CustomJSHover(
                    code="""
                        const colormap = %s;
                        const divs = [];
                        for (let k of Object.keys(source.data)) {
                          const val = source.data[k][value];
                          const color = colormap[k];
                          if (k === "time" || k === "nthreads" || val < 1.e-3) {
                            continue;
                          }
                          const label = k.length >= 20 ? k.slice(0, 20) + '…' : k;

                          // Unshift so that the ordering of the labels is the same as
                          // the ordering of the stackers.
                          divs.unshift(
                            '<div>'
                              + '<span style="font-weight: bold; color:' + color + ';">'
                                + label
                              + '</span>'
                              + ': '
                              +  val.toFixed(1)
                              + '</div>'
                          )

                        }
                        divs.unshift(
                          '<div>'
                            + '<span style="font-weight: bold; color: darkgrey;">nthreads: </span>'
                            + source.data.nthreads[value]
                            + '</div>'
                        );
                        return divs.join('\\n')
                        """
                    % dict(
                        zip(stackers, colors)
                    ),  # sneak the color mapping into the callback
                    args={"source": self.source},
                )
                # Add the HoverTool to the top line renderer.
                top_line = None
                for line in reversed(self._line_renderers.values()):
                    if line.visible:
                        top_line = line
                        break
                self._hover.renderers = [top_line]
                self._hover.formatters = {"$index": formatter}

            self._last_drawn = time()
            self._last_transition_count = self.scheduler.transition_counter
        elif self._should_update():
            # Possibly update the y range if new threads have been added
            max_nthreads = max(self.plugin.nthreads)
            if self.root.y_range.end != max_nthreads:
                self.root.y_range.end = max_nthreads
            # Update the data, only including existing columns, rather than redrawing
            # the whole chart.
            self.source.data = self._get_timeseries(restrict_to_existing=True)
            self._last_transition_count = self.scheduler.transition_counter


class TaskProgress(DashboardComponent):
    """Progress bars per task type"""

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler

        data = progress_quads(
            dict(all={}, memory={}, erred={}, released={}, processing={}, queued={})
        )
        self.source = ColumnDataSource(data=data)

        x_range = DataRange1d(range_padding=0)
        y_range = Range1d(-8, 0)

        self.root = figure(
            title="Progress",
            name="task_progress",
            x_range=x_range,
            y_range=y_range,
            toolbar_location="above",
            tools="",
            min_border_bottom=50,
            **kwargs,
        )
        self.root.line(  # just to define early ranges
            x=[0, 0.9], y=[-1, 0], line_color="#FFFFFF", alpha=0.0
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="left",
            right="right",
            fill_color="#aaaaaa",
            line_color="#aaaaaa",
            fill_alpha=0.1,
            line_alpha=0.3,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="left",
            right="released-loc",
            fill_color="color",
            line_color="color",
            fill_alpha=0.6,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="released-loc",
            right="memory-loc",
            fill_color="color",
            line_color="color",
            fill_alpha=1.0,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="memory-loc",
            right="erred-loc",
            fill_color="black",
            fill_alpha=0.5,
            line_alpha=0,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="erred-loc",
            right="processing-loc",
            fill_color="gray",
            fill_alpha=0.35,
            line_alpha=0,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="processing-loc",
            right="queued-loc",
            fill_color="gray",
            hatch_pattern="/",
            hatch_color="white",
            fill_alpha=0.35,
            line_alpha=0,
        )
        self.root.quad(
            source=self.source,
            top="top",
            bottom="bottom",
            left="queued-loc",
            right="no-worker-loc",
            fill_color="red",
            hatch_pattern="/",
            hatch_color="black",
            fill_alpha=0.35,
            line_alpha=0,
        )
        self.root.text(
            source=self.source,
            text="show-name",
            y="bottom",
            x="left",
            x_offset=5,
            text_font_size=value("10pt"),
        )
        self.root.text(
            source=self.source,
            text="done",
            y="bottom",
            x="right",
            x_offset=-5,
            text_align="right",
            text_font_size=value("10pt"),
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
                    <span style="font-size: 14px; font-weight: bold;">Queued:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@queued</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">No-worker:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@no_worker</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Processing:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@processing</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
                </div>
                """,
        )
        help_ = HelpTool(
            redirect="https://docs.dask.org/en/stable/dashboard.html#progress",
            description="A description of the progress bars plot.",
        )
        self.root.add_tools(hover, help_)

    @without_property_validation
    @log_errors
    def update(self):
        state = {
            "memory": {},
            "erred": {},
            "released": {},
            "processing": {},
            "waiting": {},
            "queued": {},
            "no_worker": {},
        }

        for tp in self.scheduler.task_prefixes.values():
            states = tp.states
            if any(states.get(s) for s in state.keys()):
                state["memory"][tp.name] = states["memory"]
                state["erred"][tp.name] = states["erred"]
                state["released"][tp.name] = states["released"]
                state["processing"][tp.name] = states["processing"]
                state["waiting"][tp.name] = states["waiting"]
                state["queued"][tp.name] = states["queued"]
                state["no_worker"][tp.name] = states["no-worker"]

        state["all"] = {k: sum(v[k] for v in state.values()) for k in state["memory"]}

        if not state["all"] and not len(self.source.data["all"]):
            return

        d = progress_quads(state)

        update(self.source, d)

        totals = {
            k: sum(state[k].values())
            for k in [
                "all",
                "memory",
                "erred",
                "released",
                "waiting",
                "queued",
                "no_worker",
            ]
        }
        totals["processing"] = totals["all"] - sum(
            v for k, v in totals.items() if k != "all"
        )

        self.root.title.text = (
            "Progress -- total: %(all)s, "
            "waiting: %(waiting)s, "
            "queued: %(queued)s, "
            "processing: %(processing)s, "
            "in-memory: %(memory)s, "
            "no-worker: %(no_worker)s, "
            "erred: %(erred)s" % totals
        )


class FinePerformanceMetrics(DashboardComponent):
    """
    The main overview of the Fine Performance Metrics page.
    """

    BASE_TOOLS = ["pan", "wheel_zoom", "box_zoom", "reset"]
    scheduler: Scheduler
    task_exec_by_prefix_src: ColumnDataSource
    task_exec_by_prefix_xmax: float
    task_exec_by_activity_src: ColumnDataSource
    get_data_by_activity_src: ColumnDataSource
    substantial_change: bool
    visible_functions: list[str]
    visible_activities: list[str]
    stacked_chart_visible_activities: list[str]
    function_selector: MultiChoice
    span_tag_selector: MultiChoice
    unit_selector: Select
    task_exec_by_activity_chart: figure | None
    task_exec_by_prefix_chart: figure | None
    get_data_by_activity_chart: figure | None

    @log_errors
    def __init__(self, scheduler: Scheduler, **kwargs: Any):
        self.scheduler = scheduler
        self.task_exec_by_prefix_src = ColumnDataSource(data={})
        self.task_exec_by_prefix_xmax = 0.0
        self.task_exec_by_activity_src = ColumnDataSource(data={})
        self.get_data_by_activity_src = ColumnDataSource(data={})
        self.substantial_change = False
        self.visible_functions = []
        self.visible_activities = []
        self.stacked_chart_visible_activities = []
        self.task_exec_by_activity_chart = None
        self.task_exec_by_prefix_chart = None
        self.get_data_by_activity_chart = None

        # Selectors
        self.function_selector = MultiChoice(
            title="Filter by function",
            placeholder="Select specific functions",
            value=[],
            options=[],
        )
        self.span_tag_selector = MultiChoice(
            title="Filter by span tag",
            placeholder="Select specific span tags",
            value=[],
            options=[],
        )
        self.unit_selector = Select(title="Unit selection", options=["seconds"])
        self.unit_selector.value = "seconds"
        self.unit_selector.on_change("value", self._handle_change_unit)

        selectors_row = row(
            children=[
                self.span_tag_selector,
                self.function_selector,
                self.unit_selector,
            ],
            sizing_mode="stretch_width",
        )
        self.root = column(
            selectors_row,
            sizing_mode="scale_width",
        )

    def _handle_change_unit(self, attr: str, old: str, new: str) -> None:
        self.substantial_change = True
        assert self.task_exec_by_prefix_chart
        fmt = self._bokeh_unit_format()
        self.task_exec_by_prefix_chart.xaxis[0].formatter.format = fmt

    @without_property_validation
    @log_errors
    def update(self):
        self._update_selectors()
        self._build_data_sources()

        needs_figures_row = self.task_exec_by_activity_chart is None
        if needs_figures_row:
            self.substantial_change = True

        if needs_figures_row:
            # First call to update()
            self.task_exec_by_prefix_chart = (
                self._build_task_execution_by_prefix_chart()
            )
            self.task_exec_by_activity_chart = self._build_pie_chart(
                source=self.task_exec_by_activity_src,
                title="Task execution, by activity",
            )
            self.get_data_by_activity_chart = self._build_pie_chart(
                source=self.get_data_by_activity_src,
                # get_data is called via RPC for data pull; the metrics for it are
                # recorded on the worker *donating* the data
                title="Send data, by activity",
            )

        self.task_exec_by_prefix_chart.x_range.end = self.task_exec_by_prefix_xmax * 1.1

        if self.substantial_change:
            # Visible activities and/or functions changed
            self.substantial_change = False
            self._update_task_execution_by_prefix_chart()
            figures_row = row(
                children=[
                    self.task_exec_by_prefix_chart,
                    self.task_exec_by_activity_chart,
                    self.get_data_by_activity_chart,
                ],
                sizing_mode="stretch_width",
            )

            if needs_figures_row:
                # First iteration, initial assignment of figures row
                self.root.children.append(figures_row)
            else:
                # Otherwise needs forced refresh by replacing the figures row
                self.root.children[-1] = figures_row

    def _update_selectors(self) -> None:
        """Update choices available in

        - self.unit_selector
        - self.function_selector
        - self.span_tag_selector
        """
        units = set()
        functions = set()

        for k in self.scheduler.cumulative_worker_metrics:
            if not isinstance(k, tuple):
                continue
            context, *other, activity, unit = k

            assert isinstance(unit, str)
            units.add(unit)

            if context == "execute":
                (function,) = other
                assert isinstance(function, str)
                functions.add(function)

        units.difference_update(self.unit_selector.options)
        if units:
            self.unit_selector.options.extend(units)

        if functions:
            # Added on the fly by Span.cumulative_worker_metrics
            functions.add("N/A")
        functions.difference_update(self.function_selector.options)
        if functions:
            self.function_selector.options.extend(functions)
            self.function_selector.title = (
                f"Filter by function ({len(self.function_selector.options)}):"
            )

        spans_ext: SpansSchedulerExtension | None = self.scheduler.extensions.get(
            "spans"
        )
        if spans_ext:
            tags = set(spans_ext.spans_search_by_tag)
            tags.difference_update(self.span_tag_selector.options)
            if tags:
                self.span_tag_selector.options.extend(tags)
                self.span_tag_selector.title = (
                    f"Filter by span tag ({len(self.span_tag_selector.options)}):"
                )

    def _bokeh_unit_format(self) -> str:
        unit = self.unit_selector.value
        if unit == "seconds":
            return "00:00:00"
        elif unit == "bytes":
            return "0.00b"
        elif unit == "count":
            return "0"
        else:
            return "0.000"

    def _format(self, val: float) -> str:
        unit = self.unit_selector.value
        if unit == "seconds":
            return format_time(val)
        elif unit == "bytes":
            return format_bytes(int(val))
        # count or custom user-defined metric
        elif (ival := int(val)) == val:
            return str(ival)
        else:
            return str(val)

    def _get_palette(self) -> list[str]:
        n = len(self.visible_activities)
        try:
            from bokeh.palettes import interp_palette

            return list(interp_palette(Viridis11, n))
        except ImportError:
            # Bokeh 2.4
            return [Viridis11[i % len(Viridis11)] for i in range(n)]

    def _build_data_sources(self) -> None:
        """Pre-process and filter fine performance metrics; build data tables in
        Bokeh format

        Updates:

        - self.substantial_change
        - self.visible_activities
        - self.visible_functions
        - self.task_exec_by_prefix_src.data
        - self.task_exec_by_activity_src.data
        - self.get_data_by_activity_src.data
        """
        visible_functions = set()
        visible_activities = set()
        execute_by_func: defaultdict[tuple[str, str], float] = defaultdict(float)
        execute: defaultdict[str, float] = defaultdict(float)
        get_data: defaultdict[str, float] = defaultdict(float)

        function_sel = set(self.function_selector.value)

        spans_ext: SpansSchedulerExtension | None = self.scheduler.extensions.get(
            "spans"
        )
        if spans_ext and self.span_tag_selector.value:
            span = spans_ext.merge_by_tags(*self.span_tag_selector.value)
            metrics = span.cumulative_worker_metrics
        elif spans_ext and spans_ext.spans:
            # Calculate idle time
            span = spans_ext.merge_all()
            metrics = span.cumulative_worker_metrics
        else:
            # Spans extension is not loaded
            metrics = {
                k: v
                for k, v in self.scheduler.cumulative_worker_metrics.items()
                if isinstance(k, tuple)
                and len(k) == 4  # Skip get-data, gather-deps, and memory-monitor
            }

        for (context, function, activity, unit), v in metrics.items():
            if context != "execute":
                continue  # TODO visualize 'shuffle' metrics
            assert isinstance(function, str)
            assert isinstance(unit, str)
            assert self.unit_selector.value
            if unit != self.unit_selector.value:
                continue
            if function_sel and function not in function_sel:
                continue

            # Custom metrics won't necessarily contain a string as the label
            activity = str(activity)

            # TODO We could implement some fancy logic in spans.py to change the label
            #      if no other spans are running at the same time.
            if not self.span_tag_selector.value and activity == "idle or other spans":
                activity = "idle"

            execute_by_func[function, activity] += v
            execute[activity] += v
            visible_functions.add(function)
            visible_activities.add(activity)

        if not self.function_selector.value and not self.span_tag_selector.value:
            for k, v in self.scheduler.cumulative_worker_metrics.items():
                if isinstance(k, tuple) and k[0] == "get-data":
                    _, activity, unit = k
                    assert isinstance(activity, str)
                    assert isinstance(unit, str)
                    assert self.unit_selector.value
                    if unit == self.unit_selector.value:
                        visible_activities.add(activity)
                        get_data[activity] += v

            # Ignore memory-monitor and gather-dep metrics

        if visible_functions != set(self.visible_functions):
            self.substantial_change = True
            self.visible_functions = sorted(visible_functions)

        if visible_activities != set(self.visible_activities):
            self.visible_activities = sorted(visible_activities)

        (
            self.task_exec_by_prefix_src.data,
            self.task_exec_by_prefix_xmax,
        ) = self._build_task_execution_by_prefix_data(execute_by_func)
        self.task_exec_by_activity_src.data = self._build_pie_data(execute)
        self.get_data_by_activity_src.data = self._build_pie_data(get_data)

    def _build_pie_data(self, data: defaultdict[str, float]) -> dict[str, list]:
        """Build the data source for a pie chart by activity

        See also
        --------
        _build_pie_chart
        """
        activities = self.visible_activities
        # Generate palette based on all visible activities to make sure that
        # colors match between the plots
        palette = self._get_palette()
        values = [data[activity] for activity in activities]

        # Sort by values from largest to smallest
        # Hide activities that are missing in the current plot from the legend
        idx = [i for i, v in sorted(enumerate(values), key=lambda el: -el[1]) if v]
        activities = [activities[i] for i in idx]
        palette = [palette[i] for i in idx]
        values = [values[i] for i in idx]

        total_value = sum(values)
        total_text = self._format(total_value)
        percent_k = 100.0 / total_value if total_value else 0.0
        angle_k = 2.0 * math.pi / total_value if total_value else 0.0
        return {
            "activity": activities,
            "value": values,
            "text": [self._format(v) + f" ({v * percent_k:.0f}%)" for v in values],
            "angle": [v * angle_k for v in values],
            "color": palette,
            "total_text": [total_text] * len(values),
        }

    def _build_pie_chart(self, source: ColumnDataSource, title: str) -> figure:
        """Create pie chart by activity

        See also
        --------
        _build_pie_data
        """
        piechart = figure(
            height=500,
            sizing_mode="scale_both",
            title=title,
            tools="hover",
            tooltips="@{activity}: @text<br>total: @{total_text}",
            x_range=(-0.5, 1.0),
        )
        piechart.axis.axis_label = None
        piechart.axis.visible = False
        piechart.grid.grid_line_color = None

        piechart.wedge(
            x=0,
            y=1,
            radius=0.4,
            start_angle=cumsum("angle", include_zero=True),
            end_angle=cumsum("angle"),
            line_color="white",
            fill_color="color",
            legend_field="activity",
            source=source,
        )
        return piechart

    def _build_task_execution_by_prefix_data(
        self, data: defaultdict[tuple[str, str], float]
    ) -> tuple[dict[str, list], float]:
        """Build the data source for the execute by function stacked chart

        See also
        --------
        _build_task_execution_by_prefix_chart
        _update_task_execution_by_prefix_chart
        """
        func_totals = [
            sum(data[function, activity] for activity in self.visible_activities)
            for function in self.visible_functions
        ]
        perc_k = [100.0 / v if v else 0.0 for v in func_totals]
        out: dict[str, list] = {
            "__functions": self.visible_functions,
            "____total_text": [self._format(v) for v in func_totals],
        }

        sc_visible_activities = []
        for activity in self.visible_activities:
            values = [data[function, activity] for function in self.visible_functions]
            if not any(values):
                continue
            sc_visible_activities.append(activity)
            out[activity] = values
            out[f"__{activity}_text"] = [
                self._format(v) + f" ({v * perc_ki:.0f}%)"
                for v, perc_ki in zip(values, perc_k)
            ]
        if sc_visible_activities != self.stacked_chart_visible_activities:
            self.substantial_change = True
            self.stacked_chart_visible_activities[:] = sc_visible_activities

        return out, max(func_totals, default=0.0)

    def _build_task_execution_by_prefix_chart(self) -> figure:
        """Create empty stacked bar chart for execute by function

        See also
        --------
        _build_task_execution_by_prefix_data
        _update_task_execution_by_prefix_chart
        """
        barchart = figure(
            y_range=[],
            height=500,
            sizing_mode="scale_both",
            title="Task execution, by function",
            tools=",".join(self.BASE_TOOLS),
        )
        barchart.yaxis.visible = True
        # As of Bokeh 3.1, DataRange1D (the default) does not work when switching back
        # from bytes (GiBs) to seconds (hundreds). So we need to manually update it.
        barchart.x_range = Range1d(0, 1)
        barchart.xaxis[0].formatter = NumeralTickFormatter(format="00:00:00")
        barchart.xaxis.major_label_orientation = 0.4
        barchart.grid.grid_line_color = None
        return barchart

    def _update_task_execution_by_prefix_chart(self) -> None:
        """Rebuild X axis and tooltips of execution by prefix stacked chart

        See also
        --------
        _build_task_execution_by_prefix_data
        _build_task_execution_by_prefix_chart
        """
        barchart = self.task_exec_by_prefix_chart
        assert barchart is not None
        barchart.y_range = FactorRange(*self.visible_functions)

        palette = [
            p
            for p, a in zip(self._get_palette(), self.visible_activities)
            if a in self.stacked_chart_visible_activities
        ]
        assert len(palette) == len(self.stacked_chart_visible_activities)
        renderers = barchart.hbar_stack(
            self.stacked_chart_visible_activities,
            y="__functions",
            width=0.9,
            source=self.task_exec_by_prefix_src,
            color=palette,
            legend_label=self.stacked_chart_visible_activities,
        )

        # Create or refresh hovertools on top of base tools
        barchart.tools = barchart.tools[: len(self.BASE_TOOLS)]

        for vbar in renderers:
            tooltips = [
                ("function", "@__functions"),
                (vbar.name, f"@{{__{vbar.name}_text}}"),
                ("total", "@____total_text"),
            ]
            barchart.add_tools(HoverTool(tooltips=tooltips, renderers=[vbar]))
        barchart.renderers = renderers


class Contention(DashboardComponent):
    """
    Event Loop Health (and GIL Contention, if configured)
    """

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.data = dict(
            names=[
                ("Scheduler", "Event Loop"),
                ("Scheduler", "GIL"),
                ("Workers", "Event Loop"),
                ("Workers", "GIL"),
            ],
            values=[0, 0, 0, 0],
            text=["0s", "0%", "0s", "0%"],
        )
        title = "Event Loop & GIL Contention"

        # Remove GIL related names/values if not monitoring GIL
        if not self.scheduler.monitor.monitor_gil_contention:
            title = "Event Loop"
            for key in self.data:
                self.data[key] = self.data[key][::2]

        self.source = ColumnDataSource(data=self.data)
        self.root = figure(
            title=title,
            x_range=FactorRange(*self.data["names"]),
            y_range=(0, 1),
            tools="",
            toolbar_location="above",
            **kwargs,
        )
        self.root.vbar(
            x="names",
            top="values",
            width=0.9,
            line_color="white",
            source=self.source,
            fill_color=factor_cmap(
                field_name="names",
                palette=["#b8e0ce", "#81aae4"],
                factors=["Event Loop", "GIL"],
                start=1,
                end=2,
            ),
        )

        self.root.x_range.group_padding = 0.25
        self.root.xaxis.minor_tick_line_alpha = 0
        self.root.ygrid.visible = True
        self.root.xgrid.visible = False

        hover = HoverTool(
            tooltips=[("Name", "@names"), ("Value", "@text")], mode="vline"
        )
        self.root.add_tools(hover)

    @without_property_validation
    @log_errors
    def update(self):
        s = self.scheduler

        self.data["values"] = [
            s._tick_interval_observed,
            self.gil_contention_scheduler,
            sum(w.metrics["event_loop_interval"] for w in s.workers.values())
            / (len(s.workers) or 1),
            self.gil_contention_workers,
        ][:: 1 if s.monitor.monitor_gil_contention else 2]

        # Format event loop as time and GIL (if configured) as %
        self.data["text"] = [
            (
                f"{x * 100:.1f}%"
                if i % 2 and s.monitor.monitor_gil_contention
                else format_time(x)
            )
            for i, x in enumerate(self.data["values"])
        ]
        update(self.source, self.data)

    @property
    def gil_contention_workers(self) -> float:
        workers = self.scheduler.workers
        if workers:
            return sum(
                w.metrics.get("gil_contention", 0) for w in workers.values()
            ) / len(workers)
        return float("NaN")

    @property
    def gil_contention_scheduler(self) -> float:
        return self.scheduler.monitor.recent().get("gil_contention", float("NaN"))


class ExceptionsTable(DashboardComponent):
    """
    Exceptions logged in tasks.

    Since there might be many related exceptions (e.g., all tasks in a given
    task group fail for the same reason), we make a best-effort attempt to
    (1) aggregate to the task group, and (2) deduplicate similar looking tasks.
    """

    scheduler: Scheduler

    def __init__(self, scheduler: Scheduler, width: int = 1000, **kwargs: Any):
        self.scheduler = scheduler

        self.names = [
            "Task",
            "Exception",
            "Traceback",
            "Worker(s)",
            "Count",
        ]

        self.source = ColumnDataSource({k: [] for k in self.names})

        code_formatter = HTMLTemplateFormatter(
            template='<code title="<%- value %>"><%= value %></code>'
        )
        columns = [
            TableColumn(
                field="Task",
                title="Task",
                formatter=code_formatter,
                width=150,
            ),
            TableColumn(
                field="Exception",
                title="Exception",
                formatter=code_formatter,
                width=300,
            ),
            TableColumn(
                field="Traceback",
                title="Traceback",
                formatter=code_formatter,
                width=300,
            ),
            TableColumn(
                field="Worker(s)",
                title="Worker(s)",
                formatter=code_formatter,
                width=200,
            ),
            TableColumn(
                field="Count",
                title="Count",
                formatter=NumberFormatter(format="0,0"),
                width=50,
            ),
        ]

        if "sizing_mode" in kwargs:
            sizing_mode = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            sizing_mode = {}
        self.root = DataTable(
            source=self.source,
            columns=columns,
            reorderable=True,
            sortable=True,
            width=width,
            index_position=None,
            **_DATATABLE_STYLESHEETS_KWARGS,
            **sizing_mode,
        )

    @without_property_validation
    def update(self):
        new_data = {name: [] for name in self.names}
        erred_tasks = self.scheduler.erred_tasks

        for ts in erred_tasks:
            new_data["Task"].append(ts.key)
            new_data["Exception"].append(ts.exception_text)
            new_data["Traceback"].append(ts.traceback_text)
            new_data["Worker(s)"].append(",\n".join(ts.erred_on))
            new_data["Count"].append(len(ts.erred_on))

        update(self.source, new_data)


class WorkerTable(DashboardComponent):
    """Status of the current workers

    This is two plots, a text-based table for each host and a thin horizontal
    plot laying out hosts by their current memory use.
    """

    excluded_names = {
        "executing",
        "in_flight",
        "in_memory",
        "ready",
        "time",
        # Use scheduler.WorkerState.memory.managed instead of
        # scheduler.WorkerState.metrics["managed_bytes"]; the two measures are slightly
        # different. See explanation in scheduler.WorkerState.memory().
        "managed_bytes",
        "spilled_bytes",
    }

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.names = [
            "name",
            "address",
            "nthreads",
            "cpu",
            "memory",
            "memory_limit",
            "memory_percent",
            "memory_managed",
            "memory_unmanaged_old",
            "memory_unmanaged_recent",
            "memory_spilled",
            "num_fds",
            "host_net_io.read_bps",
            "host_net_io.write_bps",
            "host_disk_io.read_bps",
            "host_disk_io.write_bps",
            "cpu_fraction",
        ]
        workers = self.scheduler.workers.values()
        self.extra_names = sorted(
            {
                m
                for ws in workers
                for m, v in ws.metrics.items()
                if m not in self.names and isinstance(v, (str, int, float))
            }
            - self.excluded_names
        )

        table_names = [
            "name",
            "address",
            "nthreads",
            "cpu",
            "memory",
            "memory_limit",
            "memory_percent",
            "memory_managed",
            "memory_unmanaged_old",
            "memory_unmanaged_recent",
            "memory_spilled",
            "num_fds",
            "host_net_io.read_bps",
            "host_net_io.write_bps",
            "host_disk_io.read_bps",
            "host_disk_io.write_bps",
        ]
        column_title_renames = {
            "memory_limit": "limit",
            "memory_percent": "memory %",
            "memory_managed": "managed",
            "memory_unmanaged_old": "unmanaged old",
            "memory_unmanaged_recent": "unmanaged recent",
            "memory_spilled": "spilled",
            "num_fds": "# fds",
            "host_net_io.read_bps": "net read",
            "host_net_io.write_bps": "net write",
            "host_disk_io.read_bps": "disk read",
            "host_disk_io.write_bps": "disk write",
        }

        self.source = ColumnDataSource({k: [] for k in self.names})

        columns = {
            name: TableColumn(field=name, title=column_title_renames.get(name, name))
            for name in table_names
        }

        formatters = {
            "cpu": NumberFormatter(format="0 %"),
            "memory_percent": NumberFormatter(format="0.0 %"),
            "memory": NumberFormatter(format="0.0 b"),
            "memory_limit": NumberFormatter(format="0.0 b"),
            "memory_managed": NumberFormatter(format="0.0 b"),
            "memory_unmanaged_old": NumberFormatter(format="0.0 b"),
            "memory_unmanaged_recent": NumberFormatter(format="0.0 b"),
            "memory_spilled": NumberFormatter(format="0.0 b"),
            "host_net_io.read_bps": NumberFormatter(format="0 b"),
            "host_net_io.write_bps": NumberFormatter(format="0 b"),
            "num_fds": NumberFormatter(format="0"),
            "nthreads": NumberFormatter(format="0"),
            "host_disk_io.read_bps": NumberFormatter(format="0 b"),
            "host_disk_io.write_bps": NumberFormatter(format="0 b"),
        }

        table = DataTable(
            source=self.source,
            columns=[columns[n] for n in table_names],
            reorderable=True,
            sortable=True,
            width_policy="max",
            index_position=None,
            **_DATATABLE_STYLESHEETS_KWARGS,
        )

        for name in table_names:
            if name in formatters:
                table.columns[table_names.index(name)].formatter = formatters[name]

        extra_names = ["name", "address"] + self.extra_names
        extra_columns = {
            name: TableColumn(field=name, title=column_title_renames.get(name, name))
            for name in extra_names
        }

        extra_table = DataTable(
            source=self.source,
            columns=[extra_columns[n] for n in extra_names],
            reorderable=True,
            sortable=True,
            width_policy="max",
            index_position=None,
            **_DATATABLE_STYLESHEETS_KWARGS,
        )

        for name in extra_names:
            if name in formatters:
                extra_table.columns[extra_names.index(name)].formatter = formatters[
                    name
                ]

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">Worker (@name): </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@memory_percent{0.0 %}</span>
                </div>
                """,
        )

        mem_plot = figure(
            title="Memory Use (%)",
            toolbar_location=None,
            x_range=(0, 1),
            y_range=(-0.1, 0.1),
            height=60,
            tools="",
            min_border_right=0,
            **kwargs,
        )
        mem_plot.scatter(
            source=self.source, x="memory_percent", y=0, size=10, fill_alpha=0.5
        )
        mem_plot.ygrid.visible = False
        mem_plot.yaxis.minor_tick_line_alpha = 0
        mem_plot.xaxis.visible = False
        mem_plot.yaxis.visible = False
        mem_plot.add_tools(hover, BoxSelectTool())

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">Worker (@name): </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@cpu_fraction{0 %}</span>
                </div>
                """,
        )

        cpu_plot = figure(
            title="CPU Use (%)",
            toolbar_location=None,
            x_range=(0, 1),
            y_range=(-0.1, 0.1),
            height=60,
            tools="",
            min_border_right=0,
            **kwargs,
        )
        cpu_plot.scatter(
            source=self.source, x="cpu_fraction", y=0, size=10, fill_alpha=0.5
        )
        cpu_plot.ygrid.visible = False
        cpu_plot.yaxis.minor_tick_line_alpha = 0
        cpu_plot.xaxis.visible = False
        cpu_plot.yaxis.visible = False
        cpu_plot.add_tools(hover, BoxSelectTool())
        self.cpu_plot = cpu_plot

        if "sizing_mode" in kwargs:
            sizing_mode = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            sizing_mode = {}

        components = [cpu_plot, mem_plot, table]
        if self.extra_names:
            components.append(extra_table)

        self.root = column(*components, **sizing_mode)

    @without_property_validation
    def update(self):
        data = {name: [] for name in self.names + self.extra_names}
        for i, ws in enumerate(
            sorted(self.scheduler.workers.values(), key=lambda ws: str(ws.name))
        ):
            minfo = ws.memory

            for name in self.names + self.extra_names:
                if "." in name:
                    n0, _, n1 = name.partition(".")
                    v = ws.metrics.get(n0, {}).get(n1, None)
                else:
                    v = ws.metrics.get(name, None)
                data[name].append(v)

            data["name"][-1] = ws.name if ws.name is not None else i
            data["address"][-1] = ws.address
            if ws.memory_limit:
                data["memory_percent"][-1] = ws.metrics["memory"] / ws.memory_limit
            else:
                data["memory_percent"][-1] = ""
            data["memory_limit"][-1] = ws.memory_limit
            data["memory_managed"][-1] = minfo.managed
            data["memory_unmanaged_old"][-1] = minfo.unmanaged_old
            data["memory_unmanaged_recent"][-1] = minfo.unmanaged_recent
            data["memory_unmanaged_recent"][-1] = minfo.unmanaged_recent
            data["memory_spilled"][-1] = minfo.spilled
            data["cpu"][-1] = ws.metrics["cpu"] / 100.0
            data["cpu_fraction"][-1] = ws.metrics["cpu"] / 100.0 / ws.nthreads
            data["nthreads"][-1] = ws.nthreads

        for name in self.names + self.extra_names:
            if name == "name":
                data[name].insert(0, f"Total ({len(data[name])})")
                continue
            try:
                if len(self.scheduler.workers) == 0:
                    total_data = None
                elif name == "memory_percent":
                    total_mem = sum(
                        ws.memory_limit for ws in self.scheduler.workers.values()
                    )
                    total_data = (
                        (
                            sum(
                                ws.metrics["memory"]
                                for ws in self.scheduler.workers.values()
                            )
                            / total_mem
                        )
                        if total_mem
                        else ""
                    )
                elif name == "cpu":
                    total_data = (
                        sum(ws.metrics["cpu"] for ws in self.scheduler.workers.values())
                        / 100
                        / len(self.scheduler.workers.values())
                    )
                elif name == "cpu_fraction":
                    total_data = (
                        sum(ws.metrics["cpu"] for ws in self.scheduler.workers.values())
                        / 100
                        / sum(ws.nthreads for ws in self.scheduler.workers.values())
                    )
                else:
                    total_data = sum(data[name])

                data[name].insert(0, total_data)
            except TypeError:
                data[name].insert(0, None)

        self.source.data.update(data)


class Shuffling(DashboardComponent):
    """Occupancy (in time) per worker"""

    @log_errors
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "worker": [],
                "y": [],
                "comm_memory": [],
                "comm_memory_limit": [],
                "comm_buckets": [],
                "comm_avg_duration": [],
                "comm_avg_size": [],
                "comm_read": [],
                "comm_written": [],
                "comm_color": [],
                "disk_memory": [],
                "disk_memory_limit": [],
                "disk_buckets": [],
                "disk_avg_duration": [],
                "disk_avg_size": [],
                "disk_read": [],
                "disk_written": [],
                "disk_color": [],
            }
        )
        self.totals_source = ColumnDataSource(
            {
                "x": ["Network Send", "Network Receive", "Disk Write", "Disk Read"],
                "values": [0, 0, 0, 0],
            }
        )

        self.comm_memory = figure(
            title="Comms Buffer",
            tools="",
            toolbar_location="above",
            x_range=Range1d(0, 100_000_000),
            **kwargs,
        )
        self.comm_memory.hbar(
            source=self.source,
            right="comm_memory",
            y="y",
            height=0.9,
            color="comm_color",
        )
        hover = HoverTool(
            tooltips=[
                ("Memory Used", "@comm_memory{0.00 b}"),
                ("Average Write", "@comm_avg_size{0.00 b}"),
                ("# Buckets", "@comm_buckets"),
                ("Average Duration", "@comm_avg_duration"),
            ],
            formatters={"@comm_avg_duration": "datetime"},
            mode="hline",
        )
        self.comm_memory.add_tools(hover)
        self.comm_memory.x_range.start = 0
        self.comm_memory.x_range.end = 1
        self.comm_memory.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")

        self.disk_memory = figure(
            title="Disk Buffer",
            tools="",
            toolbar_location="above",
            x_range=Range1d(0, 100_000_000),
            **kwargs,
        )
        self.disk_memory.yaxis.visible = False

        self.disk_memory.hbar(
            source=self.source,
            right="disk_memory",
            y="y",
            height=0.9,
            color="disk_color",
        )

        hover = HoverTool(
            tooltips=[
                ("Memory Used", "@disk_memory{0.00 b}"),
                ("Average Write", "@disk_avg_size{0.00 b}"),
                ("# Buckets", "@disk_buckets"),
                ("Average Duration", "@disk_avg_duration"),
            ],
            formatters={"@disk_avg_duration": "datetime"},
            mode="hline",
        )
        self.disk_memory.add_tools(hover)
        self.disk_memory.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")

        self.totals = figure(
            title="Total movement",
            tools="",
            toolbar_location="above",
            **kwargs,
        )
        titles = ["Network Send", "Network Receive", "Disk Write", "Disk Read"]
        self.totals = figure(
            x_range=titles,
            title="Totals",
            toolbar_location=None,
            tools="",
            **kwargs,
        )

        self.totals.vbar(
            x="x",
            top="values",
            width=0.9,
            source=self.totals_source,
        )

        self.totals.xgrid.grid_line_color = None
        self.totals.y_range.start = 0
        self.totals.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")

        hover = HoverTool(
            tooltips=[("Total", "@values{0.00b}")],
            mode="vline",
        )
        self.totals.add_tools(hover)

        self.root = row(self.comm_memory, self.disk_memory)

    @without_property_validation
    @log_errors
    def update(self):
        input = self.scheduler.extensions["shuffle"].heartbeats
        if not input:
            return

        input = list(input.values())[-1]  # TODO: multiple concurrent shuffles

        data = defaultdict(list)
        now = time()

        for i, (worker, d) in enumerate(input.items()):
            data["y"].append(i)
            data["worker"].append(worker)
            for prefix in ["comm", "disk"]:
                memory_limit = d[prefix]["memory_limit"] or 0
                data[f"{prefix}_total"].append(d[prefix]["total"])
                data[f"{prefix}_memory"].append(d[prefix]["memory"])
                data[f"{prefix}_memory_limit"].append(memory_limit)
                data[f"{prefix}_buckets"].append(d[prefix]["buckets"])
                data[f"{prefix}_avg_duration"].append(d[prefix]["avg_duration"])
                data[f"{prefix}_avg_size"].append(d[prefix]["avg_size"])
                data[f"{prefix}_read"].append(d[prefix]["read"])
                data[f"{prefix}_written"].append(d[prefix]["written"])
                if self.scheduler.workers[worker].last_seen < now - 5:
                    data[f"{prefix}_color"].append("gray")
                elif d[prefix]["memory"] > memory_limit:
                    data[f"{prefix}_color"].append("red")
                else:
                    data[f"{prefix}_color"].append("blue")

        totals = {
            "x": ["Network Send", "Network Receive", "Disk Write", "Disk Read"],
            "values": [
                sum(data["comm_written"]),
                sum(data["comm_read"]),
                sum(data["disk_written"]),
                sum(data["disk_read"]),
            ],
        }
        update(self.totals_source, totals)

        update(self.source, dict(data))
        limit = max(data["comm_memory_limit"] + data["disk_memory_limit"]) * 1.2
        self.comm_memory.x_range.end = limit
        self.disk_memory.x_range.end = limit


_STYLES = {
    "width": "100%",
    "height": "100%",
    "max-width": "1920px",
    "max-height": "1080px",
    "padding": "12px",
    "border": "1px solid lightgray",
    "box-shadow": "inset 1px 0 8px 0 lightgray",
    "overflow": "auto",
}


class SchedulerLogs:
    def __init__(self, scheduler, start=None):
        logs = scheduler.get_logs(start=start, timestamps=True)

        if not logs:
            logs_html = (
                '<p style="font-family: monospace; margin: 0;">No logs to report</p>'
            )
        else:
            logs_html = Log(
                "\n".join(
                    "%s - %s"
                    % (datetime.fromtimestamp(time).strftime("%H:%M:%S.%f"), line)
                    for time, level, line in logs
                )
            )._repr_html_()

        self.root = Div(text=logs_html, styles=_STYLES)


@log_errors
def systemmonitor_doc(scheduler, extra, doc):
    sysmon = SystemMonitor(scheduler, sizing_mode="stretch_both")
    doc.title = "Dask: Scheduler System Monitor"
    add_periodic_callback(doc, sysmon, 500)

    doc.add_root(sysmon.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def shuffling_doc(scheduler, extra, doc):
    doc.title = "Dask: Shuffling"

    shuffling = Shuffling(scheduler, width=400, height=400)
    workers_memory = WorkersMemory(scheduler, width=400, height=400)
    timeseries = SystemTimeseries(
        scheduler, width=1600, height=200, follow_interval=10000
    )
    event_loop = Contention(scheduler, width=200, height=400)

    add_periodic_callback(doc, shuffling, 200)
    add_periodic_callback(doc, workers_memory, 200)
    add_periodic_callback(doc, timeseries, 500)
    add_periodic_callback(doc, event_loop, 500)

    timeseries.bandwidth.y_range = timeseries.disk.y_range

    doc.add_root(
        column(
            row(
                workers_memory.root,
                shuffling.comm_memory,
                shuffling.disk_memory,
                shuffling.totals,
                event_loop.root,
            ),
            row(column(timeseries.bandwidth, timeseries.disk)),
        )
    )
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def stealing_doc(scheduler, extra, doc):
    occupancy = Occupancy(scheduler)
    stealing_ts = StealingTimeSeries(scheduler)
    stealing_events = StealingEvents(scheduler)
    stealing_events.root.x_range = stealing_ts.root.x_range
    doc.title = "Dask: Work Stealing"
    add_periodic_callback(doc, occupancy, 500)
    add_periodic_callback(doc, stealing_ts, 500)
    add_periodic_callback(doc, stealing_events, 500)

    doc.add_root(
        row(
            occupancy.root,
            column(
                stealing_ts.root,
                stealing_events.root,
                sizing_mode="stretch_both",
            ),
        )
    )

    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def events_doc(scheduler, extra, doc):
    events = Events(scheduler, "all", height=250)
    events.update()
    add_periodic_callback(doc, events, 500)
    doc.title = "Dask: Scheduler Events"
    doc.add_root(column(events.root, sizing_mode="scale_width"))
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def exceptions_doc(scheduler, extra, doc):
    table = ExceptionsTable(scheduler)
    table.update()
    add_periodic_callback(doc, table, 1000)
    doc.title = "Dask: Exceptions"
    doc.add_root(table.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def workers_doc(scheduler, extra, doc):
    table = WorkerTable(scheduler, sizing_mode="stretch_width")
    table.update()
    add_periodic_callback(doc, table, 500)
    doc.title = "Dask: Workers"
    doc.add_root(table.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def hardware_doc(scheduler, extra, doc):
    hw = Hardware(scheduler)
    hw.update()
    doc.title = "Dask: Cluster Hardware Bandwidth"
    doc.add_root(hw.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME

    add_periodic_callback(doc, hw, 500)


@log_errors
def tasks_doc(scheduler, extra, doc):
    ts = TaskStream(
        scheduler,
        n_rectangles=dask.config.get(
            "distributed.scheduler.dashboard.tasks.task-stream-length"
        ),
        clear_interval="60s",
        sizing_mode="stretch_both",
    )
    ts.update()
    add_periodic_callback(doc, ts, 5000)
    doc.title = "Dask: Task Stream"
    doc.add_root(ts.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def graph_doc(scheduler, extra, doc):
    graph = TaskGraph(scheduler, sizing_mode="stretch_both")
    doc.title = "Dask: Task Graph"
    graph.update()
    add_periodic_callback(doc, graph, 200)
    doc.add_root(graph.root)

    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def tg_graph_doc(scheduler, extra, doc):
    tg_graph = TaskGroupGraph(scheduler, sizing_mode="stretch_both")
    doc.title = "Dask: Task Groups Graph"
    tg_graph.update()
    add_periodic_callback(doc, tg_graph, 200)
    doc.add_root(tg_graph.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME


@log_errors
def status_doc(scheduler, extra, doc):
    cluster_memory = ClusterMemory(scheduler, sizing_mode="stretch_both")
    cluster_memory.update()
    add_periodic_callback(doc, cluster_memory, 100)
    doc.add_root(cluster_memory.root)

    if len(scheduler.workers) <= 100:
        workers_memory = WorkersMemory(scheduler, sizing_mode="stretch_both")

        processing = CurrentLoad(scheduler, sizing_mode="stretch_both")

        processing_root = processing.processing_figure
    else:
        workers_memory = WorkersMemoryHistogram(scheduler, sizing_mode="stretch_both")
        processing = ProcessingHistogram(scheduler, sizing_mode="stretch_both")

        processing_root = processing.root

    current_load = CurrentLoad(scheduler, sizing_mode="stretch_both")
    workers_transfer_bytes = WorkersTransferBytes(scheduler, sizing_mode="stretch_both")

    cpu_root = current_load.cpu_figure

    workers_memory.update()
    workers_transfer_bytes.update()
    processing.update()
    current_load.update()

    add_periodic_callback(doc, workers_memory, 100)
    add_periodic_callback(doc, workers_transfer_bytes, 100)
    add_periodic_callback(doc, processing, 100)
    add_periodic_callback(doc, current_load, 100)

    doc.add_root(workers_memory.root)

    tabs = [
        TabPanel(child=processing_root, title="Processing"),
        TabPanel(child=cpu_root, title="CPU"),
        TabPanel(child=workers_transfer_bytes.root, title="Data Transfer"),
    ]

    help_ = HelpTool(
        redirect="https://docs.dask.org/en/stable/dashboard.html#task-processing-cpu-utilization-occupancy-data-transfer",
        description="A description of Task Processing/CPU Utilization/Occupancy",
    )
    for tab in tabs:
        tab.child.toolbar_location = "above"
        tab.child.add_tools(help_)

    proc_tabs = Tabs(tabs=tabs, name="processing_tabs", sizing_mode="stretch_both")
    doc.add_root(proc_tabs)

    task_stream = TaskStream(
        scheduler,
        n_rectangles=dask.config.get(
            "distributed.scheduler.dashboard.status.task-stream-length"
        ),
        clear_interval="5s",
        sizing_mode="stretch_both",
    )
    task_stream.update()
    add_periodic_callback(doc, task_stream, 100)
    doc.add_root(task_stream.root)

    task_progress = TaskProgress(scheduler, sizing_mode="stretch_both")
    task_progress.update()
    add_periodic_callback(doc, task_progress, 100)
    doc.add_root(task_progress.root)

    doc.title = "Dask: Status"
    doc.theme = BOKEH_THEME
    doc.template = env.get_template("status.html")
    doc.template_variables.update(extra)


@log_errors
@curry
def individual_doc(cls, interval, scheduler, extra, doc, fig_attr="root", **kwargs):
    # Note: @log_errors and @curry are not compatible
    fig = cls(scheduler, sizing_mode="stretch_both", **kwargs)
    fig.update()
    add_periodic_callback(doc, fig, interval)
    doc.add_root(getattr(fig, fig_attr))
    doc.theme = BOKEH_THEME
    doc.title = "Dask: " + funcname(cls)


@log_errors
def individual_profile_doc(scheduler, extra, doc):
    prof = ProfileTimePlot(scheduler, sizing_mode="stretch_both", doc=doc)
    doc.add_root(prof.root)
    prof.trigger_update()
    doc.theme = BOKEH_THEME


@log_errors
def individual_profile_server_doc(scheduler, extra, doc):
    prof = ProfileServer(scheduler, sizing_mode="stretch_both", doc=doc)
    doc.add_root(prof.root)
    prof.trigger_update()
    doc.theme = BOKEH_THEME


@log_errors
def profile_doc(scheduler, extra, doc):
    doc.title = "Dask: Profile"
    prof = ProfileTimePlot(scheduler, sizing_mode="stretch_both", doc=doc)
    doc.add_root(prof.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME

    prof.trigger_update()


@log_errors
def profile_server_doc(scheduler, extra, doc):
    doc.title = "Dask: Profile of Event Loop"
    prof = ProfileServer(scheduler, sizing_mode="stretch_both", doc=doc)
    doc.add_root(prof.root)
    doc.template = env.get_template("simple.html")
    doc.template_variables.update(extra)
    doc.theme = BOKEH_THEME

    prof.trigger_update()
