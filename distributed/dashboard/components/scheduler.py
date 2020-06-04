from collections import defaultdict
import logging
import math
from numbers import Number
import operator
import os

from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    ColorBar,
    DataRange1d,
    HoverTool,
    ResetTool,
    PanTool,
    WheelZoomTool,
    TapTool,
    OpenURL,
    Range1d,
    value,
    NumeralTickFormatter,
    BoxZoomTool,
    AdaptiveTicker,
    BasicTicker,
    NumberFormatter,
    BoxSelectTool,
    GroupFilter,
    CDSView,
)
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure
from bokeh.palettes import Viridis11
from bokeh.themes import Theme
from bokeh.transform import factor_cmap, linear_cmap
from bokeh.io import curdoc
import dask
from dask import config
from dask.utils import format_bytes, key_split
from tlz import pipe
from tlz.curried import map, concat, groupby
from tornado import escape

try:
    import numpy as np
except ImportError:
    np = False

from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (
    DashboardComponent,
    ProfileTimePlot,
    ProfileServer,
    SystemMonitor,
)
from distributed.dashboard.utils import (
    transpose,
    BOKEH_VERSION,
    PROFILING,
    without_property_validation,
    update,
)
from distributed.metrics import time
from distributed.utils import log_errors, format_time, parse_timedelta
from distributed.diagnostics.progress_stream import color_of, progress_quads
from distributed.diagnostics.graph_layout import GraphLayout
from distributed.diagnostics.task_stream import TaskStreamPlugin
from distributed.diagnostics.task_stream import color_of as ts_color_of
from distributed.diagnostics.task_stream import colors as ts_color_lookup

if dask.config.get("distributed.dashboard.export-tool"):
    from distributed.dashboard.export_tool import ExportTool
else:
    ExportTool = None

logger = logging.getLogger(__name__)

from jinja2 import Environment, FileSystemLoader

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "..", "http", "templates")
    )
)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "..", "theme.yaml"))
TICKS_1024 = {"base": 1024, "mantissas": [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]}

nan = float("nan")
inf = float("inf")


class Occupancy(DashboardComponent):
    """ Occupancy (in time) per worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
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

            fig = figure(
                title="Occupancy",
                tools="",
                id="bk-occupancy-plot",
                x_axis_type="datetime",
                **kwargs,
            )
            rect = fig.rect(
                source=self.source, x="x", width="ms", y="y", height=1, color="color"
            )
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            # fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0s')
            fig.x_range.start = 0

            tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))

            hover = HoverTool()
            hover.tooltips = "@worker : @occupancy s."
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover, tap)

            self.root = fig

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())

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
                self.root.title.text = "Occupancy -- total time: %s  wall time: %s" % (
                    format_time(total),
                    format_time(total / self.scheduler.total_nthreads),
                )
            else:
                self.root.title.text = "Occupancy"

            if occupancy:
                result = {
                    "occupancy": occupancy,
                    "worker": [ws.address for ws in workers],
                    "ms": ms,
                    "color": color,
                    "escaped_worker": [escape.url_escape(ws.address) for ws in workers],
                    "x": x,
                    "y": y,
                }

                update(self.source, result)


class ProcessingHistogram(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource(
                {"left": [1, 2], "right": [10, 10], "top": [0, 0]}
            )

            self.root = figure(
                title="Tasks Processing (Histogram)",
                id="bk-nprocessing-histogram-plot",
                name="processing_hist",
                y_axis_label="frequency",
                tools="",
                **kwargs,
            )

            self.root.xaxis.minor_tick_line_alpha = 0
            self.root.ygrid.visible = False

            self.root.toolbar.logo = None
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


class NBytesHistogram(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource(
                {"left": [1, 2], "right": [10, 10], "top": [0, 0]}
            )

            self.root = figure(
                title="Bytes Stored (Histogram)",
                name="nbytes_hist",
                id="bk-nbytes-histogram-plot",
                y_axis_label="frequency",
                tools="",
                **kwargs,
            )

            self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            self.root.xaxis.ticker = AdaptiveTicker(**TICKS_1024)
            self.root.xaxis.major_label_orientation = -math.pi / 12

            self.root.xaxis.minor_tick_line_alpha = 0
            self.root.ygrid.visible = False

            self.root.toolbar.logo = None
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
        nbytes = np.asarray([ws.nbytes for ws in self.scheduler.workers.values()])
        counts, x = np.histogram(nbytes, bins=40)
        d = {"left": x[:-1], "right": x[1:], "top": counts}
        self.source.data.update(d)

        self.root.title.text = "Bytes stored (Histogram): " + format_bytes(nbytes.sum())


class BandwidthTypes(DashboardComponent):
    """ Bar chart showing bandwidth per type """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
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

            fig = figure(
                title="Bandwidth by Type",
                tools="",
                id="bk-bandwidth-type-plot",
                name="bandwidth_type_histogram",
                y_range=["a", "b"],
                **kwargs,
            )
            rect = fig.rect(
                source=self.source,
                x="bandwidth-half",
                y="type",
                width="bandwidth",
                height=1,
                color="blue",
            )
            fig.x_range.start = 0
            fig.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            fig.xaxis.ticker = AdaptiveTicker(**TICKS_1024)
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.ygrid.visible = False

            fig.toolbar.logo = None
            fig.toolbar_location = None

            hover = HoverTool()
            hover.tooltips = "@type: @bandwidth_text / s"
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover)

            self.fig = fig

    @without_property_validation
    def update(self):
        with log_errors():
            bw = self.scheduler.bandwidth_types
            self.fig.y_range.factors = list(sorted(bw))
            result = {
                "bandwidth": list(bw.values()),
                "bandwidth-half": [b / 2 for b in bw.values()],
                "type": list(bw.keys()),
                "bandwidth_text": list(map(format_bytes, bw.values())),
            }
            self.fig.title.text = "Bandwidth: " + format_bytes(self.scheduler.bandwidth)

            update(self.source, result)


class BandwidthWorkers(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, **kwargs):
        with log_errors():
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

            fig = figure(
                title="Bandwidth by Worker",
                tools="",
                id="bk-bandwidth-worker-plot",
                name="bandwidth_worker_heatmap",
                x_range=["a", "b"],
                y_range=["a", "b"],
                **kwargs,
            )
            fig.xaxis.major_label_orientation = -math.pi / 12
            rect = fig.rect(
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
            fig.add_layout(color_bar, "right")

            fig.toolbar.logo = None
            fig.toolbar_location = None

            hover = HoverTool()
            hover.tooltips = """
            <div>
                <p><b>Source:</b> @source </p>
                <p><b>Destination:</b> @destination </p>
                <p><b>Bandwidth:</b> @bandwidth_text / s</p>
            </div>
            """
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover)

            self.fig = fig

    @without_property_validation
    def update(self):
        with log_errors():
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
                else:
                    return address

            x, y, value = zip(*[(name(a), name(b), c) for (a, b), c in bw.items()])

            self.color_map.high = max(value)

            factors = list(sorted(set(x + y)))
            self.fig.x_range.factors = factors
            self.fig.y_range.factors = factors[::-1]

            result = {
                "source": x,
                "destination": y,
                "bandwidth": value,
                "bandwidth_text": list(map(format_bytes, value)),
            }
            self.fig.title.text = "Bandwidth: " + format_bytes(self.scheduler.bandwidth)

            update(self.source, result)


class ComputerPerKey(DashboardComponent):
    """ Bar chart showing time spend in action by key prefix"""

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler

            es = [p for p in self.scheduler.plugins if isinstance(p, TaskStreamPlugin)]
            if not es:
                self.plugin = TaskStreamPlugin(self.scheduler)
            else:
                self.plugin = es[0]

            compute_data = {
                "times": [0.2, 0.1],
                "color": [ts_color_lookup["transfer"], ts_color_lookup["compute"]],
                "names": ["sum", "sum_partial"],
            }

            self.compute_source = ColumnDataSource(data=compute_data)

            fig = figure(
                title="Compute Time Per Task",
                tools="",
                id="bk-Compute-by-key-plot",
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
                legend_field="names",
            )

            fig.y_range.start = 0
            fig.min_border_right = 20
            fig.min_border_bottom = 60
            fig.yaxis.axis_label = "Time (s)"
            fig.yaxis[0].formatter = NumeralTickFormatter(format="0.0s")
            fig.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
            fig.xaxis.major_label_orientation = -math.pi / 12
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.xgrid.visible = False

            fig.toolbar.logo = None
            fig.toolbar_location = None

            hover = HoverTool()
            hover.tooltips = """
            <div>
                <p><b>Name:</b> @names</p>
                <p><b>Time:</b> @times s</p>
            </div>
            """
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover)

            self.fig = fig

    @without_property_validation
    def update(self):
        with log_errors():
            compute_times = defaultdict(float)

            for key, ts in self.scheduler.task_prefixes.items():
                name = key_split(key)
                for action, t in ts.all_durations.items():
                    if action == "compute":
                        compute_times[name] += t

            # order by largest time first
            compute_times = sorted(
                compute_times.items(), key=lambda x: x[1], reverse=True
            )

            compute_colors = list()
            compute_names = list()
            compute_time = list()
            for name, t in compute_times:
                compute_names.append(name)
                compute_colors.append(ts_color_of(name))
                compute_time.append(t)

            self.fig.x_range.factors = compute_names
            self.fig.title.text = "Compute Time Per Task"

            compute_result = dict(
                times=compute_time, color=compute_colors, names=compute_names,
            )

            update(self.compute_source, compute_result)


class AggregateAction(DashboardComponent):
    """ Bar chart showing time spend in action by key prefix"""

    def __init__(self, scheduler, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler

            es = [p for p in self.scheduler.plugins if isinstance(p, TaskStreamPlugin)]
            if not es:
                self.plugin = TaskStreamPlugin(self.scheduler)
            else:
                self.plugin = es[0]

            action_data = {
                "times": [0.2, 0.1],
                "color": [ts_color_lookup["transfer"], ts_color_lookup["compute"]],
                "names": ["transfer", "compute"],
            }

            self.action_source = ColumnDataSource(data=action_data)

            fig = figure(
                title="Aggregate Per Action",
                tools="",
                id="bk-aggregate-per-action-plot",
                name="aggregate_per_action",
                x_range=["a", "b"],
                **kwargs,
            )

            rect = fig.vbar(
                source=self.action_source,
                x="names",
                top="times",
                width=0.7,
                color="color",
                legend_field="names",
            )

            fig.y_range.start = 0
            fig.min_border_right = 20
            fig.min_border_bottom = 60
            fig.yaxis[0].formatter = NumeralTickFormatter(format="0.0s")
            fig.yaxis.axis_label = "Time (s)"
            fig.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
            fig.xaxis.major_label_orientation = -math.pi / 12
            fig.xaxis.major_label_text_font_size = "16px"
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.xgrid.visible = False

            fig.toolbar.logo = None
            fig.toolbar_location = None

            hover = HoverTool()
            hover.tooltips = """
            <div>
                <p><b>Name:</b> @names</p>
                <p><b>Time:</b> @times s</p>
            </div>
            """
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover)

            self.fig = fig

    @without_property_validation
    def update(self):
        with log_errors():
            agg_times = defaultdict(float)

            for key, ts in self.scheduler.task_prefixes.items():
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

            self.fig.x_range.factors = agg_names
            self.fig.title.text = "Aggregate Time Per Action"

            action_result = dict(times=agg_time, color=agg_colors, names=agg_names,)

            update(self.action_source, action_result)


class MemoryByKey(DashboardComponent):
    """ Bar chart showing memory use by key prefix"""

    def __init__(self, scheduler, **kwargs):
        with log_errors():
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

            fig = figure(
                title="Memory Use",
                tools="",
                id="bk-memory-by-key-plot",
                name="memory_by_key",
                x_range=["a", "b"],
                **kwargs,
            )
            rect = fig.vbar(
                source=self.source, x="name", top="nbytes", width=0.9, color="color"
            )
            fig.yaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            fig.yaxis.ticker = AdaptiveTicker(**TICKS_1024)
            fig.xaxis.major_label_orientation = -math.pi / 12
            rect.nonselection_glyph = None

            fig.xaxis.minor_tick_line_alpha = 0
            fig.ygrid.visible = False

            fig.toolbar.logo = None
            fig.toolbar_location = None

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
            fig.add_tools(hover)

            self.fig = fig

    @without_property_validation
    def update(self):
        with log_errors():
            counts = defaultdict(int)
            nbytes = defaultdict(int)
            for ws in self.scheduler.workers.values():
                for ts in ws.has_what:
                    ks = key_split(ts.key)
                    counts[ks] += 1
                    nbytes[ks] += ts.nbytes

            names = list(sorted(counts))
            self.fig.x_range.factors = names
            result = {
                "name": names,
                "count": [counts[name] for name in names],
                "nbytes": [nbytes[name] for name in names],
                "nbytes_text": [format_bytes(nbytes[name]) for name in names],
                "color": [color_of(name) for name in names],
            }
            self.fig.title.text = "Total Use: " + format_bytes(sum(nbytes.values()))

            update(self.source, result)


class CurrentLoad(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, width=600, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource(
                {
                    "nprocessing": [1, 2],
                    "nprocessing-half": [0.5, 1],
                    "nprocessing-color": ["red", "blue"],
                    "nbytes": [1, 2],
                    "nbytes-half": [0.5, 1],
                    "nbytes_text": ["1B", "2B"],
                    "cpu": [1, 2],
                    "cpu-half": [0.5, 1],
                    "worker": ["a", "b"],
                    "y": [1, 2],
                    "nbytes-color": ["blue", "blue"],
                    "escaped_worker": ["a", "b"],
                }
            )

            processing = figure(
                title="Tasks Processing",
                tools="",
                id="bk-nprocessing-plot",
                name="processing_hist",
                width=int(width / 2),
                **kwargs,
            )
            rect = processing.rect(
                source=self.source,
                x="nprocessing-half",
                y="y",
                width="nprocessing",
                height=1,
                color="nprocessing-color",
            )
            processing.x_range.start = 0
            rect.nonselection_glyph = None

            nbytes = figure(
                title="Bytes stored",
                tools="",
                id="bk-nbytes-worker-plot",
                width=int(width / 2),
                name="nbytes_hist",
                **kwargs,
            )
            rect = nbytes.rect(
                source=self.source,
                x="nbytes-half",
                y="y",
                width="nbytes",
                height=1,
                color="nbytes-color",
            )
            rect.nonselection_glyph = None

            cpu = figure(
                title="CPU Utilization",
                tools="",
                id="bk-cpu-worker-plot",
                width=int(width / 2),
                name="cpu_hist",
                x_range=(0, None),
                **kwargs,
            )
            rect = cpu.rect(
                source=self.source,
                x="cpu-half",
                y="y",
                width="cpu",
                height=1,
                color="blue",
            )
            rect.nonselection_glyph = None

            nbytes.axis[0].ticker = BasicTicker(**TICKS_1024)
            nbytes.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            nbytes.xaxis.major_label_orientation = -math.pi / 12
            nbytes.x_range.start = 0

            for fig in [processing, nbytes, cpu]:
                fig.xaxis.minor_tick_line_alpha = 0
                fig.yaxis.visible = False
                fig.ygrid.visible = False

                tap = TapTool(
                    callback=OpenURL(url="./info/worker/@escaped_worker.html")
                )
                fig.add_tools(tap)

                fig.toolbar.logo = None
                fig.toolbar_location = None
                fig.yaxis.visible = False

            hover = HoverTool()
            hover.tooltips = "@worker : @nprocessing tasks"
            hover.point_policy = "follow_mouse"
            processing.add_tools(hover)

            hover = HoverTool()
            hover.tooltips = "@worker : @nbytes_text"
            hover.point_policy = "follow_mouse"
            nbytes.add_tools(hover)

            hover = HoverTool()
            hover.tooltips = "@worker : @cpu %"
            hover.point_policy = "follow_mouse"
            cpu.add_tools(hover)

            self.processing_figure = processing
            self.nbytes_figure = nbytes
            self.cpu_figure = cpu

            processing.y_range = nbytes.y_range
            cpu.y_range = nbytes.y_range

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())

            y = list(range(len(workers)))

            cpu = [int(ws.metrics["cpu"]) for ws in workers]

            nprocessing = [len(ws.processing) for ws in workers]
            processing_color = []
            for ws in workers:
                if ws in self.scheduler.idle:
                    processing_color.append("red")
                elif ws in self.scheduler.saturated:
                    processing_color.append("green")
                else:
                    processing_color.append("blue")

            nbytes = [ws.metrics["memory"] for ws in workers]
            nbytes_text = [format_bytes(nb) for nb in nbytes]
            nbytes_color = []
            max_limit = 0
            for ws, nb in zip(workers, nbytes):
                limit = (
                    getattr(self.scheduler.workers[ws.address], "memory_limit", inf)
                    or inf
                )

                if limit > max_limit and limit != inf:
                    max_limit = limit

                if nb > limit:
                    nbytes_color.append("red")
                elif nb > limit / 2:
                    nbytes_color.append("orange")
                else:
                    nbytes_color.append("blue")

            now = time()
            if any(nprocessing) or self.last + 1 < now:
                self.last = now
                result = {
                    "cpu": cpu,
                    "cpu-half": [c / 2 for c in cpu],
                    "nprocessing": nprocessing,
                    "nprocessing-half": [np / 2 for np in nprocessing],
                    "nprocessing-color": processing_color,
                    "nbytes": nbytes,
                    "nbytes-half": [nb / 2 for nb in nbytes],
                    "nbytes-color": nbytes_color,
                    "nbytes_text": nbytes_text,
                    "worker": [ws.address for ws in workers],
                    "escaped_worker": [escape.url_escape(ws.address) for ws in workers],
                    "y": y,
                }

                self.nbytes_figure.title.text = "Bytes stored: " + format_bytes(
                    sum(nbytes)
                )
                self.nbytes_figure.x_range.end = max_limit
                if self.scheduler.workers:
                    self.cpu_figure.x_range.end = (
                        max(ws.nthreads or 1 for ws in self.scheduler.workers.values())
                        * 100
                    )
                else:
                    self.cpu_figure.x_range.end = 100

                update(self.source, result)


class StealingTimeSeries(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {"time": [time(), time() + 1], "idle": [0, 0.1], "saturated": [0, 0.1]}
        )

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        fig = figure(
            title="Idle and Saturated Workers Over Time",
            x_axis_type="datetime",
            y_range=[-0.1, len(scheduler.workers) + 0.1],
            height=150,
            tools="",
            x_range=x_range,
            **kwargs,
        )
        fig.line(source=self.source, x="time", y="idle", color="red")
        fig.line(source=self.source, x="time", y="saturated", color="green")
        fig.yaxis.minor_tick_line_color = None

        fig.add_tools(
            ResetTool(), PanTool(dimensions="width"), WheelZoomTool(dimensions="width")
        )

        self.root = fig

    @without_property_validation
    def update(self):
        with log_errors():
            result = {
                "time": [time() * 1000],
                "idle": [len(self.scheduler.idle)],
                "saturated": [len(self.scheduler.saturated)],
            }
            if PROFILING:
                curdoc().add_next_tick_callback(
                    lambda: self.source.stream(result, 10000)
                )
            else:
                self.source.stream(result, 10000)


class StealingEvents(DashboardComponent):
    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        self.steal = scheduler.extensions["stealing"]
        self.last = 0
        self.source = ColumnDataSource(
            {
                "time": [time() - 20, time()],
                "level": [0, 15],
                "color": ["white", "white"],
                "duration": [0, 0],
                "radius": [1, 1],
                "cost_factor": [0, 10],
                "count": [1, 1],
            }
        )

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        fig = figure(
            title="Stealing Events",
            x_axis_type="datetime",
            y_axis_type="log",
            height=250,
            tools="",
            x_range=x_range,
            **kwargs,
        )

        fig.circle(
            source=self.source,
            x="time",
            y="cost_factor",
            color="color",
            size="radius",
            alpha=0.5,
        )
        fig.yaxis.axis_label = "Cost Multiplier"

        hover = HoverTool()
        hover.tooltips = "Level: @level, Duration: @duration, Count: @count, Cost factor: @cost_factor"
        hover.point_policy = "follow_mouse"

        fig.add_tools(
            hover,
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
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
            color = "black"

        radius = math.sqrt(min(total_duration, 10)) * 30 + 2

        d = {
            "time": time * 1000,
            "level": level,
            "count": len(msgs),
            "color": color,
            "duration": total_duration,
            "radius": radius,
            "cost_factor": min(10, self.steal.cost_multipliers[level]),
        }

        return d

    @without_property_validation
    def update(self):
        with log_errors():
            log = self.steal.log
            n = self.steal.count - self.last
            log = [log[-i] for i in range(1, n + 1) if isinstance(log[-i], list)]
            self.last = self.steal.count

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
                    curdoc().add_next_tick_callback(
                        lambda: self.source.stream(new, 10000)
                    )
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

        fig = figure(
            title=name,
            x_axis_type="datetime",
            height=height,
            tools="",
            x_range=x_range,
            **kwargs,
        )

        fig.circle(
            source=self.source,
            x="time",
            y="y",
            color="color",
            size=50,
            alpha=0.5,
            **{"legend_field" if BOKEH_VERSION >= "1.4" else "legend": "action"},
        )
        fig.yaxis.axis_label = "Action"
        fig.legend.location = "top_left"

        hover = HoverTool()
        hover.tooltips = "@action<br>@hover"
        hover.point_policy = "follow_mouse"

        fig.add_tools(
            hover,
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
        )

        self.root = fig

    @without_property_validation
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
                    times.append(msg["time"] * 1000)
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
                    curdoc().add_next_tick_callback(
                        lambda: self.source.stream(new, 10000)
                    )
                else:
                    self.source.stream(new, 10000)


class TaskStream(DashboardComponent):
    def __init__(self, scheduler, n_rectangles=1000, clear_interval="20s", **kwargs):
        self.scheduler = scheduler
        self.offset = 0
        es = [p for p in self.scheduler.plugins if isinstance(p, TaskStreamPlugin)]
        if not es:
            self.plugin = TaskStreamPlugin(self.scheduler)
        else:
            self.plugin = es[0]
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
    def update(self):
        if self.index == self.plugin.index:
            return
        with log_errors():
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
        id="bk-task-stream-plot",
        x_range=x_range,
        y_range=y_range,
        toolbar_location="above",
        x_axis_type="datetime",
        min_border_right=35,
        tools="",
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

    root.add_tools(
        hover,
        tap,
        BoxZoomTool(),
        ResetTool(),
        PanTool(dimensions="width"),
        WheelZoomTool(dimensions="width"),
    )
    if ExportTool:
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
        self.invisible_count = 0  # number of invisible nodes

        self.node_source = ColumnDataSource(
            {"x": [], "y": [], "name": [], "state": [], "visible": [], "key": []}
        )
        self.edge_source = ColumnDataSource({"x": [], "y": [], "visible": []})

        node_view = CDSView(
            source=self.node_source,
            filters=[GroupFilter(column_name="visible", group="True")],
        )
        edge_view = CDSView(
            source=self.edge_source,
            filters=[GroupFilter(column_name="visible", group="True")],
        )

        node_colors = factor_cmap(
            "state",
            factors=["waiting", "processing", "memory", "released", "erred"],
            palette=["gray", "green", "red", "blue", "black"],
        )

        self.root = figure(title="Task Graph", **kwargs)
        self.root.multi_line(
            xs="x",
            ys="y",
            source=self.edge_source,
            line_width=1,
            view=edge_view,
            color="black",
            alpha=0.3,
        )
        rect = self.root.square(
            x="x",
            y="y",
            size=10,
            color=node_colors,
            source=self.node_source,
            view=node_view,
            **{"legend_field" if BOKEH_VERSION >= "1.4" else "legend": "state"},
        )
        self.root.xgrid.grid_line_color = None
        self.root.ygrid.grid_line_color = None

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
    def update(self):
        with log_errors():
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
            if len(tasks) > self.max_items:
                # graph to big - no update, reset for next time
                self.invisible_count = len(tasks)
                return
            for key in new:
                try:
                    task = tasks[key]
                except KeyError:
                    continue
                xx = x[key]
                yy = y[key]
                node_key.append(escape.url_escape(key))
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
            self.visible_updates = []
            self.node_source.patch({"visible": updates})
            self.invisible_count += len(updates)

        if self.layout.visible_edge_updates:
            updates = self.layout.visible_edge_updates
            updates = [(i, c) for i, c in updates if i < m]
            self.visible_updates = []
            self.edge_source.patch({"visible": updates})

    def __del__(self):
        self.scheduler.remove_plugin(self.layout)


class TaskProgress(DashboardComponent):
    """ Progress bars per task type """

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler

        data = progress_quads(
            dict(all={}, memory={}, erred={}, released={}, processing={})
        )
        self.source = ColumnDataSource(data=data)

        x_range = DataRange1d(range_padding=0)
        y_range = Range1d(-8, 0)

        self.root = figure(
            id="bk-task-progress-plot",
            title="Progress",
            name="task_progress",
            x_range=x_range,
            y_range=y_range,
            toolbar_location=None,
            tools="",
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
                    <span style="font-size: 14px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Ready:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@processing</span>
                </div>
                """,
        )
        self.root.add_tools(hover)

    @without_property_validation
    def update(self):
        with log_errors():
            state = {
                "memory": {},
                "erred": {},
                "released": {},
                "processing": {},
                "waiting": {},
            }

            for tp in self.scheduler.task_prefixes.values():
                active_states = tp.active_states
                if any(active_states.get(s) for s in state.keys()):
                    state["memory"][tp.name] = active_states["memory"]
                    state["erred"][tp.name] = active_states["erred"]
                    state["released"][tp.name] = active_states["released"]
                    state["processing"][tp.name] = active_states["processing"]
                    state["waiting"][tp.name] = active_states["waiting"]

            state["all"] = {
                k: sum(v[k] for v in state.values()) for k in state["memory"]
            }

            if not state["all"] and not len(self.source.data["all"]):
                return

            d = progress_quads(state)

            update(self.source, d)

            totals = {
                k: sum(state[k].values())
                for k in ["all", "memory", "erred", "released", "waiting"]
            }
            totals["processing"] = totals["all"] - sum(
                v for k, v in totals.items() if k != "all"
            )

            self.root.title.text = (
                "Progress -- total: %(all)s, "
                "in-memory: %(memory)s, processing: %(processing)s, "
                "waiting: %(waiting)s, "
                "erred: %(erred)s" % totals
            )


class WorkerTable(DashboardComponent):
    """ Status of the current workers

    This is two plots, a text-based table for each host and a thin horizontal
    plot laying out hosts by their current memory use.
    """

    excluded_names = {"executing", "in_flight", "in_memory", "ready", "time"}

    def __init__(self, scheduler, width=800, **kwargs):
        self.scheduler = scheduler
        self.names = [
            "name",
            "address",
            "nthreads",
            "cpu",
            "memory",
            "memory_limit",
            "memory_percent",
            "num_fds",
            "read_bytes",
            "write_bytes",
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
            "num_fds",
            "read_bytes",
            "write_bytes",
        ]

        self.source = ColumnDataSource({k: [] for k in self.names})

        columns = {
            name: TableColumn(field=name, title=name.replace("_percent", " %"))
            for name in table_names
        }

        formatters = {
            "cpu": NumberFormatter(format="0.0 %"),
            "memory_percent": NumberFormatter(format="0.0 %"),
            "memory": NumberFormatter(format="0 b"),
            "memory_limit": NumberFormatter(format="0 b"),
            "read_bytes": NumberFormatter(format="0 b"),
            "write_bytes": NumberFormatter(format="0 b"),
            "num_fds": NumberFormatter(format="0"),
            "nthreads": NumberFormatter(format="0"),
        }

        if BOKEH_VERSION < "0.12.15":
            dt_kwargs = {"row_headers": False}
        else:
            dt_kwargs = {"index_position": None}

        table = DataTable(
            source=self.source,
            columns=[columns[n] for n in table_names],
            reorderable=True,
            sortable=True,
            width=width,
            **dt_kwargs,
        )

        for name in table_names:
            if name in formatters:
                table.columns[table_names.index(name)].formatter = formatters[name]

        extra_names = ["name", "address"] + self.extra_names
        extra_columns = {
            name: TableColumn(field=name, title=name.replace("_percent", "%"))
            for name in extra_names
        }

        extra_table = DataTable(
            source=self.source,
            columns=[extra_columns[n] for n in extra_names],
            reorderable=True,
            sortable=True,
            width=width,
            **dt_kwargs,
        )

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">Worker (@name): </span>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@memory_percent</span>
                </div>
                """,
        )

        mem_plot = figure(
            title="Memory Use (%)",
            toolbar_location=None,
            x_range=(0, 1),
            y_range=(-0.1, 0.1),
            height=60,
            width=width,
            tools="",
            **kwargs,
        )
        mem_plot.circle(
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
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@cpu</span>
                </div>
                """,
        )

        cpu_plot = figure(
            title="CPU Use (%)",
            toolbar_location=None,
            x_range=(0, 1),
            y_range=(-0.1, 0.1),
            height=60,
            width=width,
            tools="",
            **kwargs,
        )
        cpu_plot.circle(
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

        self.root = column(*components, id="bk-worker-table", **sizing_mode)

    @without_property_validation
    def update(self):
        data = {name: [] for name in self.names + self.extra_names}
        for i, (addr, ws) in enumerate(
            sorted(self.scheduler.workers.items(), key=lambda kv: str(kv[1].name))
        ):
            for name in self.names + self.extra_names:
                data[name].append(ws.metrics.get(name, None))
            data["name"][-1] = ws.name if ws.name is not None else i
            data["address"][-1] = ws.address
            if ws.memory_limit:
                data["memory_percent"][-1] = ws.metrics["memory"] / ws.memory_limit
            else:
                data["memory_percent"][-1] = ""
            data["memory_limit"][-1] = ws.memory_limit
            data["cpu"][-1] = ws.metrics["cpu"] / 100.0
            data["cpu_fraction"][-1] = ws.metrics["cpu"] / 100.0 / ws.nthreads
            data["nthreads"][-1] = ws.nthreads

        for name in self.names + self.extra_names:
            if name == "name":
                data[name].insert(
                    0, "Total ({nworkers})".format(nworkers=len(data[name]))
                )
                continue
            try:
                data[name].insert(0, sum(data[name]))
            except TypeError:
                data[name].insert(0, None)

        self.source.data.update(data)


def systemmonitor_doc(scheduler, extra, doc):
    with log_errors():
        sysmon = SystemMonitor(scheduler, sizing_mode="stretch_both")
        doc.title = "Dask: Scheduler System Monitor"
        add_periodic_callback(doc, sysmon, 500)

        doc.add_root(sysmon.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def stealing_doc(scheduler, extra, doc):
    with log_errors():
        occupancy = Occupancy(scheduler, height=200, sizing_mode="scale_width")
        stealing_ts = StealingTimeSeries(scheduler, sizing_mode="scale_width")
        stealing_events = StealingEvents(scheduler, sizing_mode="scale_width")
        stealing_events.root.x_range = stealing_ts.root.x_range
        doc.title = "Dask: Work Stealing"
        add_periodic_callback(doc, occupancy, 500)
        add_periodic_callback(doc, stealing_ts, 500)
        add_periodic_callback(doc, stealing_events, 500)

        doc.add_root(
            column(
                occupancy.root,
                stealing_ts.root,
                stealing_events.root,
                sizing_mode="scale_width",
            )
        )

        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def events_doc(scheduler, extra, doc):
    with log_errors():
        events = Events(scheduler, "all", height=250)
        events.update()
        add_periodic_callback(doc, events, 500)
        doc.title = "Dask: Scheduler Events"
        doc.add_root(column(events.root, sizing_mode="scale_width"))
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def workers_doc(scheduler, extra, doc):
    with log_errors():
        table = WorkerTable(scheduler)
        table.update()
        add_periodic_callback(doc, table, 500)
        doc.title = "Dask: Workers"
        doc.add_root(table.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def tasks_doc(scheduler, extra, doc):
    with log_errors():
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


def graph_doc(scheduler, extra, doc):
    with log_errors():
        graph = TaskGraph(scheduler, sizing_mode="stretch_both")
        doc.title = "Dask: Task Graph"
        graph.update()
        add_periodic_callback(doc, graph, 200)
        doc.add_root(graph.root)

        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def status_doc(scheduler, extra, doc):
    with log_errors():
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

        task_progress = TaskProgress(scheduler, sizing_mode="stretch_both")
        task_progress.update()
        add_periodic_callback(doc, task_progress, 100)

        if len(scheduler.workers) < 50:
            current_load = CurrentLoad(scheduler, sizing_mode="stretch_both")
            current_load.update()
            add_periodic_callback(doc, current_load, 100)
            doc.add_root(current_load.nbytes_figure)
            doc.add_root(current_load.processing_figure)
        else:
            nbytes_hist = NBytesHistogram(scheduler, sizing_mode="stretch_both")
            nbytes_hist.update()
            processing_hist = ProcessingHistogram(scheduler, sizing_mode="stretch_both")
            processing_hist.update()
            add_periodic_callback(doc, nbytes_hist, 100)
            add_periodic_callback(doc, processing_hist, 100)
            current_load_fig = row(
                nbytes_hist.root, processing_hist.root, sizing_mode="stretch_both"
            )

            doc.add_root(nbytes_hist.root)
            doc.add_root(processing_hist.root)

        doc.title = "Dask: Status"
        doc.add_root(task_progress.root)
        doc.add_root(task_stream.root)
        doc.theme = BOKEH_THEME
        doc.template = env.get_template("status.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME


def individual_task_stream_doc(scheduler, extra, doc):
    task_stream = TaskStream(
        scheduler, n_rectangles=1000, clear_interval="10s", sizing_mode="stretch_both"
    )
    task_stream.update()
    add_periodic_callback(doc, task_stream, 100)
    doc.add_root(task_stream.root)
    doc.theme = BOKEH_THEME


def individual_nbytes_doc(scheduler, extra, doc):
    current_load = CurrentLoad(scheduler, sizing_mode="stretch_both")
    current_load.update()
    add_periodic_callback(doc, current_load, 100)
    doc.add_root(current_load.nbytes_figure)
    doc.theme = BOKEH_THEME


def individual_cpu_doc(scheduler, extra, doc):
    current_load = CurrentLoad(scheduler, sizing_mode="stretch_both")
    current_load.update()
    add_periodic_callback(doc, current_load, 100)
    doc.add_root(current_load.cpu_figure)
    doc.theme = BOKEH_THEME


def individual_nprocessing_doc(scheduler, extra, doc):
    current_load = CurrentLoad(scheduler, sizing_mode="stretch_both")
    current_load.update()
    add_periodic_callback(doc, current_load, 100)
    doc.add_root(current_load.processing_figure)
    doc.theme = BOKEH_THEME


def individual_progress_doc(scheduler, extra, doc):
    task_progress = TaskProgress(scheduler, height=160, sizing_mode="stretch_both")
    task_progress.update()
    add_periodic_callback(doc, task_progress, 100)
    doc.add_root(task_progress.root)
    doc.theme = BOKEH_THEME


def individual_graph_doc(scheduler, extra, doc):
    with log_errors():
        graph = TaskGraph(scheduler, sizing_mode="stretch_both")
        graph.update()

        add_periodic_callback(doc, graph, 200)
        doc.add_root(graph.root)
        doc.theme = BOKEH_THEME


def individual_profile_doc(scheduler, extra, doc):
    with log_errors():
        prof = ProfileTimePlot(scheduler, sizing_mode="scale_width", doc=doc)
        doc.add_root(prof.root)
        prof.trigger_update()
        doc.theme = BOKEH_THEME


def individual_profile_server_doc(scheduler, extra, doc):
    with log_errors():
        prof = ProfileServer(scheduler, sizing_mode="scale_width", doc=doc)
        doc.add_root(prof.root)
        prof.trigger_update()
        doc.theme = BOKEH_THEME


def individual_workers_doc(scheduler, extra, doc):
    with log_errors():
        table = WorkerTable(scheduler)
        table.update()
        add_periodic_callback(doc, table, 500)
        doc.add_root(table.root)
        doc.theme = BOKEH_THEME


def individual_bandwidth_types_doc(scheduler, extra, doc):
    with log_errors():
        bw = BandwidthTypes(scheduler, sizing_mode="stretch_both")
        bw.update()
        add_periodic_callback(doc, bw, 500)
        doc.add_root(bw.fig)
        doc.theme = BOKEH_THEME


def individual_bandwidth_workers_doc(scheduler, extra, doc):
    with log_errors():
        bw = BandwidthWorkers(scheduler, sizing_mode="stretch_both")
        bw.update()
        add_periodic_callback(doc, bw, 500)
        doc.add_root(bw.fig)
        doc.theme = BOKEH_THEME


def individual_memory_by_key_doc(scheduler, extra, doc):
    with log_errors():
        component = MemoryByKey(scheduler, sizing_mode="stretch_both")
        component.update()
        add_periodic_callback(doc, component, 500)
        doc.add_root(component.fig)
        doc.theme = BOKEH_THEME


def individual_compute_time_per_key_doc(scheduler, extra, doc):
    with log_errors():
        component = ComputerPerKey(scheduler, sizing_mode="stretch_both")
        component.update()
        add_periodic_callback(doc, component, 500)
        doc.add_root(component.fig)
        doc.theme = BOKEH_THEME


def individual_aggregate_time_per_action_doc(scheduler, extra, doc):
    with log_errors():
        component = AggregateAction(scheduler, sizing_mode="stretch_both")
        component.update()
        add_periodic_callback(doc, component, 500)
        doc.add_root(component.fig)
        doc.theme = BOKEH_THEME


def profile_doc(scheduler, extra, doc):
    with log_errors():
        doc.title = "Dask: Profile"
        prof = ProfileTimePlot(scheduler, sizing_mode="stretch_both", doc=doc)
        doc.add_root(prof.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME

        prof.trigger_update()


def profile_server_doc(scheduler, extra, doc):
    with log_errors():
        doc.title = "Dask: Profile of Event Loop"
        prof = ProfileServer(scheduler, sizing_mode="stretch_both", doc=doc)
        doc.add_root(prof.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME

        prof.trigger_update()
