from __future__ import print_function, division, absolute_import

from functools import partial
import logging
import math
from numbers import Number
from operator import add
import os

from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    DataRange1d,
    HoverTool,
    ResetTool,
    PanTool,
    WheelZoomTool,
    TapTool,
    OpenURL,
    Range1d,
    Plot,
    Quad,
    value,
    LinearAxis,
    NumeralTickFormatter,
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
from bokeh.transform import factor_cmap
from bokeh.io import curdoc
from toolz import pipe, merge
from tornado import escape

try:
    import numpy as np
except ImportError:
    np = False

from . import components
from .components import (
    DashboardComponent,
    ProfileTimePlot,
    ProfileServer,
    add_periodic_callback,
)
from .core import BokehServer
from .worker import SystemMonitor, counters_doc
from .utils import transpose, BOKEH_VERSION, without_property_validation
from ..metrics import time
from ..utils import log_errors, format_bytes, format_time
from ..diagnostics.progress_stream import color_of, progress_quads, nbytes_bar
from ..diagnostics.progress import AllProgress
from ..diagnostics.graph_layout import GraphLayout
from ..diagnostics.task_stream import TaskStreamPlugin

try:
    from cytoolz.curried import map, concat, groupby, valmap, first
except ImportError:
    from toolz.curried import map, concat, groupby, valmap, first

logger = logging.getLogger(__name__)


PROFILING = False

from jinja2 import Environment, FileSystemLoader

env = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
)

template_variables = {
    "pages": ["status", "workers", "tasks", "system", "profile", "graph"]
}

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "theme.yaml"))

nan = float("nan")
inf = float("inf")


@without_property_validation
def update(source, data):
    """ Update source with data

    This checks a few things first

    1.  If the data is the same, then don't update
    2.  If numpy is available and the data is numeric, then convert to numpy
        arrays
    3.  If profiling then perform the update in another callback
    """
    if not np or not any(isinstance(v, np.ndarray) for v in source.data.values()):
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
                    "bokeh_address": ["", ""],
                }
            )

            fig = figure(
                title="Occupancy",
                tools="",
                id="bk-occupancy-plot",
                x_axis_type="datetime",
                **kwargs
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

            tap = TapTool(callback=OpenURL(url="http://@bokeh_address/main"))

            hover = HoverTool()
            hover.tooltips = "@worker : @occupancy s."
            hover.point_policy = "follow_mouse"
            fig.add_tools(hover, tap)

            self.root = fig

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())

            bokeh_addresses = []
            for ws in workers:
                addr = self.scheduler.get_worker_service_addr(ws.address, "bokeh")
                bokeh_addresses.append("%s:%d" % addr if addr is not None else "")

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
                    format_time(total / self.scheduler.total_ncores),
                )
            else:
                self.root.title.text = "Occupancy"

            if occupancy:
                result = {
                    "occupancy": occupancy,
                    "worker": [ws.address for ws in workers],
                    "ms": ms,
                    "color": color,
                    "bokeh_address": bokeh_addresses,
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
                title="Tasks Processing",
                id="bk-nprocessing-histogram-plot",
                name="processing_hist",
                **kwargs
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
                color="blue",
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
                title="Bytes Stored",
                name="nbytes_hist",
                id="bk-nbytes-histogram-plot",
                **kwargs
            )
            self.root.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
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
                color="blue",
            )

    @without_property_validation
    def update(self):
        nbytes = np.asarray([ws.nbytes for ws in self.scheduler.workers.values()])
        counts, x = np.histogram(nbytes, bins=40)
        d = {"left": x[:-1], "right": x[1:], "top": counts}
        self.source.data.update(d)

        self.root.title.text = "Bytes stored: " + format_bytes(nbytes.sum())


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
                    "worker": ["a", "b"],
                    "y": [1, 2],
                    "nbytes-color": ["blue", "blue"],
                    "bokeh_address": ["", ""],
                }
            )

            processing = figure(
                title="Tasks Processing",
                tools="",
                id="bk-nprocessing-plot",
                name="processing_hist",
                width=int(width / 2),
                **kwargs
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
                **kwargs
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

            nbytes.axis[0].ticker = BasicTicker(mantissas=[1, 256, 512], base=1024)
            nbytes.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            nbytes.xaxis.major_label_orientation = -math.pi / 12
            nbytes.x_range.start = 0

            for fig in [processing, nbytes]:
                fig.xaxis.minor_tick_line_alpha = 0
                fig.yaxis.visible = False
                fig.ygrid.visible = False

                tap = TapTool(callback=OpenURL(url="http://@bokeh_address/main"))
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

            self.processing_figure = processing
            self.nbytes_figure = nbytes

            processing.y_range = nbytes.y_range

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())

            bokeh_addresses = []
            for ws in workers:
                addr = self.scheduler.get_worker_service_addr(ws.address, "bokeh")
                bokeh_addresses.append("%s:%d" % addr if addr is not None else "")

            y = list(range(len(workers)))
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

                if limit > max_limit:
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
                    "nprocessing": nprocessing,
                    "nprocessing-half": [np / 2 for np in nprocessing],
                    "nprocessing-color": processing_color,
                    "nbytes": nbytes,
                    "nbytes-half": [nb / 2 for nb in nbytes],
                    "nbytes-color": nbytes_color,
                    "nbytes_text": nbytes_text,
                    "bokeh_address": bokeh_addresses,
                    "worker": [ws.address for ws in workers],
                    "y": y,
                }

                self.nbytes_figure.title.text = "Bytes stored: " + format_bytes(
                    sum(nbytes)
                )

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
            **kwargs
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
            **kwargs
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
            **kwargs
        )

        fig.circle(
            source=self.source,
            x="time",
            y="y",
            color="color",
            size=50,
            alpha=0.5,
            legend="action",
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


class TaskStream(components.TaskStream):
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

        components.TaskStream.__init__(
            self, n_rectangles=n_rectangles, clear_interval=clear_interval, **kwargs
        )

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

            # If there has been a significant delay then clear old rectangles
            first_end = min(map(add, rectangles["start"], rectangles["duration"]))
            if first_end > self.last:
                last = self.last
                self.last = first_end
                if first_end > last + self.clear_interval * 1000:
                    self.offset = min(rectangles["start"])
                    self.source.data.update({k: [] for k in rectangles})

            rectangles["start"] = [x - self.offset for x in rectangles["start"]]

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


class GraphPlot(DashboardComponent):
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
            legend="state",
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
                node_name.append(task.prefix)

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
        ps = [p for p in scheduler.plugins if isinstance(p, AllProgress)]
        if ps:
            self.plugin = ps[0]
        else:
            self.plugin = AllProgress(scheduler)

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
            **kwargs
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
            state = {"all": valmap(len, self.plugin.all), "nbytes": self.plugin.nbytes}
            for k in ["memory", "erred", "released", "processing", "waiting"]:
                state[k] = valmap(len, self.plugin.state[k])
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


class MemoryUse(DashboardComponent):
    """ The memory usage across the cluster, grouped by task type """

    def __init__(self, scheduler, **kwargs):
        self.scheduler = scheduler
        ps = [p for p in scheduler.plugins if isinstance(p, AllProgress)]
        if ps:
            self.plugin = ps[0]
        else:
            self.plugin = AllProgress(scheduler)

        self.source = ColumnDataSource(
            data=dict(
                name=[],
                left=[],
                right=[],
                center=[],
                color=[],
                percent=[],
                MB=[],
                text=[],
            )
        )

        self.root = Plot(
            id="bk-nbytes-plot",
            x_range=DataRange1d(),
            y_range=DataRange1d(),
            toolbar_location=None,
            outline_line_color=None,
            **kwargs
        )

        self.root.add_glyph(
            self.source,
            Quad(
                top=1,
                bottom=0,
                left="left",
                right="right",
                fill_color="color",
                fill_alpha=1,
            ),
        )

        self.root.add_layout(LinearAxis(), "left")
        self.root.add_layout(LinearAxis(), "below")

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
                """,
        )
        self.root.add_tools(hover)

    @without_property_validation
    def update(self):
        with log_errors():
            nb = nbytes_bar(self.plugin.nbytes)
            update(self.source, nb)
            self.root.title.text = "Memory Use: %0.2f MB" % (
                sum(self.plugin.nbytes.values()) / 1e6
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
            "worker",
            "ncores",
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
            {m for ws in workers for m in ws.metrics if m not in self.names}
            - self.excluded_names
        )

        table_names = [
            "worker",
            "ncores",
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
            "ncores": NumberFormatter(format="0"),
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
            **dt_kwargs
        )

        for name in table_names:
            if name in formatters:
                table.columns[table_names.index(name)].formatter = formatters[name]

        extra_names = ["worker"] + self.extra_names
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
            **dt_kwargs
        )

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@worker: </span>
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
            **kwargs
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
                  <span style="font-size: 10px; font-family: Monaco, monospace;">@worker: </span>
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
            **kwargs
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
        for addr, ws in sorted(self.scheduler.workers.items()):
            for name in self.names + self.extra_names:
                data[name].append(ws.metrics.get(name, None))
            data["worker"][-1] = ws.address
            if ws.memory_limit:
                data["memory_percent"][-1] = ws.metrics["memory"] / ws.memory_limit
            else:
                data["memory_percent"][-1] = ""
            data["memory_limit"][-1] = ws.memory_limit
            data["cpu"][-1] = ws.metrics["cpu"] / 100.0
            data["cpu_fraction"][-1] = ws.metrics["cpu"] / 100.0 / ws.ncores
            data["ncores"][-1] = ws.ncores

        self.source.data.update(data)


def systemmonitor_doc(scheduler, extra, doc):
    with log_errors():
        sysmon = SystemMonitor(scheduler, sizing_mode="stretch_both")
        doc.title = "Dask: Scheduler System Monitor"
        add_periodic_callback(doc, sysmon, 500)

        for subdoc in sysmon.root.children:
            doc.add_root(subdoc)
        doc.template = env.get_template("system.html")
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
            n_rectangles=100000,
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
        graph = GraphPlot(scheduler, sizing_mode="stretch_both")
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
            n_rectangles=1000,
            clear_interval="10s",
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
        graph = GraphPlot(scheduler, sizing_mode="stretch_both")
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


def profile_doc(scheduler, extra, doc):
    with log_errors():
        doc.title = "Dask: Profile"
        prof = ProfileTimePlot(scheduler, sizing_mode="scale_width", doc=doc)
        doc.add_root(prof.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME

        prof.trigger_update()


def profile_server_doc(scheduler, extra, doc):
    with log_errors():
        doc.title = "Dask: Profile of Event Loop"
        prof = ProfileServer(scheduler, sizing_mode="scale_width", doc=doc)
        doc.add_root(prof.root)
        doc.template = env.get_template("simple.html")
        doc.template_variables.update(extra)
        doc.theme = BOKEH_THEME

        prof.trigger_update()


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None, prefix="", **kwargs):
        self.scheduler = scheduler
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.server_kwargs = kwargs
        self.server_kwargs["prefix"] = prefix or None

        self.apps = {
            "/system": systemmonitor_doc,
            "/stealing": stealing_doc,
            "/workers": workers_doc,
            "/events": events_doc,
            "/counters": counters_doc,
            "/tasks": tasks_doc,
            "/status": status_doc,
            "/profile": profile_doc,
            "/profile-server": profile_server_doc,
            "/graph": graph_doc,
            "/individual-task-stream": individual_task_stream_doc,
            "/individual-progress": individual_progress_doc,
            "/individual-graph": individual_graph_doc,
            "/individual-profile": individual_profile_doc,
            "/individual-profile-server": individual_profile_server_doc,
            "/individual-nbytes": individual_nbytes_doc,
            "/individual-nprocessing": individual_nprocessing_doc,
            "/individual-workers": individual_workers_doc,
        }

        self.apps = {k: partial(v, scheduler, self.extra) for k, v in self.apps.items()}

        self.loop = io_loop or scheduler.loop
        self.server = None

    @property
    def extra(self):
        return merge({"prefix": self.prefix}, template_variables)

    @property
    def my_server(self):
        return self.scheduler

    def listen(self, *args, **kwargs):
        super(BokehScheduler, self).listen(*args, **kwargs)

        from .scheduler_html import routes

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)
