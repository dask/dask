from __future__ import annotations

import logging
import math
import os

from bokeh.core.properties import without_property_validation
from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    DataRange1d,
    HoverTool,
    NumeralTickFormatter,
    PanTool,
    ResetTool,
    WheelZoomTool,
)
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.palettes import RdBu
from bokeh.plotting import figure
from bokeh.themes import Theme
from jinja2 import Environment, FileSystemLoader
from tlz import merge, partition_all

from dask.utils import format_bytes, format_time

from distributed.dashboard.components import add_periodic_callback
from distributed.dashboard.components.shared import (
    DashboardComponent,
    ProfileServer,
    ProfileTimePlot,
    SystemMonitor,
)
from distributed.dashboard.utils import _DATATABLE_STYLESHEETS_KWARGS, transpose, update
from distributed.metrics import time
from distributed.utils import log_errors

logger = logging.getLogger(__name__)
env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "..", "..", "http", "templates")
    )
)

BOKEH_THEME = Theme(
    filename=os.path.join(os.path.dirname(__file__), "..", "theme.yaml")
)

template_variables = {"pages": ["status", "system", "profile"]}


def standard_doc(title, active_page, *, template="simple.html"):
    def decorator(f):
        @log_errors(unroll_stack=1)
        def wrapper(arg, extra, doc):
            doc.title = title
            doc.template = env.get_template(template)
            if active_page is not None:
                doc.template_variables["active_page"] = active_page
            doc.template_variables.update(extra)
            doc.theme = BOKEH_THEME
            return f(arg, extra, doc)

        return wrapper

    return decorator


class StateTable(DashboardComponent):
    """Currently running tasks"""

    def __init__(self, worker):
        self.worker = worker

        names = ["Stored", "Executing", "Ready", "Waiting", "Connections", "Serving"]
        self.source = ColumnDataSource({name: [] for name in names})

        columns = {name: TableColumn(field=name, title=name) for name in names}

        table = DataTable(
            source=self.source,
            columns=[columns[n] for n in names],
            height=70,
            **_DATATABLE_STYLESHEETS_KWARGS,
        )
        self.root = table

    @without_property_validation
    @log_errors
    def update(self):
        w = self.worker
        d = {
            "Stored": [len(w.data)],
            "Executing": ["%d / %d" % (w.state.executing_count, w.state.nthreads)],
            "Ready": [len(w.state.ready)],
            "Waiting": [len(w.state.waiting)],
            "Connections": [w.state.transfer_incoming_count],
            "Serving": [len(w._comms)],
        }
        update(self.source, d)


class CommunicatingStream(DashboardComponent):
    @log_errors
    def __init__(self, worker, height=300, **kwargs):
        self.worker = worker
        names = [
            "start",
            "stop",
            "middle",
            "duration",
            "who",
            "y",
            "hover",
            "alpha",
            "bandwidth",
            "total",
        ]

        self.transfer_incoming = ColumnDataSource({name: [] for name in names})
        self.transfer_outgoing = ColumnDataSource({name: [] for name in names})

        x_range = DataRange1d(range_padding=0)
        y_range = DataRange1d(range_padding=0)

        fig = figure(
            title="Peer Communications",
            x_axis_type="datetime",
            x_range=x_range,
            y_range=y_range,
            height=height,
            tools="",
            **kwargs,
        )

        fig.rect(
            source=self.transfer_incoming,
            x="middle",
            y="y",
            width="duration",
            height=0.9,
            color="red",
            alpha="alpha",
        )
        fig.rect(
            source=self.transfer_outgoing,
            x="middle",
            y="y",
            width="duration",
            height=0.9,
            color="blue",
            alpha="alpha",
        )

        hover = HoverTool(point_policy="follow_mouse", tooltips="""@hover""")
        fig.add_tools(
            hover,
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width"),
        )

        self.root = fig

        self.last_transfer_incoming_count_total = 0
        self.last_transfer_outgoing_count_total = 0
        self.who = dict()

    @without_property_validation
    @log_errors
    def update(self):
        transfer_outgoing_log = self.worker.transfer_outgoing_log
        n = (
            self.worker.transfer_outgoing_count_total
            - self.last_transfer_outgoing_count_total
        )
        transfer_outgoing_log = [
            transfer_outgoing_log[-i].copy() for i in range(1, n + 1)
        ]
        self.last_transfer_outgoing_count_total = (
            self.worker.transfer_outgoing_count_total
        )

        transfer_incoming_log = self.worker.transfer_incoming_log
        n = (
            self.worker.state.transfer_incoming_count_total
            - self.last_transfer_incoming_count_total
        )
        transfer_incoming_log = [
            transfer_incoming_log[-i].copy() for i in range(1, n + 1)
        ]
        self.last_transfer_incoming_count_total = (
            self.worker.state.transfer_incoming_count_total
        )

        for [msgs, source] in [
            [transfer_incoming_log, self.transfer_incoming],
            [transfer_outgoing_log, self.transfer_outgoing],
        ]:
            for msg in msgs:
                if "compressed" in msg:
                    del msg["compressed"]
                del msg["keys"]

                bandwidth = msg["total"] / (msg["duration"] or 0.5)
                bw = max(min(bandwidth / 500e6, 1), 0.3)
                msg["alpha"] = bw
                try:
                    msg["y"] = self.who[msg["who"]]
                except KeyError:
                    self.who[msg["who"]] = len(self.who)
                    msg["y"] = self.who[msg["who"]]

                msg["hover"] = "{} / {} = {}/s".format(
                    format_bytes(msg["total"]),
                    format_time(msg["duration"]),
                    format_bytes(msg["total"] / msg["duration"]),
                )

                for k in ["middle", "duration", "start", "stop"]:
                    msg[k] = msg[k] * 1000

            if msgs:
                msgs = transpose(msgs)
                if (
                    len(source.data["stop"])
                    and min(msgs["start"]) > source.data["stop"][-1] + 10000
                ):
                    source.data.update(msgs)
                else:
                    source.stream(msgs, rollover=10000)


class CommunicatingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({"x": [], "in": [], "out": []})

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        fig = figure(
            title="Communication History",
            x_axis_type="datetime",
            y_range=[-0.1, worker.state.transfer_incoming_count_limit + 0.5],
            height=150,
            tools="",
            x_range=x_range,
            **kwargs,
        )
        fig.line(source=self.source, x="x", y="in", color="red")
        fig.line(source=self.source, x="x", y="out", color="blue")

        fig.add_tools(
            ResetTool(), PanTool(dimensions="width"), WheelZoomTool(dimensions="width")
        )

        self.root = fig

    @without_property_validation
    @log_errors
    def update(self):
        self.source.stream(
            {
                "x": [time() * 1000],
                "out": [len(self.worker._comms)],
                "in": [self.worker.state.transfer_incoming_count],
            },
            10000,
        )


class ExecutingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({"x": [], "y": []})

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        fig = figure(
            title="Executing History",
            x_axis_type="datetime",
            y_range=[-0.1, worker.state.nthreads + 0.1],
            height=150,
            tools="",
            x_range=x_range,
            **kwargs,
        )
        fig.line(source=self.source, x="x", y="y")

        fig.add_tools(
            ResetTool(), PanTool(dimensions="width"), WheelZoomTool(dimensions="width")
        )

        self.root = fig

    @without_property_validation
    @log_errors
    def update(self):
        self.source.stream(
            {"x": [time() * 1000], "y": [self.worker.state.executing_count]}, 1000
        )


class Counters(DashboardComponent):
    def __init__(self, server, sizing_mode="stretch_both", **kwargs):
        self.server = server
        self.counter_figures = {}
        self.counter_sources = {}
        self.digest_figures = {}
        self.digest_sources = {}
        self.sizing_mode = sizing_mode

        if self.server.digests:
            for name in self.server.digests:
                self.add_digest_figure(name)
        for name in self.server.counters:
            self.add_counter_figure(name)

        figures = merge(self.digest_figures, self.counter_figures)
        figures = [figures[k] for k in sorted(figures)]

        if len(figures) <= 5:
            self.root = column(figures, sizing_mode=sizing_mode)
        else:
            self.root = column(
                *(
                    row(*pair, sizing_mode=sizing_mode)
                    for pair in partition_all(2, figures)
                ),
                sizing_mode=sizing_mode,
            )

    @log_errors
    def add_digest_figure(self, name):
        n = len(self.server.digests[name].intervals)
        sources = {i: ColumnDataSource({"x": [], "y": []}) for i in range(n)}

        kwargs = {}
        if name.endswith("duration"):
            kwargs["x_axis_type"] = "datetime"

        fig = figure(
            title=name, tools="", height=150, sizing_mode=self.sizing_mode, **kwargs
        )
        fig.yaxis.visible = False
        fig.ygrid.visible = False
        if name.endswith("bandwidth") or name.endswith("bytes"):
            fig.xaxis[0].formatter = NumeralTickFormatter(format="0.0b")

        for i in range(n):
            alpha = 0.3 + 0.3 * (n - i) / n
            fig.line(
                source=sources[i],
                x="x",
                y="y",
                alpha=alpha,
                color=RdBu[max(n, 3)][-i],
            )

        fig.xaxis.major_label_orientation = math.pi / 12
        self.digest_sources[name] = sources
        self.digest_figures[name] = fig
        return fig

    @log_errors
    def add_counter_figure(self, name):
        n = len(self.server.counters[name].intervals)
        sources = {
            i: ColumnDataSource({"x": [], "y": [], "y-center": [], "counts": []})
            for i in range(n)
        }

        fig = figure(
            title=name,
            tools="",
            height=150,
            sizing_mode=self.sizing_mode,
            x_range=sorted(str(x) for x in self.server.counters[name].components[0]),
        )
        fig.ygrid.visible = False

        for i in range(n):
            width = 0.5 + 0.4 * i / n
            fig.rect(
                source=sources[i],
                x="x",
                y="y-center",
                width=width,
                height="y",
                alpha=0.3,
                color=RdBu[max(n, 3)][-i],
            )
            hover = HoverTool(point_policy="follow_mouse", tooltips="""@x : @counts""")
            fig.add_tools(hover)
            fig.xaxis.major_label_orientation = math.pi / 12

        self.counter_sources[name] = sources
        self.counter_figures[name] = fig
        return fig

    @without_property_validation
    @log_errors
    def update(self):
        for name, fig in self.digest_figures.items():
            digest = self.server.digests[name]
            d = {}
            for i, d in enumerate(digest.components):
                if d.size():
                    ys, xs = d.histogram(100)
                    xs = xs[1:]
                    if name.endswith("duration"):
                        xs *= 1000
                    self.digest_sources[name][i].data.update({"x": xs, "y": ys})
            fig.title.text = "%s: %d" % (name, digest.size())

        for name, fig in self.counter_figures.items():
            counter = self.server.counters[name]
            d = {}
            for i, d in enumerate(counter.components):
                if d:
                    xs = sorted(d)
                    factor = counter.intervals[0] / counter.intervals[i]
                    counts = [d[x] for x in xs]
                    ys = [factor * c for c in counts]
                    y_centers = [y / 2 for y in ys]
                    xs = [str(x) for x in xs]
                    d = {"x": xs, "y": ys, "y-center": y_centers, "counts": counts}
                    self.counter_sources[name][i].data.update(d)
                fig.title.text = "%s: %d" % (name, counter.size())
                fig.x_range.factors = [str(x) for x in xs]


@standard_doc("Dask Worker Internal Monitor", active_page="status")
def status_doc(worker, extra, doc):
    statetable = StateTable(worker)
    executing_ts = ExecutingTimeSeries(worker, sizing_mode="scale_width")
    communicating_ts = CommunicatingTimeSeries(worker, sizing_mode="scale_width")
    communicating_stream = CommunicatingStream(worker, sizing_mode="scale_width")

    xr = executing_ts.root.x_range
    communicating_ts.root.x_range = xr
    communicating_stream.root.x_range = xr

    add_periodic_callback(doc, statetable, 200)
    add_periodic_callback(doc, executing_ts, 200)
    add_periodic_callback(doc, communicating_ts, 200)
    add_periodic_callback(doc, communicating_stream, 200)
    doc.add_root(
        column(
            statetable.root,
            executing_ts.root,
            communicating_ts.root,
            communicating_stream.root,
            sizing_mode="scale_width",
        )
    )


@standard_doc("Dask Worker Monitor", active_page="system")
def systemmonitor_doc(worker, extra, doc):
    sysmon = SystemMonitor(worker, sizing_mode="scale_width")
    add_periodic_callback(doc, sysmon, 500)
    doc.add_root(sysmon.root)


@standard_doc("Dask Work Counters", active_page="counters")
def counters_doc(server, extra, doc):
    counter = Counters(server, sizing_mode="stretch_both")
    add_periodic_callback(doc, counter, 500)
    doc.add_root(counter.root)


@standard_doc("Dask Worker Profile", active_page="profile")
def profile_doc(server, extra, doc):
    profile = ProfileTimePlot(server, sizing_mode="stretch_both", doc=doc)
    doc.add_root(profile.root)
    profile.trigger_update()


@standard_doc("Dask: Profile of Event Loop", active_page=None)
def profile_server_doc(server, extra, doc):
    profile = ProfileServer(server, sizing_mode="stretch_both", doc=doc)
    doc.add_root(profile.root)
    profile.trigger_update()
