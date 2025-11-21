from __future__ import annotations

import asyncio
import sys
import weakref

import tlz as toolz
from bokeh.core.properties import without_property_validation
from bokeh.layouts import column, row
from bokeh.models import (
    Button,
    ColumnDataSource,
    DataRange1d,
    HoverTool,
    LabelSet,
    NumeralTickFormatter,
    Range1d,
    Select,
    Title,
)
from bokeh.palettes import Spectral9
from bokeh.plotting import figure
from tornado import gen

import dask

from distributed import profile
from distributed.compatibility import WINDOWS
from distributed.dashboard.components import DashboardComponent
from distributed.dashboard.utils import update
from distributed.utils import log_errors

if dask.config.get("distributed.dashboard.export-tool"):
    from distributed.dashboard.export_tool import ExportTool
else:
    ExportTool = None  # type: ignore


profile_interval = dask.config.get("distributed.worker.profile.interval")
profile_interval = dask.utils.parse_timedelta(profile_interval, default="ms")


class Processing(DashboardComponent):
    """Processing and distribution per core

    This shows how many tasks are actively running on each worker and how many
    tasks are enqueued for each worker and how many are in the common pool
    """

    def __init__(self, **kwargs):
        data = self.processing_update({"processing": {}, "nthreads": {}})
        self.source = ColumnDataSource(data)

        x_range = Range1d(-1, 1)
        fig = figure(
            title="Processing and Pending",
            tools="",
            x_range=x_range,
            **kwargs,
        )
        fig.quad(
            source=self.source,
            left=0,
            right="right",
            color=Spectral9[0],
            top="top",
            bottom="bottom",
        )

        fig.xaxis.minor_tick_line_alpha = 0
        fig.yaxis.visible = False
        fig.ygrid.visible = False

        hover = HoverTool()
        fig.add_tools(hover)
        hover = fig.select(HoverTool)
        hover.tooltips = """
        <div>
            <span style="font-size: 14px; font-weight: bold;">Host:</span>&nbsp;
            <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
        </div>
        <div>
            <span style="font-size: 14px; font-weight: bold;">Processing:</span>&nbsp;
            <span style="font-size: 10px; font-family: Monaco, monospace;">@processing</span>
        </div>
        """
        hover.point_policy = "follow_mouse"

        self.root = fig

    @without_property_validation
    @log_errors
    def update(self, messages):
        msg = messages["processing"]
        if not msg.get("nthreads"):
            return
        data = self.processing_update(msg)
        x_range = self.root.x_range
        max_right = max(data["right"])
        cores = max(data["nthreads"])
        if x_range.end < max_right:
            x_range.end = max_right + 2
        elif x_range.end > 2 * max_right + cores:  # way out there, walk back
            x_range.end = x_range.end * 0.95 + max_right * 0.05

        update(self.source, data)

    @staticmethod
    @log_errors
    def processing_update(msg):
        names = sorted(msg["processing"])
        names = sorted(names)
        processing = msg["processing"]
        processing = [processing[name] for name in names]
        nthreads = msg["nthreads"]
        nthreads = [nthreads[name] for name in names]
        n = len(names)
        d = {
            "name": list(names),
            "processing": processing,
            "right": list(processing),
            "top": list(range(n, 0, -1)),
            "bottom": list(range(n - 1, -1, -1)),
            "nthreads": nthreads,
        }

        d["alpha"] = [0.7] * n

        return d


class ProfilePlot(DashboardComponent):
    """Time plots of the current resource usage on the cluster

    This is two plots, one for CPU and Memory and another for Network I/O
    """

    def __init__(self, **kwargs):
        state = profile.create()
        data = profile.plot_data(state, profile_interval)
        self.states = data.pop("states")
        self.root, self.source = profile.plot_figure(data, **kwargs)

        @without_property_validation
        @log_errors
        def cb(attr, old, new):
            try:
                ind = new.indices[0]
            except IndexError:
                return
            data = profile.plot_data(self.states[ind], profile_interval)
            del self.states[:]
            self.states.extend(data.pop("states"))
            update(self.source, data)
            self.source.selected = old

        self.source.selected.on_change("indices", cb)

    @without_property_validation
    @log_errors
    def update(self, state):
        self.state = state
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        update(self.source, data)


class ProfileTimePlot(DashboardComponent):
    """Time plots of the current resource usage on the cluster

    This is two plots, one for CPU and Memory and another for Network I/O
    """

    def __init__(self, server, doc=None, **kwargs):
        if doc is not None:
            self.doc = weakref.ref(doc)
            try:
                self.key = doc.session_context.request.arguments.get("key", None)
            except AttributeError:
                self.key = None
            if isinstance(self.key, list):
                self.key = self.key[0]
            if isinstance(self.key, bytes):
                self.key = self.key.decode()
            self.task_names = ["All", self.key] if self.key else ["All"]
        else:
            self.key = None
            self.task_names = ["All"]

        self.server = server
        self.start = None
        self.stop = None
        self.ts = {"count": [], "time": []}
        self.state = profile.create()
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        self.profile_plot, self.source = profile.plot_figure(data, **kwargs)
        changing = [False]  # avoid repeated changes from within callback

        @without_property_validation
        @log_errors
        def cb(attr, old, new):
            if changing[0] or len(new) == 0:
                return

            data = profile.plot_data(self.states[new[0]], profile_interval)
            del self.states[:]
            self.states.extend(data.pop("states"))
            changing[0] = True  # don't recursively trigger callback
            update(self.source, data)
            self.source.selected.indices = old
            changing[0] = False

        self.source.selected.on_change("indices", cb)

        self.ts_source = ColumnDataSource({"time": [], "count": []})
        self.ts_plot = figure(
            title="Activity over time",
            height=150,
            x_axis_type="datetime",
            active_drag="xbox_select",
            tools="xpan,xwheel_zoom,xbox_select,reset",
            sizing_mode="stretch_width",
            toolbar_location="above",
        )
        self.ts_plot.line("time", "count", source=self.ts_source)
        self.ts_plot.circle(
            "time",
            "count",
            source=self.ts_source,
            color=None,
            selection_color="orange",
            radius=1,
        )
        self.ts_plot.yaxis.visible = False
        self.ts_plot.grid.visible = False

        @log_errors
        def ts_change(attr, old, new):
            selected = self.ts_source.selected.indices
            if selected:
                start = self.ts_source.data["time"][min(selected)] / 1000
                stop = self.ts_source.data["time"][max(selected)] / 1000
                self.start, self.stop = min(start, stop), max(start, stop)
            else:
                self.start = self.stop = None
            self.trigger_update(update_metadata=False)

        self.ts_source.selected.on_change("indices", ts_change)

        self.reset_button = Button(label="Reset", button_type="success")
        self.reset_button.on_click(lambda: self.update(self.state))

        self.update_button = Button(label="Update", button_type="success")
        self.update_button.on_click(self.trigger_update)

        self.select = Select(value=self.task_names[-1], options=self.task_names)

        def select_cb(attr, old, new):
            if new == "All":
                new = None
            self.key = new
            self.trigger_update(update_metadata=False)

        self.select.on_change("value", select_cb)

        self.root = column(
            row(
                self.select,
                self.reset_button,
                self.update_button,
                sizing_mode="scale_width",
                height=250,
            ),
            self.profile_plot,
            self.ts_plot,
            **kwargs,
        )

        self.subtitle = Title(text=" ", text_font_style="italic")
        self.profile_plot.add_layout(self.subtitle, "above")
        if not dask.config.get("distributed.worker.profile.enabled"):
            self.subtitle.text = "Profiling is disabled."

            self.select.disabled = True
            self.reset_button.disabled = True
            self.update_button.disabled = True
        elif sys.version_info.minor == 11:
            self.subtitle.text = "Profiling is disabled due to a known deadlock in CPython 3.11 that can be triggered by the profiler. See https://github.com/dask/distributed/issues/8616 for more information."
            self.select.disabled = True
            self.reset_button.disabled = True
            self.update_button.disabled = True

    @without_property_validation
    @log_errors
    def update(self, state, metadata=None):
        self.state = state
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        update(self.source, data)

        if metadata is not None and metadata["counts"]:
            self.task_names = ["All"] + sorted(metadata["keys"])
            self.select.options = self.task_names
            if self.key and self.key in metadata["keys"]:
                ts = metadata["keys"][self.key]
            else:
                ts = metadata["counts"]
            times, counts = zip(*ts)
            self.ts = {"count": counts, "time": [t * 1000 for t in times]}

            self.ts_source.data.update(self.ts)

    @without_property_validation
    def trigger_update(self, update_metadata=True):
        @log_errors
        async def cb():
            prof = await self.server.get_profile(
                key=self.key, start=self.start, stop=self.stop
            )
            if update_metadata:
                metadata = await self.server.get_profile_metadata()
            else:
                metadata = None
            if isinstance(prof, gen.Future):
                prof, metadata = await asyncio.gather(prof, metadata)
            self.doc().add_next_tick_callback(lambda: self.update(prof, metadata))

        self.server.loop.add_callback(cb)


class ProfileServer(DashboardComponent):
    """Time plots of the current resource usage on the cluster

    This is two plots, one for CPU and Memory and another for Network I/O
    """

    def __init__(self, server, doc=None, **kwargs):
        if doc is not None:
            self.doc = weakref.ref(doc)
        self.server = server
        self.log = self.server.io_loop.profile
        self.start = None
        self.stop = None
        self.ts = {"count": [], "time": []}
        self.state = profile.get_profile(self.log)
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        self.profile_plot, self.source = profile.plot_figure(data, **kwargs)

        changing = [False]  # avoid repeated changes from within callback

        @without_property_validation
        @log_errors
        def cb(attr, old, new):
            if changing[0] or len(new) == 0:
                return

            data = profile.plot_data(self.states[new[0]], profile_interval)
            del self.states[:]
            self.states.extend(data.pop("states"))
            changing[0] = True  # don't recursively trigger callback
            update(self.source, data)
            self.source.selected.indices = old
            changing[0] = False

        self.source.selected.on_change("indices", cb)

        self.ts_source = ColumnDataSource({"time": [], "count": []})
        self.ts_plot = figure(
            title="Activity over time",
            height=150,
            x_axis_type="datetime",
            active_drag="xbox_select",
            tools="xpan,xwheel_zoom,xbox_select,reset",
            sizing_mode="stretch_width",
            toolbar_location="above",
        )
        self.ts_plot.line("time", "count", source=self.ts_source)
        self.ts_plot.circle(
            "time",
            "count",
            source=self.ts_source,
            color=None,
            selection_color="orange",
            radius=1,
        )
        self.ts_plot.yaxis.visible = False
        self.ts_plot.grid.visible = False

        @log_errors
        def ts_change(attr, old, new):
            selected = self.ts_source.selected.indices
            if selected:
                start = self.ts_source.data["time"][min(selected)] / 1000
                stop = self.ts_source.data["time"][max(selected)] / 1000
                self.start, self.stop = min(start, stop), max(start, stop)
            else:
                self.start = self.stop = None
            self.trigger_update()

        self.ts_source.selected.on_change("indices", ts_change)

        self.reset_button = Button(label="Reset", button_type="success")
        self.reset_button.on_click(lambda: self.update(self.state))

        self.update_button = Button(label="Update", button_type="success")
        self.update_button.on_click(self.trigger_update)

        self.root = column(
            row(self.reset_button, self.update_button, sizing_mode="scale_width"),
            self.profile_plot,
            self.ts_plot,
            **kwargs,
        )

        self.subtitle = Title(text=" ", text_font_style="italic")
        self.profile_plot.add_layout(self.subtitle, "above")
        if not dask.config.get("distributed.worker.profile.enabled"):
            self.subtitle.text = "Profiling is disabled."
            self.reset_button.disabled = True
            self.update_button.disabled = True
        elif sys.version_info.minor == 11:
            self.subtitle.text = "Profiling is disabled due to a known deadlock in CPython 3.11 that can be triggered by the profiler. See https://github.com/dask/distributed/issues/8616 for more information."
            self.reset_button.disabled = True
            self.update_button.disabled = True

    @without_property_validation
    @log_errors
    def update(self, state):
        self.state = state
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        update(self.source, data)

    @without_property_validation
    def trigger_update(self):
        self.state = profile.get_profile(self.log, start=self.start, stop=self.stop)
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        update(self.source, data)
        times = [t * 1000 for t, _ in self.log]
        counts = list(toolz.pluck("count", toolz.pluck(1, self.log)))
        self.ts_source.data.update({"time": times, "count": counts})


class SystemMonitor(DashboardComponent):
    def __init__(self, worker, height=150, last_count=None, **kwargs):
        self.worker = worker

        names = worker.monitor.quantities
        self.last_count = 0
        if last_count is not None:
            names = worker.monitor.range_query(start=last_count)
            self.last_count = last_count
        self.source = ColumnDataSource({name: [] for name in names})
        self.label_source = ColumnDataSource(
            {
                "x": [5] * 3,
                "y": [70, 55, 40],
                "cpu": ["max: 45%", "min: 45%", "mean: 45%"],
                "memory": ["max: 133.5MiB", "min: 23.6MiB", "mean: 115.4MiB"],
            }
        )
        update(self.source, self.get_data())

        x_range = DataRange1d(follow="end", follow_interval=20000, range_padding=0)

        tools = "reset,xpan,xwheel_zoom"

        self.cpu = figure(
            title="CPU",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            toolbar_location="above",
            x_range=x_range,
            **kwargs,
        )
        self.cpu.line(source=self.source, x="time", y="cpu")
        self.cpu.yaxis.axis_label = "Percentage"
        self.cpu.add_layout(
            LabelSet(
                x="x",
                y="y",
                x_units="screen",
                y_units="screen",
                text="cpu",
                text_font_size="1em",
                source=self.label_source,
            )
        )
        self.mem = figure(
            title="Memory",
            x_axis_type="datetime",
            height=height,
            tools=tools,
            toolbar_location="above",
            x_range=x_range,
            **kwargs,
        )
        self.mem.line(source=self.source, x="time", y="memory")
        self.mem.yaxis.axis_label = "Bytes"
        self.mem.add_layout(
            LabelSet(
                x="x",
                y="y",
                x_units="screen",
                y_units="screen",
                text="memory",
                text_font_size="1em",
                source=self.label_source,
            )
        )
        self.bandwidth = figure(
            title="Bandwidth",
            x_axis_type="datetime",
            height=height,
            x_range=x_range,
            tools=tools,
            toolbar_location="above",
            **kwargs,
        )
        self.bandwidth.line(
            source=self.source,
            x="time",
            y="host_net_io.read_bps",
            color="red",
            legend_label="read",
        )
        self.bandwidth.line(
            source=self.source,
            x="time",
            y="host_net_io.write_bps",
            color="blue",
            legend_label="write",
        )
        self.bandwidth.yaxis.axis_label = "Bytes / second"

        # self.cpu.yaxis[0].formatter = NumeralTickFormatter(format='0%')
        self.bandwidth.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")
        self.mem.yaxis[0].formatter = NumeralTickFormatter(format="0.0b")

        plots = [self.cpu, self.mem, self.bandwidth]

        if not WINDOWS:
            self.num_fds = figure(
                title="Number of File Descriptors",
                x_axis_type="datetime",
                height=height,
                x_range=x_range,
                tools=tools,
                toolbar_location="above",
                **kwargs,
            )

            self.num_fds.line(source=self.source, x="time", y="num_fds")
            plots.append(self.num_fds)

        if "sizing_mode" in kwargs:
            kw = {"sizing_mode": kwargs["sizing_mode"]}
        else:
            kw = {}

        if not WINDOWS:
            self.num_fds.y_range.start = 0
        self.mem.y_range.start = 0
        self.cpu.y_range.start = 0
        self.bandwidth.y_range.start = 0

        self.root = column(*plots, **kw)
        self.worker.monitor.update()

    def get_data(self):
        d = self.worker.monitor.range_query(start=self.last_count)
        d["time"] = [x * 1000 for x in d["time"]]
        self.last_count = self.worker.monitor.count
        return d

    @without_property_validation
    @log_errors
    def update(self):
        def mean(x):
            return sum(x) / len(x)

        self.source.stream(self.get_data(), 1000)
        self.label_source.data["cpu"] = [
            "{}: {:.1f}%".format(f.__name__, f(self.source.data["cpu"]))
            for f in [min, max, mean]
        ]
        self.label_source.data["memory"] = [
            "{}: {}".format(
                f.__name__, dask.utils.format_bytes(f(self.source.data["memory"]))
            )
            for f in [min, max, mean]
        ]
