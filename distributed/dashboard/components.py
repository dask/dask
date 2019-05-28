from __future__ import print_function, division, absolute_import

from bisect import bisect
from operator import add
from time import time
import weakref

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource,
    Plot,
    DataRange1d,
    LinearAxis,
    HoverTool,
    BoxZoomTool,
    ResetTool,
    PanTool,
    WheelZoomTool,
    Range1d,
    Quad,
    TapTool,
    OpenURL,
    Button,
    Select,
)
from bokeh.palettes import Spectral9
from bokeh.plotting import figure
import dask
from tornado import gen
import toolz

from .utils import without_property_validation, BOKEH_VERSION
from ..diagnostics.progress_stream import nbytes_bar
from .. import profile
from ..utils import log_errors, parse_timedelta

if dask.config.get("distributed.dashboard.export-tool"):
    from .export_tool import ExportTool
else:
    ExportTool = None


profile_interval = dask.config.get("distributed.worker.profile.interval")
profile_interval = parse_timedelta(profile_interval, default="ms")


class DashboardComponent(object):
    """ Base class for Dask.distributed UI dashboard components.

    This class must have two attributes, ``root`` and ``source``, and one
    method ``update``:

    *  source: a Bokeh ColumnDataSource
    *  root: a Bokeh Model
    *  update: a method that consumes the messages dictionary found in
               distributed.bokeh.messages
    """

    def __init__(self):
        self.source = None
        self.root = None

    def update(self, messages):
        """ Reads from bokeh.distributed.messages and updates self.source """


class TaskStream(DashboardComponent):
    """ Task Stream

    The start and stop time of tasks as they occur on each core of the cluster.
    """

    def __init__(self, n_rectangles=1000, clear_interval="20s", **kwargs):
        """
        kwargs are applied to the bokeh.models.plots.Plot constructor
        """
        self.n_rectangles = n_rectangles
        clear_interval = parse_timedelta(clear_interval, default="ms")
        self.clear_interval = clear_interval
        self.last = 0

        self.source, self.root = task_stream_figure(clear_interval, **kwargs)

        # Required for update callback
        self.task_stream_index = [0]

    @without_property_validation
    def update(self, messages):
        with log_errors():
            index = messages["task-events"]["index"]
            rectangles = messages["task-events"]["rectangles"]

            if not index or index[-1] == self.task_stream_index[0]:
                return

            ind = bisect(index, self.task_stream_index[0])
            rectangles = {
                k: [v[i] for i in range(ind, len(index))] for k, v in rectangles.items()
            }
            self.task_stream_index[0] = index[-1]

            # If there has been a significant delay then clear old rectangles
            if rectangles["start"]:
                m = min(map(add, rectangles["start"], rectangles["duration"]))
                if m > self.last:
                    self.last, last = m, self.last
                    if m > last + self.clear_interval:
                        self.source.data.update(rectangles)
                        return

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
        **kwargs
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

    tap = TapTool(callback=OpenURL(url="/profile?key=@name"))

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


class MemoryUsage(DashboardComponent):
    """ The memory usage across the cluster, grouped by task type """

    def __init__(self, **kwargs):
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
    def update(self, messages):
        with log_errors():
            msg = messages["progress"]
            if not msg:
                return
            nb = nbytes_bar(msg["nbytes"])
            self.source.data.update(nb)
            self.root.title.text = "Memory Use: %0.2f MB" % (
                sum(msg["nbytes"].values()) / 1e6
            )


class Processing(DashboardComponent):
    """ Processing and distribution per core

    This shows how many tasks are actively running on each worker and how many
    tasks are enqueued for each worker and how many are in the common pool
    """

    def __init__(self, **kwargs):
        data = self.processing_update({"processing": {}, "ncores": {}})
        self.source = ColumnDataSource(data)

        x_range = Range1d(-1, 1)
        fig = figure(
            title="Processing and Pending",
            tools="",
            x_range=x_range,
            id="bk-processing-stacks-plot",
            **kwargs
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
    def update(self, messages):
        with log_errors():
            msg = messages["processing"]
            if not msg.get("ncores"):
                return
            data = self.processing_update(msg)
            x_range = self.root.x_range
            max_right = max(data["right"])
            cores = max(data["ncores"])
            if x_range.end < max_right:
                x_range.end = max_right + 2
            elif x_range.end > 2 * max_right + cores:  # way out there, walk back
                x_range.end = x_range.end * 0.95 + max_right * 0.05

            self.source.data.update(data)

    @staticmethod
    def processing_update(msg):
        with log_errors():
            names = sorted(msg["processing"])
            names = sorted(names)
            processing = msg["processing"]
            processing = [processing[name] for name in names]
            ncores = msg["ncores"]
            ncores = [ncores[name] for name in names]
            n = len(names)
            d = {
                "name": list(names),
                "processing": processing,
                "right": list(processing),
                "top": list(range(n, 0, -1)),
                "bottom": list(range(n - 1, -1, -1)),
                "ncores": ncores,
            }

            d["alpha"] = [0.7] * n

            return d


class ProfilePlot(DashboardComponent):
    """ Time plots of the current resource usage on the cluster

    This is two plots, one for CPU and Memory and another for Network I/O
    """

    def __init__(self, **kwargs):
        state = profile.create()
        data = profile.plot_data(state, profile_interval)
        self.states = data.pop("states")
        self.root, self.source = profile.plot_figure(data, **kwargs)

        @without_property_validation
        def cb(attr, old, new):
            with log_errors():
                try:
                    selected = new.indices
                except AttributeError:
                    selected = new["1d"]["indices"]
                try:
                    ind = selected[0]
                except IndexError:
                    return
                data = profile.plot_data(self.states[ind], profile_interval)
                del self.states[:]
                self.states.extend(data.pop("states"))
                self.source.data.update(data)
                self.source.selected = old

        if BOKEH_VERSION >= "1.0.0":
            self.source.selected.on_change("indices", cb)
        else:
            self.source.on_change("selected", cb)

    @without_property_validation
    def update(self, state):
        with log_errors():
            self.state = state
            data = profile.plot_data(self.state, profile_interval)
            self.states = data.pop("states")
            self.source.data.update(data)


class ProfileTimePlot(DashboardComponent):
    """ Time plots of the current resource usage on the cluster

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
            self.task_names = ["All", self.key]
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
        def cb(attr, old, new):
            if changing[0]:
                return
            with log_errors():
                if isinstance(new, list):  # bokeh >= 1.0
                    selected = new
                else:
                    selected = new["1d"]["indices"]
                try:
                    ind = selected[0]
                except IndexError:
                    return
                data = profile.plot_data(self.states[ind], profile_interval)
                del self.states[:]
                self.states.extend(data.pop("states"))
                changing[0] = True  # don't recursively trigger callback
                self.source.data.update(data)
                if isinstance(new, list):  # bokeh >= 1.0
                    self.source.selected.indices = old
                else:
                    self.source.selected = old
                changing[0] = False

        if BOKEH_VERSION >= "1.0.0":
            self.source.selected.on_change("indices", cb)
        else:
            self.source.on_change("selected", cb)

        self.ts_source = ColumnDataSource({"time": [], "count": []})
        self.ts_plot = figure(
            title="Activity over time",
            height=100,
            x_axis_type="datetime",
            active_drag="xbox_select",
            y_range=[0, 1 / profile_interval],
            tools="xpan,xwheel_zoom,xbox_select,reset",
            **kwargs
        )
        self.ts_plot.line("time", "count", source=self.ts_source)
        self.ts_plot.circle(
            "time", "count", source=self.ts_source, color=None, selection_color="orange"
        )
        self.ts_plot.yaxis.visible = False
        self.ts_plot.grid.visible = False

        def ts_change(attr, old, new):
            with log_errors():
                try:
                    selected = self.ts_source.selected.indices
                except AttributeError:
                    selected = self.ts_source.selected["1d"]["indices"]
                if selected:
                    start = self.ts_source.data["time"][min(selected)] / 1000
                    stop = self.ts_source.data["time"][max(selected)] / 1000
                    self.start, self.stop = min(start, stop), max(start, stop)
                else:
                    self.start = self.stop = None
                self.trigger_update(update_metadata=False)

        if BOKEH_VERSION >= "1.0.0":
            self.ts_source.selected.on_change("indices", ts_change)
        else:
            self.ts_source.on_change("selected", ts_change)

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
            ),
            self.profile_plot,
            self.ts_plot,
            **kwargs
        )

    @without_property_validation
    def update(self, state, metadata=None):
        with log_errors():
            self.state = state
            data = profile.plot_data(self.state, profile_interval)
            self.states = data.pop("states")
            self.source.data.update(data)

            if metadata is not None and metadata["counts"]:
                self.task_names = ["All"] + sorted(metadata["keys"])
                self.select.options = self.task_names
                if self.key:
                    ts = metadata["keys"][self.key]
                else:
                    ts = metadata["counts"]
                times, counts = zip(*ts)
                self.ts = {"count": counts, "time": [t * 1000 for t in times]}

                self.ts_source.data.update(self.ts)

    @without_property_validation
    def trigger_update(self, update_metadata=True):
        @gen.coroutine
        def cb():
            with log_errors():
                prof = self.server.get_profile(
                    key=self.key, start=self.start, stop=self.stop
                )
                if update_metadata:
                    metadata = self.server.get_profile_metadata()
                else:
                    metadata = None
                if isinstance(prof, gen.Future):
                    prof, metadata = yield [prof, metadata]
                self.doc().add_next_tick_callback(lambda: self.update(prof, metadata))

        self.server.loop.add_callback(cb)


class ProfileServer(DashboardComponent):
    """ Time plots of the current resource usage on the cluster

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
        def cb(attr, old, new):
            if changing[0]:
                return
            with log_errors():
                if isinstance(new, list):  # bokeh >= 1.0
                    selected = new
                else:
                    selected = new["1d"]["indices"]
                try:
                    ind = selected[0]
                except IndexError:
                    return
                data = profile.plot_data(self.states[ind], profile_interval)
                del self.states[:]
                self.states.extend(data.pop("states"))
                changing[0] = True  # don't recursively trigger callback
                self.source.data.update(data)
                if isinstance(new, list):  # bokeh >= 1.0
                    self.source.selected.indices = old
                else:
                    self.source.selected = old
                changing[0] = False

        if BOKEH_VERSION >= "1.0.0":
            self.source.selected.on_change("indices", cb)
        else:
            self.source.on_change("selected", cb)

        self.ts_source = ColumnDataSource({"time": [], "count": []})
        self.ts_plot = figure(
            title="Activity over time",
            height=100,
            x_axis_type="datetime",
            active_drag="xbox_select",
            y_range=[0, 1 / profile_interval],
            tools="xpan,xwheel_zoom,xbox_select,reset",
            **kwargs
        )
        self.ts_plot.line("time", "count", source=self.ts_source)
        self.ts_plot.circle(
            "time", "count", source=self.ts_source, color=None, selection_color="orange"
        )
        self.ts_plot.yaxis.visible = False
        self.ts_plot.grid.visible = False

        def ts_change(attr, old, new):
            with log_errors():
                try:
                    selected = self.ts_source.selected.indices
                except AttributeError:
                    selected = self.ts_source.selected["1d"]["indices"]
                if selected:
                    start = self.ts_source.data["time"][min(selected)] / 1000
                    stop = self.ts_source.data["time"][max(selected)] / 1000
                    self.start, self.stop = min(start, stop), max(start, stop)
                else:
                    self.start = self.stop = None
                self.trigger_update()

        if BOKEH_VERSION >= "1.0.0":
            self.ts_source.selected.on_change("indices", ts_change)
        else:
            self.ts_source.on_change("selected", ts_change)

        self.reset_button = Button(label="Reset", button_type="success")
        self.reset_button.on_click(lambda: self.update(self.state))

        self.update_button = Button(label="Update", button_type="success")
        self.update_button.on_click(self.trigger_update)

        self.root = column(
            row(self.reset_button, self.update_button, sizing_mode="scale_width"),
            self.profile_plot,
            self.ts_plot,
            **kwargs
        )

    @without_property_validation
    def update(self, state):
        with log_errors():
            self.state = state
            data = profile.plot_data(self.state, profile_interval)
            self.states = data.pop("states")
            self.source.data.update(data)

    @without_property_validation
    def trigger_update(self):
        self.state = profile.get_profile(self.log, start=self.start, stop=self.stop)
        data = profile.plot_data(self.state, profile_interval)
        self.states = data.pop("states")
        self.source.data.update(data)
        times = [t * 1000 for t, _ in self.log]
        counts = list(toolz.pluck("count", toolz.pluck(1, self.log)))
        self.ts_source.data.update({"time": times, "count": counts})


def add_periodic_callback(doc, component, interval):
    """ Add periodic callback to doc in a way that avoids reference cycles

    If we instead use ``doc.add_periodic_callback(component.update, 100)`` then
    the component stays in memory as a reference cycle because its method is
    still around.  This way we avoid that and let things clean up a bit more
    nicely.

    TODO: we still have reference cycles.  Docs seem to be referred to by their
    add_periodic_callback methods.
    """
    ref = weakref.ref(component)

    doc.add_periodic_callback(lambda: update(ref), interval)
    _attach(doc, component)


def update(ref):
    comp = ref()
    if comp is not None:
        comp.update()


def _attach(doc, component):
    if not hasattr(doc, "components"):
        doc.components = set()

    doc.components.add(component)
