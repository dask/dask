from __future__ import annotations

from collections.abc import Iterable
from typing import TypeVar

from bokeh.core.properties import without_property_validation
from bokeh.models import (
    BasicTicker,
    ColumnDataSource,
    HoverTool,
    NumeralTickFormatter,
    OpenURL,
    Range1d,
    TapTool,
)
from bokeh.plotting import figure

from dask.utils import format_bytes

from distributed.dashboard.components import DashboardComponent, add_periodic_callback
from distributed.dashboard.components.scheduler import (
    BOKEH_THEME,
    TICKS_1024,
    XLABEL_ORIENTATION,
    MemoryColor,
)
from distributed.dashboard.utils import update
from distributed.utils import log_errors, url_escape

T = TypeVar("T")


class RMMMemoryUsage(DashboardComponent, MemoryColor):
    """
    GPU memory usage plot that includes information about memory
    managed by RMM. If an RMM pool is being used, shows the amount of
    pool memory utilized.
    """

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        DashboardComponent.__init__(self)
        MemoryColor.__init__(self, neutral_color="#76B900")

        self.last = 0
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
                "rmm_used": [],
                "rmm_total": [],
                "gpu_used": [],
                "gpu_total": [],
                "spilled": [],
            }
        )

        self.root = figure(
            title="RMM memory used",
            tools="",
            width=int(width / 2),
            name="rmm_memory",
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
                <span style="font-size: 12px; font-weight: bold;">RMM memory used:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@rmm_used{0.00 b} / @rmm_total{0.00 b}</span>
            </div>
            <div>
                <span style="font-size: 12px; font-weight: bold;">GPU memory used:</span>&nbsp;
                <span style="font-size: 10px; font-family: Monaco, monospace;">@gpu_used{0.00 b} / @gpu_total{0.00 b}</span>
            </div>
            <div>
                <span style="font-size: 12px; font-weight: bold;">Spilled to CPU:</span>&nbsp;
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

        workers = list(self.scheduler.workers.values())

        width = []
        x = []
        color = []
        max_limit = 0
        rmm_used = []
        rmm_total = []
        gpu_used = []
        gpu_total = []
        spilled = []

        for ws in workers:
            try:
                rmm_metrics = ws.metrics["rmm"]
                gpu_metrics = ws.metrics["gpu"]
                gpu_info = ws.extra["gpu"]
            except KeyError:
                rmm_metrics = {"rmm-used": 0, "rmm-total": 0}
                gpu_metrics = {"memory-used": 0}
                gpu_info = {"memory-total": 0}

            try:
                cudf_metrics = ws.metrics["cudf"]
            except KeyError:
                cudf_metrics = {"cudf-spilled": 0}

            rmm_used_worker = rmm_metrics["rmm-used"]  # RMM memory only
            rmm_total_worker = rmm_metrics["rmm-total"]
            gpu_used_worker = gpu_metrics["memory-used"]  # All GPU memory
            gpu_total_worker = gpu_info["memory-total"]
            spilled_worker = cudf_metrics["cudf-spilled"] or 0  # memory spilled to host

            max_limit = max(
                max_limit, gpu_total_worker, gpu_used_worker + spilled_worker
            )
            color_i = self._memory_color(gpu_used_worker, gpu_total_worker, ws.status)

            width += [
                rmm_used_worker,
                rmm_total_worker - rmm_used_worker,
                gpu_used_worker - rmm_total_worker,
                spilled_worker,
            ]
            x += [sum(width[-4:i]) + width[i] / 2 for i in range(-4, 0)]
            color += [color_i, color_i, color_i, "grey"]

            # memory info
            rmm_used.append(rmm_used_worker)
            rmm_total.append(rmm_total_worker)
            gpu_used.append(gpu_used_worker)
            gpu_total.append(gpu_total_worker)
            spilled.append(spilled_worker)

        title = f"RMM memory used: {format_bytes(sum(rmm_used))} / {format_bytes(sum(rmm_total))}\nGPU memory used: {format_bytes(sum(gpu_used))} / {format_bytes(sum(gpu_total))}"
        if sum(spilled):
            title += f" + {format_bytes(sum(spilled))} spilled to CPU"
        self.root.title.text = title

        result = {
            "width": width,
            "x": x,
            "y": quadlist(range(len(workers))),
            "color": color,
            "alpha": [1, 0.7, 0.4, 1] * len(workers),
            "worker": quadlist(ws.address for ws in workers),
            "escaped_worker": quadlist(url_escape(ws.address) for ws in workers),
            "rmm_used": quadlist(rmm_used),
            "rmm_total": quadlist(rmm_total),
            "gpu_used": quadlist(gpu_used),
            "gpu_total": quadlist(gpu_total),
            "spilled": quadlist(spilled),
        }

        self.root.x_range.end = max_limit
        update(self.source, result)


@log_errors
def rmm_memory_doc(scheduler, extra, doc):
    rmm_load = RMMMemoryUsage(scheduler, sizing_mode="stretch_both")
    rmm_load.update()
    add_periodic_callback(doc, rmm_load, 100)
    doc.add_root(rmm_load.root)
    doc.theme = BOKEH_THEME
