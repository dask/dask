from __future__ import annotations

import math

from bokeh.core.properties import without_property_validation
from bokeh.models import (
    BasicTicker,
    ColumnDataSource,
    HoverTool,
    NumeralTickFormatter,
    OpenURL,
    TapTool,
)
from bokeh.plotting import figure

from dask.utils import format_bytes

from distributed.dashboard.components import DashboardComponent, add_periodic_callback
from distributed.dashboard.components.scheduler import BOKEH_THEME, TICKS_1024, env
from distributed.dashboard.utils import update
from distributed.utils import log_errors, url_escape


class GPUCurrentLoad(DashboardComponent):
    """How many tasks are on each worker"""

    @log_errors
    def __init__(self, scheduler, width=600, **kwargs):
        self.last = 0
        self.scheduler = scheduler
        self.source = ColumnDataSource(
            {
                "memory": [1, 2],
                "memory-half": [0.5, 1],
                "memory_text": ["1B", "2B"],
                "utilization": [1, 2],
                "utilization-half": [0.5, 1],
                "worker": ["a", "b"],
                "gpu-index": [0, 0],
                "y": [1, 2],
                "escaped_worker": ["a", "b"],
            }
        )

        memory = figure(
            title="GPU Memory",
            tools="",
            width=int(width / 2),
            name="gpu_memory_histogram",
            **kwargs,
        )
        rect = memory.rect(
            source=self.source,
            x="memory-half",
            y="y",
            width="memory",
            height=1,
            color="#76B900",
        )
        rect.nonselection_glyph = None

        utilization = figure(
            title="GPU Utilization",
            tools="",
            width=int(width / 2),
            name="gpu_utilization_histogram",
            **kwargs,
        )
        rect = utilization.rect(
            source=self.source,
            x="utilization-half",
            y="y",
            width="utilization",
            height=1,
            color="#76B900",
        )
        rect.nonselection_glyph = None

        memory.axis[0].ticker = BasicTicker(**TICKS_1024)
        memory.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
        memory.xaxis.major_label_orientation = -math.pi / 12
        memory.x_range.start = 0

        for fig in [memory, utilization]:
            fig.xaxis.minor_tick_line_alpha = 0
            fig.yaxis.visible = False
            fig.ygrid.visible = False

            tap = TapTool(callback=OpenURL(url="./info/worker/@escaped_worker.html"))
            fig.add_tools(tap)

            fig.toolbar_location = None
            fig.yaxis.visible = False

        hover = HoverTool()
        hover.tooltips = "@worker : @utilization %"
        hover.point_policy = "follow_mouse"
        utilization.add_tools(hover)

        hover = HoverTool()
        hover.tooltips = "@worker : @memory_text"
        hover.point_policy = "follow_mouse"
        memory.add_tools(hover)

        self.memory_figure = memory
        self.utilization_figure = utilization

        self.utilization_figure.y_range = memory.y_range
        self.utilization_figure.x_range.start = 0
        self.utilization_figure.x_range.end = 100

    @without_property_validation
    @log_errors
    def update(self):
        workers = list(self.scheduler.workers.values())

        utilization = []
        memory = []
        gpu_index = []
        y = []
        memory_total = 0
        memory_max = 0
        worker = []

        for idx, ws in enumerate(workers):
            try:
                mem_used = ws.metrics["gpu_memory_used"]
                mem_total = ws.metrics["gpu-memory-total"]
                u = ws.metrics["gpu_utilization"]
            except KeyError:
                continue
            memory_max = max(memory_max, mem_total)
            memory_total += mem_total
            utilization.append(int(u) if u else 0)
            memory.append(mem_used)
            worker.append(ws.address)
            gpu_index.append(idx)
            y.append(idx)

        memory_text = [format_bytes(m) for m in memory]

        result = {
            "memory": memory,
            "memory-half": [m / 2 for m in memory],
            "memory_text": memory_text,
            "utilization": utilization,
            "utilization-half": [u / 2 for u in utilization],
            "worker": worker,
            "gpu-index": gpu_index,
            "y": y,
            "escaped_worker": [url_escape(w) for w in worker],
        }

        self.memory_figure.title.text = "GPU Memory: {} / {}".format(
            format_bytes(sum(memory)),
            format_bytes(memory_total),
        )
        self.memory_figure.x_range.end = memory_max

        update(self.source, result)


@log_errors
def gpu_memory_doc(scheduler, extra, doc):
    gpu_load = GPUCurrentLoad(scheduler, sizing_mode="stretch_both")
    gpu_load.update()
    add_periodic_callback(doc, gpu_load, 100)
    doc.add_root(gpu_load.memory_figure)
    doc.theme = BOKEH_THEME


@log_errors
def gpu_utilization_doc(scheduler, extra, doc):
    gpu_load = GPUCurrentLoad(scheduler, sizing_mode="stretch_both")
    gpu_load.update()
    add_periodic_callback(doc, gpu_load, 100)
    doc.add_root(gpu_load.utilization_figure)
    doc.theme = BOKEH_THEME


@log_errors
def gpu_doc(scheduler, extra, doc):
    gpu_load = GPUCurrentLoad(scheduler, sizing_mode="stretch_both")
    gpu_load.update()
    add_periodic_callback(doc, gpu_load, 100)
    doc.add_root(gpu_load.memory_figure)
    doc.add_root(gpu_load.utilization_figure)

    doc.title = "Dask: GPU"
    doc.theme = BOKEH_THEME
    doc.template = env.get_template("gpu.html")
    doc.template_variables.update(extra)
