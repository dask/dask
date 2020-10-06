import math

from distributed.dashboard.components import DashboardComponent, add_periodic_callback

from bokeh.plotting import figure
from bokeh.models import (
    ColumnDataSource,
    BasicTicker,
    NumeralTickFormatter,
    TapTool,
    OpenURL,
    HoverTool,
)
from tornado import escape
from dask.utils import format_bytes
from distributed.utils import log_errors
from distributed.dashboard.components.scheduler import BOKEH_THEME, TICKS_1024
from distributed.dashboard.utils import without_property_validation, update


try:
    import pynvml

    pynvml.nvmlInit()
except Exception:
    pass


class GPUCurrentLoad(DashboardComponent):
    """ How many tasks are on each worker """

    def __init__(self, scheduler, width=600, **kwargs):
        with log_errors():
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
                id="bk-gpu-memory-worker-plot",
                width=int(width / 2),
                name="gpu_memory_histogram",
                **kwargs
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
                id="bk-gpu-utilization-worker-plot",
                width=int(width / 2),
                name="gpu_utilization_histogram",
                **kwargs
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

                tap = TapTool(
                    callback=OpenURL(url="./info/worker/@escaped_worker.html")
                )
                fig.add_tools(tap)

                fig.toolbar.logo = None
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
    def update(self):
        with log_errors():
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
                    info = ws.extra["gpu"]
                except KeyError:
                    continue
                metrics = ws.metrics["gpu"]
                u = metrics["utilization"]
                mem_used = metrics["memory-used"]
                mem_total = info["memory-total"]
                memory_max = max(memory_max, mem_total)
                memory_total += mem_total
                utilization.append(int(u))
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
                "escaped_worker": [escape.url_escape(w) for w in worker],
            }

            self.memory_figure.title.text = "GPU Memory: %s / %s" % (
                format_bytes(sum(memory)),
                format_bytes(memory_total),
            )
            self.memory_figure.x_range.end = memory_max

            update(self.source, result)


def gpu_memory_doc(scheduler, extra, doc):
    gpu_load = GPUCurrentLoad(scheduler, sizing_mode="stretch_both")
    gpu_load.update()
    add_periodic_callback(doc, gpu_load, 100)
    doc.add_root(gpu_load.memory_figure)
    doc.theme = BOKEH_THEME


def gpu_utilization_doc(scheduler, extra, doc):
    gpu_load = GPUCurrentLoad(scheduler, sizing_mode="stretch_both")
    gpu_load.update()
    add_periodic_callback(doc, gpu_load, 100)
    doc.add_root(gpu_load.utilization_figure)
    doc.theme = BOKEH_THEME
