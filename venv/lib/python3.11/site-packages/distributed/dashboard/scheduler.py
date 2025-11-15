from __future__ import annotations

from urllib.parse import urljoin

from tlz import memoize
from tornado import web
from tornado.ioloop import IOLoop

from distributed.dashboard.components.nvml import (
    gpu_doc,
    gpu_memory_doc,
    gpu_utilization_doc,
)
from distributed.dashboard.components.rmm import rmm_memory_doc
from distributed.dashboard.components.scheduler import (
    AggregateAction,
    BandwidthTypes,
    BandwidthWorkers,
    ClusterMemory,
    ComputePerKey,
    Contention,
    CurrentLoad,
    ExceptionsTable,
    FinePerformanceMetrics,
    MemoryByKey,
    Occupancy,
    SystemMonitor,
    SystemTimeseries,
    TaskGraph,
    TaskGroupGraph,
    TaskGroupProgress,
    TaskProgress,
    TaskStream,
    WorkerNetworkBandwidth,
    WorkersMemory,
    WorkersTransferBytes,
    WorkerTable,
    events_doc,
    exceptions_doc,
    graph_doc,
    hardware_doc,
    individual_doc,
    individual_profile_doc,
    individual_profile_server_doc,
    profile_doc,
    profile_server_doc,
    shuffling_doc,
    status_doc,
    stealing_doc,
    systemmonitor_doc,
    tasks_doc,
    tg_graph_doc,
    workers_doc,
)
from distributed.dashboard.core import BokehApplication
from distributed.dashboard.worker import counters_doc

applications = {
    "/system": systemmonitor_doc,
    "/shuffle": shuffling_doc,
    "/stealing": stealing_doc,
    "/workers": workers_doc,
    "/exceptions": exceptions_doc,
    "/events": events_doc,
    "/counters": counters_doc,
    "/tasks": tasks_doc,
    "/status": status_doc,
    "/profile": profile_doc,
    "/profile-server": profile_server_doc,
    "/graph": graph_doc,
    "/hardware": hardware_doc,
    "/groups": tg_graph_doc,
    "/gpu": gpu_doc,
    "/individual-task-stream": individual_doc(
        TaskStream, 100, n_rectangles=1000, clear_interval="10s"
    ),
    "/individual-progress": individual_doc(TaskProgress, 100, height=160),
    "/individual-graph": individual_doc(TaskGraph, 200),
    "/individual-groups": individual_doc(TaskGroupGraph, 200),
    "/individual-group-progress": individual_doc(TaskGroupProgress, 200),
    "/individual-workers-memory": individual_doc(WorkersMemory, 100),
    "/individual-cluster-memory": individual_doc(ClusterMemory, 100),
    "/individual-workers-transfer-bytes": individual_doc(WorkersTransferBytes, 100),
    "/individual-cpu": individual_doc(CurrentLoad, 100, fig_attr="cpu_figure"),
    "/individual-nprocessing": individual_doc(
        CurrentLoad, 100, fig_attr="processing_figure"
    ),
    "/individual-occupancy": individual_doc(Occupancy, 100),
    "/individual-workers": individual_doc(WorkerTable, 500),
    "/individual-exceptions": individual_doc(ExceptionsTable, 1000),
    "/individual-bandwidth-types": individual_doc(BandwidthTypes, 500),
    "/individual-bandwidth-workers": individual_doc(BandwidthWorkers, 500),
    "/individual-workers-network": individual_doc(
        WorkerNetworkBandwidth, 500, fig_attr="bandwidth"
    ),
    "/individual-workers-disk": individual_doc(
        WorkerNetworkBandwidth, 500, fig_attr="disk"
    ),
    "/individual-workers-network-timeseries": individual_doc(
        SystemTimeseries, 500, fig_attr="bandwidth"
    ),
    "/individual-workers-cpu-timeseries": individual_doc(
        SystemTimeseries, 500, fig_attr="cpu"
    ),
    "/individual-workers-memory-timeseries": individual_doc(
        SystemTimeseries, 500, fig_attr="memory"
    ),
    "/individual-workers-disk-timeseries": individual_doc(
        SystemTimeseries, 500, fig_attr="disk"
    ),
    "/individual-memory-by-key": individual_doc(MemoryByKey, 500),
    "/individual-compute-time-per-key": individual_doc(ComputePerKey, 500),
    "/individual-aggregate-time-per-action": individual_doc(AggregateAction, 500),
    "/individual-scheduler-system": individual_doc(SystemMonitor, 500),
    "/individual-contention": individual_doc(Contention, 500),
    "/individual-fine-performance-metrics": individual_doc(FinePerformanceMetrics, 500),
    "/individual-profile": individual_profile_doc,
    "/individual-profile-server": individual_profile_server_doc,
    "/individual-gpu-memory": gpu_memory_doc,
    "/individual-gpu-utilization": gpu_utilization_doc,
    "/individual-rmm-memory": rmm_memory_doc,
}


@memoize
def template_variables(scheduler):
    from distributed.diagnostics.nvml import device_get_count

    template_variables = {
        "pages": [
            "status",
            "workers",
            "tasks",
            "system",
            *(["gpu"] if device_get_count() > 0 else []),
            "profile",
            "graph",
            "groups",
            "info",
        ],
        "plots": [
            {
                "url": x.strip("/"),
                "name": " ".join(x.strip("/").split("-")[1:])
                .title()
                .replace("Cpu", "CPU")
                .replace("Gpu", "GPU")
                .replace("Rmm", "RMM"),
            }
            for x in applications
            if "individual" in x
        ]
        + [
            {"url": "hardware", "name": "Hardware"},
            {"url": "shuffle", "name": "Shuffle"},
        ],
        "jupyter": scheduler.jupyter,
    }
    template_variables["plots"] = sorted(
        template_variables["plots"], key=lambda d: d["name"]
    )
    return template_variables


def connect(application, http_server, scheduler, prefix=""):
    bokeh_app = BokehApplication(
        applications,
        scheduler,
        prefix=prefix,
        template_variables=template_variables(scheduler),
    )
    application.add_application(bokeh_app)
    bokeh_app.initialize(IOLoop.current())

    routes = [
        (
            r"/",
            web.RedirectHandler,
            {"url": urljoin((prefix or "").strip("/") + "/", "status")},
        )
    ]

    if prefix:
        prefix_clean = prefix.strip("/")
        routes.append(
            (
                rf"/{prefix_clean}/?",
                web.RedirectHandler,
                {"url": urljoin(prefix_clean + "/", "status")},
            )
        )

    bokeh_app.add_handlers(r".*", routes)
    bokeh_app.start()
