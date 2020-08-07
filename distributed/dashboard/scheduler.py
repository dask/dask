from urllib.parse import urljoin

from tornado.ioloop import IOLoop
from tornado import web

try:
    import numpy as np
except ImportError:
    np = False

from .core import BokehApplication
from .components.worker import counters_doc
from .components.scheduler import (
    systemmonitor_doc,
    stealing_doc,
    workers_doc,
    events_doc,
    tasks_doc,
    status_doc,
    profile_doc,
    profile_server_doc,
    graph_doc,
    individual_task_stream_doc,
    individual_progress_doc,
    individual_graph_doc,
    individual_profile_doc,
    individual_profile_server_doc,
    individual_nbytes_doc,
    individual_cpu_doc,
    individual_nprocessing_doc,
    individual_workers_doc,
    individual_bandwidth_types_doc,
    individual_bandwidth_workers_doc,
    individual_memory_by_key_doc,
    individual_compute_time_per_key_doc,
    individual_aggregate_time_per_action_doc,
)
from .worker import counters_doc
from .components.nvml import gpu_memory_doc, gpu_utilization_doc  # noqa: 1708


template_variables = {
    "pages": ["status", "workers", "tasks", "system", "profile", "graph", "info"]
}


def connect(application, http_server, scheduler, prefix=""):
    bokeh_app = BokehApplication(
        applications, scheduler, prefix=prefix, template_variables=template_variables
    )
    application.add_application(bokeh_app)
    bokeh_app.initialize(IOLoop.current())

    bokeh_app.add_handlers(
        r".*",
        [
            (
                r"/",
                web.RedirectHandler,
                {"url": urljoin((prefix or "").strip("/") + "/", r"status")},
            )
        ],
    )


applications = {
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
    "/individual-cpu": individual_cpu_doc,
    "/individual-nprocessing": individual_nprocessing_doc,
    "/individual-workers": individual_workers_doc,
    "/individual-bandwidth-types": individual_bandwidth_types_doc,
    "/individual-bandwidth-workers": individual_bandwidth_workers_doc,
    "/individual-memory-by-key": individual_memory_by_key_doc,
    "/individual-compute-time-per-key": individual_compute_time_per_key_doc,
    "/individual-aggregate-time-per-action": individual_aggregate_time_per_action_doc,
    "/individual-gpu-memory": gpu_memory_doc,
    "/individual-gpu-utilization": gpu_utilization_doc,
}
