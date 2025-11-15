from __future__ import annotations

from importlib import import_module

import dask.config

from distributed.http.utils import RequestHandler


class PrometheusCollector:
    def __init__(self, server):
        self.server = server
        self.namespace = dask.config.get("distributed.dashboard.prometheus.namespace")
        self.subsystem = None

    def build_name(self, name):
        full_name = []
        if self.namespace:
            full_name.append(self.namespace)
        if self.subsystem:
            full_name.append(self.subsystem)
        full_name.append(name)
        return "_".join(full_name)


class PrometheusNotAvailableHandler(RequestHandler):
    def get(self):
        self.write(
            "# Prometheus metrics are not available, see: "
            "https://docs.dask.org/en/stable/how-to/setup-prometheus.html#setup-prometheus-monitoring"
        )
        self.set_header("Content-Type", "text/plain; version=0.0.4")


def import_metrics_handler(module_name: str, handler_name: str) -> type[RequestHandler]:
    """Import ``handler_name`` from ``module_name`` if ``prometheus_client``
    is installed, import the ``PrometheusNotAvailableHandler`` otherwise."""
    try:
        import prometheus_client  # noqa: F401
    except ModuleNotFoundError:
        return PrometheusNotAvailableHandler
    module = import_module(module_name)
    handler = getattr(module, handler_name)
    assert issubclass(handler, RequestHandler)
    return handler
