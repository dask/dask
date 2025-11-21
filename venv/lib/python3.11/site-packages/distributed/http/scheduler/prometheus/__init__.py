from __future__ import annotations

from distributed.http.prometheus import import_metrics_handler

routes: list[tuple] = [
    (
        "/metrics",
        import_metrics_handler(
            "distributed.http.scheduler.prometheus.core", "PrometheusHandler"
        ),
        {},
    )
]
