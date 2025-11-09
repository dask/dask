from __future__ import annotations

import logging
from collections.abc import Iterator
from time import time
from typing import ClassVar

import prometheus_client
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric

from distributed.gc import gc_collect_duration
from distributed.http.prometheus import PrometheusCollector
from distributed.http.utils import RequestHandler
from distributed.worker import Worker

logger = logging.getLogger("distributed.prometheus.worker")


class WorkerMetricCollector(PrometheusCollector):
    server: Worker

    def __init__(self, server: Worker):
        super().__init__(server)
        self.subsystem = "worker"
        self.crick_available = True
        try:
            import crick  # noqa: F401
        except ImportError:
            self.crick_available = False
            logger.debug(
                "Not all prometheus metrics available are exported. "
                "Digest-based metrics require crick to be installed."
            )

    def collect(self) -> Iterator[Metric]:
        self.server.monitor.update()
        ws = self.server.state

        tasks = GaugeMetricFamily(
            self.build_name("tasks"),
            "Number of tasks at worker.",
            labels=["state"],
        )
        for state, n in ws.task_counter.current_count(by_prefix=False).items():
            if state == "memory" and hasattr(self.server.data, "slow"):
                n_spilled = len(self.server.data.slow)
                if n - n_spilled > 0:
                    tasks.add_metric(["memory"], n - n_spilled)
                if n_spilled > 0:
                    tasks.add_metric(["disk"], n_spilled)
            else:
                tasks.add_metric([state], n)
        yield tasks

        yield GaugeMetricFamily(
            self.build_name("concurrent_fetch_requests"),
            (
                "Deprecated: This metric has been renamed to transfer_incoming_count.\n"
                "Number of open fetch requests to other workers"
            ),
            value=ws.transfer_incoming_count,
        )

        if self.server.monitor.monitor_gil_contention:
            yield CounterMetricFamily(
                self.build_name("gil_contention"),
                "GIL contention metric",
                value=self.server.monitor.cumulative_gil_contention,
                unit="seconds",
            )

        yield CounterMetricFamily(
            self.build_name("gc_collection"),
            "Total time spent on garbage collection",
            value=gc_collect_duration(),
            unit="seconds",
        )

        yield GaugeMetricFamily(
            self.build_name("threads"),
            "Number of worker threads",
            value=ws.nthreads,
        )

        yield GaugeMetricFamily(
            self.build_name("latency"),
            "Latency of worker connection",
            unit="seconds",
            value=self.server.latency,
        )

        try:
            spilled_memory, spilled_disk = self.server.data.spilled_total  # type: ignore
        except AttributeError:
            spilled_memory, spilled_disk = 0, 0  # spilling is disabled
        process_memory = self.server.monitor.get_process_memory()
        managed_memory = min(process_memory, ws.nbytes - spilled_memory)

        memory = GaugeMetricFamily(
            self.build_name("memory_bytes"),
            "Memory breakdown",
            labels=["type"],
        )
        memory.add_metric(["managed"], managed_memory)
        memory.add_metric(["unmanaged"], process_memory - managed_memory)
        memory.add_metric(["spilled"], spilled_disk)
        yield memory

        yield GaugeMetricFamily(
            self.build_name("transfer_incoming_bytes"),
            "Total size of open data transfers from other workers",
            value=ws.transfer_incoming_bytes,
        )
        yield GaugeMetricFamily(
            self.build_name("transfer_incoming_count"),
            "Number of open data transfers from other workers",
            value=ws.transfer_incoming_count,
        )

        yield CounterMetricFamily(
            self.build_name("transfer_incoming_count_total"),
            (
                "Total number of data transfers from other workers "
                "since the worker was started"
            ),
            value=ws.transfer_incoming_count_total,
        )

        yield GaugeMetricFamily(
            self.build_name("transfer_outgoing_bytes"),
            "Total size of open data transfers to other workers",
            value=self.server.transfer_outgoing_bytes,
        )
        yield GaugeMetricFamily(
            self.build_name("transfer_outgoing_count"),
            "Number of open data transfers to other workers",
            value=self.server.transfer_outgoing_count,
        )

        yield CounterMetricFamily(
            self.build_name("transfer_outgoing_bytes_total"),
            (
                "Total size of data transfers to other workers "
                "since the worker was started (including in-progress and failed transfers)"
            ),
            value=self.server.transfer_outgoing_bytes_total,
        )

        yield CounterMetricFamily(
            self.build_name("transfer_outgoing_count_total"),
            (
                "Total number of data transfers to other workers "
                "since the worker was started"
            ),
            value=self.server.transfer_outgoing_count_total,
        )

        yield from self.collect_crick()
        yield from self.collect_spillbuffer()

        now = time()
        max_tick_duration = max(
            self.server.digests_max.pop("tick_duration", 0),
            now - self.server._last_tick,
        )
        yield GaugeMetricFamily(
            self.build_name("tick_duration_maximum"),
            "Maximum tick duration observed since Prometheus last scraped metrics",
            unit="seconds",
            value=max_tick_duration,
        )
        yield CounterMetricFamily(
            self.build_name("tick_count"),
            "Total number of ticks observed since the server started",
            value=self.server._tick_counter,
        )

    def collect_crick(self) -> Iterator[Metric]:
        # All metrics using digests require crick to be installed.
        # The following metrics will export NaN, if the corresponding digests are None
        if not self.crick_available:
            return
        assert self.server.digests

        yield GaugeMetricFamily(
            self.build_name("tick_duration_median"),
            "Median tick duration at worker",
            unit="seconds",
            value=self.server.digests["tick-duration"].components[1].quantile(50),
        )

        yield GaugeMetricFamily(
            self.build_name("task_duration_median"),
            "Median task runtime at worker",
            unit="seconds",
            value=self.server.digests["task-duration"].components[1].quantile(50),
        )

        yield GaugeMetricFamily(
            self.build_name("transfer_bandwidth_median"),
            "Bandwidth for transfer at worker",
            unit="bytes",
            value=self.server.digests["transfer-bandwidth"].components[1].quantile(50),
        )

    def collect_spillbuffer(self) -> Iterator[Metric]:
        """SpillBuffer-specific metrics.

        Additionally, you can obtain derived metrics as follows:

        cache hit ratios:
          by keys  = spill_count.memory_read / (spill_count.memory_read + spill_count.disk_read)
          by bytes = spill_bytes.memory_read / (spill_bytes.memory_read + spill_bytes.disk_read)

        mean times per key:
          pickle   = spill_time.pickle     / spill_count.disk_write
          write    = spill_time.disk_write / spill_count.disk_write
          unpickle = spill_time.unpickle   / spill_count.disk_read
          read     = spill_time.disk_read  / spill_count.disk_read

        mean bytes per key:
          write    = spill_bytes.disk_write / spill_count.disk_write
          read     = spill_bytes.disk_read  / spill_count.disk_read

        mean bytes per second:
          write    = spill_bytes.disk_write / spill_time.disk_write
          read     = spill_bytes.disk_read  / spill_time.disk_read
        """
        try:
            metrics = self.server.data.cumulative_metrics  # type: ignore
        except AttributeError:
            return  # spilling is disabled

        counters = {
            "bytes": CounterMetricFamily(
                self.build_name("spill_bytes"),
                "Total size of memory and disk accesses caused by managed data "
                "since the latest worker restart",
                labels=["activity"],
            ),
            "count": CounterMetricFamily(
                self.build_name("spill_count"),
                "Total number of memory and disk accesses caused by managed data "
                "since the latest worker restart",
                labels=["activity"],
            ),
            "seconds": CounterMetricFamily(
                self.build_name("spill_time"),
                "Total time spent spilling/unspilling since the latest worker restart",
                unit="seconds",
                labels=["activity"],
            ),
        }

        # Note: memory_read is used to calculate cache hit ratios (see docstring)
        for (label, unit), value in metrics.items():
            counters[unit].add_metric([label], value)

        yield from counters.values()


class PrometheusHandler(RequestHandler):
    _collector: ClassVar[WorkerMetricCollector | None] = None

    def __init__(self, *args, dask_server=None, **kwargs):
        super().__init__(*args, dask_server=dask_server, **kwargs)

        if PrometheusHandler._collector:
            # Especially during testing, multiple workers are started
            # sequentially in the same python process
            PrometheusHandler._collector.server = self.server
            return

        PrometheusHandler._collector = WorkerMetricCollector(self.server)
        # Register collector
        prometheus_client.REGISTRY.register(PrometheusHandler._collector)

    def get(self):
        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
