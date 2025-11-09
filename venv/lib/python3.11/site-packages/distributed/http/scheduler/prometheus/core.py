from __future__ import annotations

from collections.abc import Iterator
from time import time

import prometheus_client
import toolz
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from distributed.core import Status
from distributed.gc import gc_collect_duration
from distributed.http.prometheus import PrometheusCollector
from distributed.http.scheduler.prometheus.semaphore import SemaphoreMetricCollector
from distributed.http.scheduler.prometheus.stealing import WorkStealingMetricCollector
from distributed.http.utils import RequestHandler
from distributed.scheduler import ALL_TASK_STATES, Scheduler


class SchedulerMetricCollector(PrometheusCollector):
    server: Scheduler

    def __init__(self, server: Scheduler):
        super().__init__(server)
        self.subsystem = "scheduler"

    def collect(self) -> Iterator[GaugeMetricFamily | CounterMetricFamily]:
        self.server.monitor.update()

        yield GaugeMetricFamily(
            self.build_name("clients"),
            "Number of clients connected",
            value=len([k for k in self.server.clients if k != "fire-and-forget"]),
        )

        yield CounterMetricFamily(
            self.build_name("client_connections_added"),
            "Total number of client connections added",
            value=self.server._client_connections_added_total,
        )

        yield CounterMetricFamily(
            self.build_name("client_connections_removed"),
            "Total number of client connections removed",
            value=self.server._client_connections_removed_total,
        )

        yield GaugeMetricFamily(
            self.build_name("desired_workers"),
            "Number of workers scheduler needs for task graph",
            value=self.server.adaptive_target(),
        )

        worker_states = GaugeMetricFamily(
            self.build_name("workers"),
            "Number of workers known by scheduler",
            labels=["state"],
        )
        worker_states.add_metric(["idle"], len(self.server.idle))
        worker_states.add_metric(
            ["partially_saturated"],
            len(self.server.running)
            - len(self.server.idle)
            - len(self.server.saturated),
        )
        worker_states.add_metric(["saturated"], len(self.server.saturated))
        paused_workers = len(
            [w for w in self.server.workers.values() if w.status == Status.paused]
        )
        worker_states.add_metric(["paused"], paused_workers)
        worker_states.add_metric(
            ["retiring"],
            len(self.server.workers) - paused_workers - len(self.server.running),
        )

        yield worker_states

        yield CounterMetricFamily(
            self.build_name("workers_added"),
            "Total number of workers added",
            value=self.server._workers_added_total,
        )

        yield CounterMetricFamily(
            self.build_name("workers_removed"),
            "Total number of workers removed",
            value=self.server._workers_removed_total,
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

        yield CounterMetricFamily(
            self.build_name("last_time"),
            "SystemMonitor last time",
            value=self.server.monitor.last_time,
        )

        tasks = GaugeMetricFamily(
            self.build_name("tasks"),
            "Number of tasks known by scheduler",
            labels=["state"],
        )

        task_counter = toolz.merge_with(
            sum, (tp.states for tp in self.server.task_prefixes.values())
        )

        suspicious_tasks = CounterMetricFamily(
            self.build_name("tasks_suspicious"),
            "Total number of times a task has been marked suspicious",
            labels=["task_prefix_name"],
        )

        for tp in self.server.task_prefixes.values():
            suspicious_tasks.add_metric([tp.name], tp.suspicious)
        yield suspicious_tasks

        yield CounterMetricFamily(
            self.build_name("tasks_forgotten"),
            (
                "Total number of processed tasks no longer in memory and already "
                "removed from the scheduler job queue\n"
                "Note: Task groups on the scheduler which have all tasks "
                "in the forgotten state are not included."
            ),
            value=task_counter.get("forgotten", 0.0),
        )

        for state in ALL_TASK_STATES:
            if state != "forgotten":
                tasks.add_metric([state], task_counter.get(state, 0.0))
        yield tasks

        yield GaugeMetricFamily(
            self.build_name("task_groups"),
            "Number of task groups known by scheduler",
            value=len(self.server.task_groups),
        )

        time_spent_compute_tasks = CounterMetricFamily(
            self.build_name("tasks_compute"),
            "Total amount of compute time spent in each prefix",
            labels=["task_prefix_name"],
            unit="seconds",
        )

        for tp in self.server.task_prefixes.values():
            time_spent_compute_tasks.add_metric([tp.name], tp.all_durations["compute"])
        yield time_spent_compute_tasks

        time_spent_transfer_tasks = CounterMetricFamily(
            self.build_name("tasks_transfer"),
            "Total amount of transfer time spent in each prefix",
            labels=["task_prefix_name"],
            unit="seconds",
        )

        for tp in self.server.task_prefixes.values():
            time_spent_transfer_tasks.add_metric(
                [tp.name], tp.all_durations["transfer"]
            )
        yield time_spent_transfer_tasks

        nbytes_tasks = GaugeMetricFamily(
            self.build_name("tasks_output"),
            "Current number of bytes in memory (without duplicates) for each prefix",
            labels=["task_prefix_name"],
            unit="bytes",
        )
        for tp in self.server.task_prefixes.values():
            nbytes_tasks.add_metric([tp.name], tp.nbytes_total)
        yield nbytes_tasks

        prefix_state_counts = CounterMetricFamily(
            self.build_name("prefix_state_totals"),
            "Accumulated count of task prefix in each state",
            labels=["task_prefix_name", "state"],
        )

        for tp in self.server.task_prefixes.values():
            for state, count in tp.state_counts.items():
                prefix_state_counts.add_metric([tp.name, state], count)
        yield prefix_state_counts

        now = time()
        max_tick_duration = max(
            self.server.digests_max["tick_duration"],
            now - self.server._last_tick,
        )
        yield GaugeMetricFamily(
            self.build_name("tick_duration_maximum_seconds"),
            "Maximum tick duration observed since Prometheus last scraped metrics",
            value=max_tick_duration,
        )

        yield CounterMetricFamily(
            self.build_name("tick_count_total"),
            "Total number of ticks observed since the server started",
            value=self.server._tick_counter,
        )

        self.server.digests_max.clear()


COLLECTORS = [
    SchedulerMetricCollector,
    SemaphoreMetricCollector,
    WorkStealingMetricCollector,
]


class PrometheusHandler(RequestHandler):
    _collectors = None

    def __init__(self, *args, dask_server=None, **kwargs):
        super().__init__(*args, dask_server=dask_server, **kwargs)

        if PrometheusHandler._collectors:
            # Especially during testing, multiple schedulers are started
            # sequentially in the same python process
            for _collector in PrometheusHandler._collectors:
                _collector.server = self.server
            return

        PrometheusHandler._collectors = tuple(
            collector(self.server) for collector in COLLECTORS
        )
        # Register collectors
        for instantiated_collector in PrometheusHandler._collectors:
            prometheus_client.REGISTRY.register(instantiated_collector)

    def get(self):
        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
