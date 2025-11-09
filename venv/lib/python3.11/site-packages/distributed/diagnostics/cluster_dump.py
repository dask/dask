from __future__ import annotations

from collections.abc import Collection
from typing import Any, Literal

from distributed.cluster_dump import (
    DEFAULT_CLUSTER_DUMP_EXCLUDE,
    DEFAULT_CLUSTER_DUMP_FORMAT,
)
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.scheduler import Scheduler


class ClusterDump(SchedulerPlugin):
    """Dumps cluster state prior to Scheduler shutdown

    The Scheduler may shutdown in cases where it is in an error state,
    or when it has been unexpectedly idle for long periods of time.
    This plugin dumps the cluster state prior to Scheduler shutdown
    for debugging purposes.
    """

    def __init__(
        self,
        url: str,
        exclude: Collection[str] = DEFAULT_CLUSTER_DUMP_EXCLUDE,
        format_: Literal["msgpack", "yaml"] = DEFAULT_CLUSTER_DUMP_FORMAT,
        **storage_options: dict[str, Any],
    ):
        self.url = url
        self.exclude = exclude
        self.format = format_
        self.storage_options = storage_options

    async def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    async def before_close(self) -> None:
        await self.scheduler.dump_cluster_state_to_url(
            self.url, self.exclude, self.format, **self.storage_options
        )
