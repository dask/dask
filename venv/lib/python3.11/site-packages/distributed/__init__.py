from __future__ import annotations

# isort: off
from distributed import config  # load distributed configuration first
from distributed import widgets  # load distributed widgets second

# isort: on

import atexit
import weakref

# This finalizer registers an atexit handler that has to happen before
# distributed registers its handlers, otherwise we observe hangs on
# cluster shutdown when using the UCX comms backend. See
# https://github.com/dask/distributed/issues/7726 for more discussion
# of the problem and the search for long term solutions

weakref.finalize(lambda: None, lambda: None)
import dask
from dask.config import config  # type: ignore

from distributed.actor import Actor, ActorFuture, BaseActorFuture
from distributed.client import (
    Client,
    CompatibleExecutor,
    Future,
    as_completed,
    default_client,
    fire_and_forget,
    futures_of,
    get_task_metadata,
    get_task_stream,
    performance_report,
    wait,
)
from distributed.core import Status, connect, rpc
from distributed.deploy import (
    Adaptive,
    LocalCluster,
    SpecCluster,
    SSHCluster,
    SubprocessCluster,
)
from distributed.diagnostics.plugin import (
    CondaInstall,
    Environ,
    InstallPlugin,
    NannyPlugin,
    PipInstall,
    SchedulerPlugin,
    UploadDirectory,
    UploadFile,
    WorkerPlugin,
)
from distributed.diagnostics.progressbar import progress
from distributed.event import Event
from distributed.lock import Lock
from distributed.multi_lock import MultiLock
from distributed.nanny import Nanny
from distributed.queues import Queue
from distributed.scheduler import KilledWorker, Scheduler
from distributed.security import Security
from distributed.semaphore import Semaphore
from distributed.spans import span
from distributed.threadpoolexecutor import rejoin
from distributed.utils import CancelledError, TimeoutError, sync
from distributed.variable import Variable
from distributed.worker import (
    Reschedule,
    Worker,
    get_client,
    get_worker,
    print,
    secede,
    warn,
)
from distributed.worker_client import local_client, worker_client

try:
    # Backwards compatibility with versioneer
    from distributed._version import __commit_id__ as __git_revision__
    from distributed._version import __version__
except ImportError:
    __git_revision__ = "unknown"
    __version__ = "unknown"


__all__ = [
    "Actor",
    "ActorFuture",
    "Adaptive",
    "BaseActorFuture",
    "CancelledError",
    "Client",
    "CompatibleExecutor",
    "CondaInstall",
    "Environ",
    "Event",
    "Future",
    "KilledWorker",
    "LocalCluster",
    "Lock",
    "MultiLock",
    "Nanny",
    "NannyPlugin",
    "InstallPlugin",
    "PipInstall",
    "Queue",
    "Reschedule",
    "SSHCluster",
    "Scheduler",
    "SchedulerPlugin",
    "Security",
    "Semaphore",
    "SpecCluster",
    "Status",
    "SubprocessCluster",
    "TimeoutError",
    "UploadDirectory",
    "UploadFile",
    "Variable",
    "Worker",
    "WorkerPlugin",
    "as_completed",
    "config",
    "connect",
    "dask",
    "default_client",
    "fire_and_forget",
    "futures_of",
    "get_client",
    "get_task_metadata",
    "get_task_stream",
    "get_worker",
    "local_client",
    "performance_report",
    "print",
    "progress",
    "rejoin",
    "rpc",
    "secede",
    "span",
    "sync",
    "wait",
    "warn",
    "widgets",
    "worker_client",
]
