from __future__ import print_function, division, absolute_import

from .config import config
from .core import connect, rpc
from .deploy import LocalCluster
from .diagnostics import progress
from .client import (Client, Executor, CompatibleExecutor,
                     wait, as_completed, default_client)
from .nanny import Nanny
from .queues import Queue
from .scheduler import Scheduler
from .utils import sync
from .variable import Variable
from .worker import Worker, get_worker
from .worker_client import local_client, worker_client

from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
