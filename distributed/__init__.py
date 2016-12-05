from __future__ import print_function, division, absolute_import

from .config import config
from .core import connect, read, write, rpc
from .deploy import LocalCluster
from .diagnostics import progress
from .client import (Client, Executor, CompatibleExecutor, wait, as_completed,
        default_client)
from .nanny import Nanny
from .scheduler import Scheduler
from .utils import sync
from .worker import Worker
from .worker_client import local_client

try:
    from .collections import futures_to_collection
except:
    pass

from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
