from __future__ import print_function, division, absolute_import

from .config import config
from .core import connect, read, write, rpc
from .deploy import LocalCluster
from .diagnostics import progress
from .executor import (Executor, CompatibleExecutor, wait, as_completed,
        default_executor)
from .nanny import Nanny
from .scheduler import Scheduler
from .utils import sync
from .worker import Worker
from .worker_executor import local_executor

try:
    from .collections import futures_to_collection
except:
    pass


__version__ = '1.12.2'
