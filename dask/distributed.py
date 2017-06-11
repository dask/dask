# flake8: noqa
from __future__ import absolute_import, division, print_function

try:
    from distributed import (Client, Executor, progress, as_completed, wait,
            LocalCluster, Scheduler, Worker, Nanny, local_client, config,
            CompatibleExecutor, worker_client)
except ImportError:
    msg = ("Dask's distributed scheduler is not installed.\n\n"
           "Please either conda or pip install dask distributed:\n\n"
           "  conda install dask distributed          # either conda install\n"
           "  pip install dask distributed --upgrade  # or pip install")
    raise ImportError(msg)
else:
    try:  # newer imports
        from distributed import Queue, Variable, get_worker
    except ImportError:
        pass
