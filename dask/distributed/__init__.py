from .worker import Worker
from .scheduler import Scheduler
from .client import Client
from .ipython_utils import dask_client_from_ipclient

from warnings import warn

warn(FutureWarning('''

dask.distributed is deprecated and will be removed in a future version
'''))
