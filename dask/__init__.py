from dask import config, datasets
from dask._version import __version__
from dask.base import (
    annotate,
    compute,
    is_dask_collection,
    optimize,
    persist,
    visualize,
)
from dask.core import istask
from dask.delayed import delayed
from dask.local import get_sync as get
