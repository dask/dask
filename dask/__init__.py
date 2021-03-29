from . import config, datasets
from .core import istask
from .local import get_sync as get

try:
    from .delayed import delayed
except ImportError:
    pass
try:
    from .base import (
        annotate,
        compute,
        is_dask_collection,
        optimize,
        persist,
        visualize,
    )
except ImportError:
    pass

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
