from . import config, datasets
from ._version import get_versions
from .base import annotate, compute, is_dask_collection, optimize, persist, visualize
from .core import istask
from .delayed import delayed
from .local import get_sync as get

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions
