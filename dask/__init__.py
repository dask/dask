from __future__ import annotations

from dask import config, datasets
from dask._expr import Expr, HLGExpr, LLGExpr, SingletonExpr

try:
    # Backwards compatibility with versioneer
    from dask._version import __commit_id__
    from dask._version import __version__ as __version__

    __git_revision__ = __commit_id__
except ImportError:  # pragma: no cover
    # The version file is generated at install time; a cached install may skip it.
    # Fall back to the distribution metadata, and only report "unknown" when dask is
    # not installed at all. A bare "unknown" would break anything parsing the version
    # (e.g. xarray's DuckArrayModule).
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("dask")
    except PackageNotFoundError:
        __version__ = "unknown"

    __git_revision__ = "unknown"
    del PackageNotFoundError, version

from dask.base import (
    annotate,
    compute,
    get_annotations,
    is_dask_collection,
    optimize,
    persist,
    visualize,
)
from dask.core import istask
from dask.delayed import delayed
from dask.local import get_sync as get

__all__ = [
    "Expr",
    "HLGExpr",
    "LLGExpr",
    "SingletonExpr",
    "annotate",
    "compute",
    "config",
    "datasets",
    "delayed",
    "get",
    "get_annotations",
    "is_dask_collection",
    "istask",
    "optimize",
    "persist",
    "visualize",
]
