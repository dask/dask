from __future__ import annotations

from dask import config as config
from dask import datasets as datasets
from dask._expr import Expr as Expr
from dask._expr import HLGExpr as HLGExpr
from dask._expr import LLGExpr as LLGExpr
from dask._expr import SingletonExpr as SingletonExpr

try:
    # Backwards compatibility with versioneer
    from dask._version import __commit_id__ as __git_revision__
    from dask._version import __version__ as __version__
except ImportError:  # pragma: no cover
    git_revision = "unknown"
    version = "unknown"

from dask.base import annotate as annotate
from dask.base import compute as compute
from dask.base import get_annotations as get_annotations
from dask.base import is_dask_collection as is_dask_collection
from dask.base import optimize as optimize
from dask.base import persist as persist
from dask.base import visualize as visualize
from dask.core import istask as istask
from dask.delayed import delayed as delayed
from dask.local import get_sync as get
