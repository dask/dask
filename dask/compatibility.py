import warnings

from dask._compatibility import EMSCRIPTEN as _EMSCRIPTEN  # noqa
from dask._compatibility import PY_VERSION as _PY_VERSION  # noqa
from dask._compatibility import entry_points, parse_version  # noqa

warnings.warn(
    "`dask.compatibility` is not intended for external use and has been renamed to `dask._compatibility`. "
    "This backward-compatible shim will be removed in a future release. Please find an alternative.",
    DeprecationWarning,
    stacklevel=2,
)
