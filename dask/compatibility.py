import warnings

from dask._compatibility import (  # noqa
    _EMSCRIPTEN,
    _PY_VERSION,
    entry_points,
    parse_version,
)

warnings.warn(
    "`dask.compatibility` is not intended for external use and has been renamed to `dask._compatibility`. "
    "This backward-compatible shim will be removed in a future release. Please find an alternative.",
    DeprecationWarning,
    stacklevel=2,
)
