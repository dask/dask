import sys
import warnings

from importlib_metadata import entry_points as _entry_points
from packaging.version import parse as parse_version

_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))

_EMSCRIPTEN = sys.platform == "emscripten"


def entry_points(*args, **kwargs):
    warnings.warn(
        "`dask.compatibility.entry_points` has been replaced by `importlib_metadata.entry_points` and will be removed "
        "in a future version. Please use `importlib_metadata.entry_points` instead.",
        DeprecationWarning,
    )
    return _entry_points(*args, **kwargs)
