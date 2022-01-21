import sys

from packaging.version import parse as parse_version

_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))


def __getattr__(name):
    if name == "PY_VERSION":
        import warnings

        warnings.warn(
            "dask.compatibility.PY_VERSION is deprecated and will be removed "
            "in a future release.",
            category=FutureWarning,
        )
        return _PY_VERSION
    else:
        raise AttributeError(f"module {__name__} has no attribute {name}")
