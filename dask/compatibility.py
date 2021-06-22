import sys
from distutils.version import LooseVersion

try:
    from math import prod
except ImportError:
    # Python < 3.8
    def prod(iterable, *, start=1):
        acc = start
        for el in iterable:
            acc *= el
        return acc


_PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))


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
