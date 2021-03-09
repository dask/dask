import sys
from distutils.version import LooseVersion

# TODO: remove this import once dask requires distributed > 2.3.2
from .utils import apply  # noqa

# TODO: remove this once dask requires distributed >= 2.2.0
unicode = str  # noqa

try:
    from math import prod
except ImportError:
    # Python < 3.8
    def prod(iterable, *, start=1):
        acc = start
        for el in iterable:
            acc *= el
        return acc


PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))
