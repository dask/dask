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


PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))
