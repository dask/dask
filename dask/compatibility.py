import sys

from packaging.version import parse as parse_version

try:
    from math import prod
except ImportError:
    # Python < 3.8
    def prod(iterable, *, start=1):  # type: ignore
        acc = start
        for el in iterable:
            acc *= el
        return acc


_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))
