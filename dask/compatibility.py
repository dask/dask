import sys

# FIXME importing importlib.metadata fails when running the entire test suite with UPSTREAM_DEV=1
from importlib import metadata as importlib_metadata

from packaging.version import parse as parse_version

_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))

_EMSCRIPTEN = sys.platform == "emscripten"


def entry_points(group=None):
    """Returns an iterable of entrypoints.

    For compatibility with Python 3.8/3.9.
    In 3.10 the return type changed from a dict to an ``importlib.metadata.EntryPoints``.
    This compatibility utility can be removed once Python 3.10 is the minimum.
    """
    if _PY_VERSION >= parse_version("3.10"):
        return importlib_metadata.entry_points(group=group)
    else:
        eps = importlib_metadata.entry_points()
        if group:
            return eps.get(group, [])
        return eps
