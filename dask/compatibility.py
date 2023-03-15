import sys

from importlib_metadata import entry_points
from packaging.version import parse as parse_version

__all__ = ["entry_points"]

_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))

_EMSCRIPTEN = sys.platform == "emscripten"
