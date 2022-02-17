import sys

from packaging.version import parse as parse_version

_PY_VERSION = parse_version(".".join(map(str, sys.version_info[:3])))
