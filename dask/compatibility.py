import sys
from distutils.version import LooseVersion

# TODO: remove this import once dask requires distributed > 2.3.2
from .utils import apply  # noqa

# TODO: remove this once dask requires distributed >= 2.2.0
unicode = str  # noqa

try:
    from dataclasses import is_dataclass, fields as dataclass_fields

except ImportError:

    def is_dataclass(x):
        return False

    def dataclass_fields(x):
        return []


PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))
