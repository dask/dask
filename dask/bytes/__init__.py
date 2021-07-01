import warnings

from fsspec.core import open as fs_open_file
from fsspec.core import open_files as fs_open_files

from ..utils import _deprecated
from .core import read_bytes


@_deprecated(use_instead="fsspec.core.open")
def open_file(*arg, **kwargs):
    return fs_open_file(*arg, **kwargs)


@_deprecated(use_instead="fsspec.core.open_files")
def open_files(*arg, **kwargs):
    return fs_open_files(*arg, **kwargs)
