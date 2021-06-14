import warnings

from fsspec.core import open as fs_open_file
from fsspec.core import open_files as fs_open_files

from .core import read_bytes


def open_file(*arg, **kwargs):
    warnings.warn(
        "Importing ``open_file`` from dask.bytes is deprecated now and will be "
        "removed in a future version. To silence this warning, "
        "use ``from fsspec.core import open as open_file`` instead.",
        FutureWarning,
    )
    return fs_open_file(*arg, **kwargs)


def open_files(*arg, **kwargs):
    warnings.warn(
        "Importing ``open_files`` from dask.bytes is deprecated now and will be "
        "removed in a future version. To silence this warning, "
        "use ``from fsspec.core import open_files`` instead.",
        FutureWarning,
    )
    return fs_open_files(*arg, **kwargs)
