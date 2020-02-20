import pandas as pd
from pandas.core.dtypes.common import is_scalar

from ..delayed import delayed
from ..array import Array
from .core import Series


__all__ = ("to_numeric",)


def to_numeric(arg, meta=None):
    """
    Convert argument to a numeric type.

    The default return dtype is `float64` or `int64`
    depending on the data supplied.

    Please note that precision loss may occur if really large numbers
    are passed in. Due to the internal limitations of `ndarray`, if
    numbers smaller than `-9223372036854775808` (np.iinfo(np.int64).min)
    or larger than `18446744073709551615` (np.iinfo(np.uint64).max) are
    passed in, it is very likely they will be converted to float so that
    they can stored in an `ndarray`. These warnings apply similarly to
    `Series` since it internally leverages `ndarray`.

    Parameters
    ----------
    arg : scalar, dask.array.Array, or dask.dataframe.Series

    Returns
    -------
    ret : numeric if parsing succeeded.
        Return type depends on input. Delayed if scalar, otherwise same as input.
    """
    if isinstance(arg, Series):
        return arg.map_partitions(
            pd.to_numeric,
            token=arg._name + "-to_numeric",
            meta=meta if meta is not None else pd.to_numeric(arg._meta),
            enforce_metadata=False,
        )
    if isinstance(arg, Array):
        return arg.map_blocks(
            pd.to_numeric,
            name=arg._name + "-to_numeric",
            meta=meta if meta is not None else pd.to_numeric(arg._meta),
        )
    if is_scalar(arg):
        return delayed(pd.to_numeric, pure=True)(arg)

    raise TypeError(
        "arg must be a list, tuple, dask.array.Array, or dask.dataframe.Series"
    )
