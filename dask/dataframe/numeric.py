import pandas as pd
from pandas.core.dtypes.common import is_scalar as pd_is_scalar

from ..utils import derived_from
from ..delayed import delayed
from ..array import Array
from .core import Series


__all__ = ("to_numeric",)


@derived_from(pd)
def to_numeric(arg, errors="raise", downcast=None, meta=None):
    """
    Return type depends on input. Delayed if scalar, otherwise same as input.
    """
    if downcast not in (None, "integer", "signed", "unsigned", "float"):
        raise ValueError("invalid downcasting method provided")

    if errors not in ("ignore", "raise", "coerce"):
        raise ValueError("invalid error value specified")

    is_series = isinstance(arg, Series)
    is_array = isinstance(arg, Array)
    is_scalar = pd_is_scalar(arg)

    if not any([is_series, is_array, is_scalar]):
        raise TypeError(
            "arg must be a list, tuple, dask.array.Array, or dask.dataframe.Series"
        )

    if meta is not None:
        if is_scalar:
            raise KeyError("``meta`` is not allowed when input is a scalar.")
        if downcast is not None:
            raise KeyError("Only one of downcast and meta should be specified.")
    else:
        if is_series or is_array:
            if downcast is None:
                meta = pd.to_numeric(arg._meta)
            else:
                if downcast in ["integer", "signed"]:
                    dtype = "int8"
                elif downcast == "unsigned":
                    dtype = "uint8"
                elif downcast == "float":
                    dtype = "float32"
                meta = (0, dtype)

    if is_series:
        return arg.map_partitions(
            pd.to_numeric,
            token=arg._name + "-to_numeric",
            meta=meta,
            enforce_metadata=False,
            errors=errors,
            downcast=downcast,
        )
    if is_array:
        return arg.map_blocks(
            pd.to_numeric,
            name=arg._name + "-to_numeric",
            meta=meta,
            errors=errors,
            downcast=downcast,
        )
    if is_scalar:
        obj = lambda x: pd.to_numeric(x, errors=errors, downcast=downcast)
        return delayed(obj, name="to_numeric", pure=True)(arg)
