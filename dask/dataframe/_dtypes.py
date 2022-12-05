import pandas as pd

from dask.dataframe._compat import PANDAS_GT_150
from dask.dataframe.extensions import make_array_nonempty, make_scalar


@make_array_nonempty.register(pd.DatetimeTZDtype)
def _(dtype):
    return pd.array([pd.Timestamp(1), pd.NaT], dtype=dtype)


@make_scalar.register(pd.DatetimeTZDtype)
def _(x):
    return pd.Timestamp(1, tz=x.tz, unit=x.unit)


@make_array_nonempty.register(pd.StringDtype)
def _(dtype):
    return pd.array(["a", pd.NA], dtype=dtype)


if PANDAS_GT_150:

    @make_array_nonempty.register(pd.ArrowDtype)
    def _make_array_nonempty_pyarrow_dtype(dtype):
        return dtype.empty(2)


@make_scalar.register(str)
def _(x):
    return "s"


@make_array_nonempty.register(pd.BooleanDtype)
def _(dtype):
    return pd.array([True, pd.NA], dtype=dtype)


@make_scalar.register(bool)
def _(x):
    return True
