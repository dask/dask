import pandas as pd

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


@make_scalar.register(str)
def _(x):
    return "s"


@make_array_nonempty.register(pd.BooleanDtype)
def _(dtype):
    return pd.array([True, pd.NA], dtype=dtype)


@make_scalar.register(bool)
def _(x):
    return True
