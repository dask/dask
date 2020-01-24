import pandas as pd
from ._compat import PANDAS_GT_100
from .extensions import make_array_nonempty, make_scalar

if PANDAS_GT_100:

    @make_array_nonempty.register(pd.StringDtype)
    def _(dtype):
        return pd.array(["a", pd.NA], dtype=dtype)

    @make_scalar.register(str)
    def _(x):
        return "s"

    @make_array_nonempty.register(pd.BooleanDtype)
    def _dtype(dtype):
        return pd.array([True, pd.NA], dtype=dtype)

    @make_scalar.register(bool)
    def _(x):
        return True
