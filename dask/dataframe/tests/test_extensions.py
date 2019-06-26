from decimal import Decimal
import pytest

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq, PANDAS_VERSION
from dask.compatibility import PY2

pd = pytest.importorskip("pandas", minversion="0.23.4")

from pandas.tests.extension.decimal.array import DecimalArray, DecimalDtype
from dask.dataframe.extensions import make_array_nonempty, make_scalar


@make_array_nonempty.register(DecimalDtype)
def _(dtype):
    kwargs = {}
    if PANDAS_VERSION >= "0.24.0rc1":
        kwargs["dtype"] = dtype

    return DecimalArray._from_sequence([Decimal("0"), Decimal("NaN")], **kwargs)


@make_scalar.register(Decimal)
def _(x):
    return Decimal("1")


@pytest.mark.skipif(PY2, reason="unhashable Context")
def test_register_extension_type():
    arr = DecimalArray._from_sequence([Decimal("1.0")] * 10)
    ser = pd.Series(arr)
    dser = dd.from_pandas(ser, 2)
    assert_eq(ser, dser)

    df = pd.DataFrame({"A": ser})
    ddf = dd.from_pandas(df, 2)
    assert_eq(df, ddf)


def test_reduction():
    pytest.importorskip("pandas", minversion="0.24.0")
    ser = pd.Series(DecimalArray._from_sequence([Decimal("0"), Decimal("1")]))
    dser = dd.from_pandas(ser, 2)
    assert_eq(ser.mean(skipna=False), dser.mean(skipna=False))

    assert_eq(ser.to_frame().mean(skipna=False), dser.to_frame().mean(skipna=False))


def test_scalar():
    result = dd.utils.make_meta(Decimal("1.0"))
    assert result == Decimal("1.0")
