from decimal import Decimal
import pytest

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq

pd = pytest.importorskip("pandas", minversion="0.24.0rc1")


from pandas.tests.extension.decimal import DecimalArray, DecimalDtype
from dask.dataframe.extensions import make_array_nonempty


@make_array_nonempty.register(DecimalDtype)
def _(dtype):
    return DecimalArray._from_sequence([Decimal('0'), Decimal('NaN')],
                                       dtype=dtype)


def test_register_extension_type():
    arr = DecimalArray._from_sequence([Decimal('1.0')] * 10)
    ser = pd.Series(arr)
    dser = dd.from_pandas(ser, 2)
    assert_eq(ser, dser)

    df = pd.DataFrame({"A": ser})
    ddf = dd.from_pandas(df, 2)
    assert_eq(df, ddf)
