import pandas as pd
import pytest
from dask.dataframe import assert_eq

from dask_expr import from_pandas


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": [1, 2, 3, 4, 1, 2]}, dtype="category")
    return pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=2)


def test_set_categories(df, pdf):
    assert df.x.cat.known
    assert_eq(df.x.cat.codes, pdf.x.cat.codes)
    ser = df.x.cat.as_unknown()
    assert not ser.cat.known
    ser = ser.cat.as_known()
    assert_eq(ser.cat.categories, pd.Index([1, 2, 3, 4]))
    ser = ser.cat.set_categories([1, 2, 3, 5, 4])
    assert_eq(ser.cat.categories, pd.Index([1, 2, 3, 5, 4]))
    assert not ser.cat.ordered
