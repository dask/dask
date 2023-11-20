import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    idx = lib.date_range("2000-01-01", periods=12, freq="T")
    pdf = lib.DataFrame({"foo": range(len(idx))}, index=idx)
    pdf["bar"] = 1
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=4)


def test_resample_count(df, pdf):
    result = df.resample("2T").count()
    expected = pdf.resample("2T").count()
    assert_eq(result, expected)

    result = df.resample("2T").count()["foo"]
    expected = pdf.resample("2T").count()["foo"]
    assert_eq(result, expected)

    q = result.simplify()
    eq = df["foo"].resample("2T").count().simplify()
    assert q._name == eq._name
