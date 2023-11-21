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


@pytest.mark.parametrize("kwargs", [{}, {"closed": "left"}])
@pytest.mark.parametrize(
    "api",
    [
        "count",
        "prod",
        "mean",
        "sum",
        "min",
        "max",
        "first",
        "last",
        "var",
        "std",
        "size",
        "nunique",
        "median",
        "quantile",
        "ohlc",
        "sem",
    ],
)
def test_resample_apis(df, pdf, api, kwargs):
    result = getattr(df.resample("2T", **kwargs), api)()
    expected = getattr(pdf.resample("2T", **kwargs), api)()
    assert_eq(result, expected)

    # No column output
    if api not in ("size",):
        result = getattr(df.resample("2T"), api)()["foo"]
        expected = getattr(pdf.resample("2T"), api)()["foo"]
        assert_eq(result, expected)

        q = result.simplify()
        eq = getattr(df["foo"].resample("2T"), api)().simplify()
        assert q._name == eq._name


def test_resample_agg(df, pdf):
    def my_sum(vals, foo=None, *, bar=None):
        return vals.sum()

    # TODO: w/o .compute() assert_eq fails w/ dtype assertion error
    result = df.resample("2T").agg(my_sum, "foo", bar="bar")
    expected = pdf.resample("2T").agg(my_sum, "foo", bar="bar")
    assert_eq(result, expected)

    result = df.resample("2T").agg(my_sum)["foo"]
    expected = pdf.resample("2T").agg(my_sum)["foo"]
    assert_eq(result, expected)

    # simplify up disabled for `agg`, function may access other columns
    q = df.resample("2T").agg(my_sum)["foo"].simplify()
    eq = df["foo"].resample("2T").agg(my_sum).simplify()
    assert q._name != eq._name
