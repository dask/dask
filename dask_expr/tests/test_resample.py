from itertools import product

import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


def resample(df, freq, how="mean", **kwargs):
    return getattr(df.resample(freq, **kwargs), how)()


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

        if api != "ohlc":
            # ohlc actually gives back a DataFrame, so this doesn't work
            q = result.simplify()
            eq = getattr(df["foo"].resample("2T"), api)().simplify()
            assert q._name == eq._name


@pytest.mark.parametrize(
    ["obj", "method", "npartitions", "freq", "closed", "label"],
    list(
        product(
            ["series", "frame"],
            ["count", "mean", "ohlc"],
            [2, 5],
            ["30min", "h", "d", "w"],
            ["right", "left"],
            ["right", "left"],
        )
    ),
)
def test_series_resample(obj, method, npartitions, freq, closed, label):
    index = lib.date_range("1-1-2000", "2-15-2000", freq="h")
    index = index.union(lib.date_range("4-15-2000", "5-15-2000", freq="h"))
    if obj == "series":
        ps = lib.Series(range(len(index)), index=index)
    elif obj == "frame":
        ps = lib.DataFrame({"a": range(len(index))}, index=index)
    ds = from_pandas(ps, npartitions=npartitions)
    # Series output

    result = resample(ds, freq, how=method, closed=closed, label=label)
    expected = resample(ps, freq, how=method, closed=closed, label=label)

    assert_eq(result, expected, check_dtype=False)

    divisions = result.divisions

    assert expected.index[0] == divisions[0]
    assert expected.index[-1] == divisions[-1]


def test_resample_agg(df, pdf):
    def my_sum(vals, foo=None, *, bar=None):
        return vals.sum()

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


@pytest.mark.parametrize("method", ["count", "nunique", "size", "sum"])
def test_resample_has_correct_fill_value(method):
    index = lib.date_range("2000-01-01", "2000-02-15", freq="h")
    index = index.union(lib.date_range("4-15-2000", "5-15-2000", freq="h"))
    ps = lib.Series(range(len(index)), index=index)
    ds = from_pandas(ps, npartitions=2)

    assert_eq(
        getattr(ds.resample("30min"), method)(), getattr(ps.resample("30min"), method)()
    )
