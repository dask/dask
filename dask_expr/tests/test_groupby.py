import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import from_pandas


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": list(range(10)) * 10, "y": range(100)})
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=4)


@pytest.mark.xfail(reason="Cannot group on a Series yet")
def test_groupby_unsupported_by(pdf, df):
    assert_eq(df.groupby(df.x).sum(), pdf.groupby(pdf.x).sum())


@pytest.mark.parametrize("api", ["sum", "mean", "min", "max", "prod", "first", "last"])
@pytest.mark.parametrize("numeric_only", [True, False])
def test_groupby_numeric(pdf, df, api, numeric_only):
    g = df.groupby("x")
    agg = getattr(g, api)(numeric_only=numeric_only)

    expect = getattr(pdf.groupby("x"), api)(numeric_only=numeric_only)
    assert_eq(agg, expect)


def test_groupby_count(pdf, df):
    g = df.groupby("x")
    agg = g.count()

    expect = pdf.groupby("x").count()
    assert_eq(agg, expect)


def test_groupby_mean_slice(pdf, df):
    g = df.groupby("x")
    agg = g.y.mean()

    expect = pdf.groupby("x").y.mean()
    assert_eq(agg, expect)


@pytest.mark.parametrize(
    "spec",
    [
        {"x": "count"},
        {"x": ["count"]},
        {"x": ["count"], "y": "mean"},
        {"x": ["sum", "mean"]},
        ["min", "mean"],
        "sum",
    ],
)
def test_groupby_agg(pdf, df, spec):
    g = df.groupby("x")
    agg = g.agg(spec)

    expect = pdf.groupby("x").agg(spec)
    assert_eq(agg, expect)


def test_groupby_getitem_agg(pdf, df):
    assert_eq(df.groupby("x").y.sum(), pdf.groupby("x").y.sum())
    assert_eq(df.groupby("x")[["y"]].sum(), pdf.groupby("x")[["y"]].sum())


def test_groupby_agg_column_projection(pdf, df):
    g = df.groupby("x")
    agg = g.agg({"x": "count"}).simplify()

    assert list(agg.frame.columns) == ["x"]
    expect = pdf.groupby("x").agg({"x": "count"})
    assert_eq(agg, expect)
