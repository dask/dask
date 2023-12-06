import numpy as np
import pytest

from dask_expr import from_pandas
from dask_expr._groupby import GroupByUDFBlockwise
from dask_expr._reductions import TreeReduce
from dask_expr._shuffle import Shuffle
from dask_expr.tests._util import _backend_library, assert_eq, xfail_gpu

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame({"x": list(range(10)) * 10, "y": range(100), "z": 1})
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=4)


@pytest.mark.xfail(reason="Cannot group on a Series yet")
def test_groupby_unsupported_by(pdf, df):
    assert_eq(df.groupby(df.x).sum(), pdf.groupby(pdf.x).sum())


@pytest.mark.parametrize(
    "api", ["sum", "mean", "min", "max", "prod", "first", "last", "var", "std"]
)
@pytest.mark.parametrize(
    "numeric_only",
    [
        pytest.param(True, marks=xfail_gpu("numeric_only not supported by cudf")),
        False,
    ],
)
def test_groupby_numeric(pdf, df, api, numeric_only):
    if not numeric_only and api in {"var", "std"}:
        pytest.xfail("not implemented")
    g = df.groupby("x")
    agg = getattr(g, api)(numeric_only=numeric_only)

    expect = getattr(pdf.groupby("x"), api)(numeric_only=numeric_only)
    assert_eq(agg, expect)

    g = df.y.groupby(df.x)
    agg = getattr(g, api)()

    expect = getattr(pdf.y.groupby(pdf.x), api)()
    assert_eq(agg, expect)

    g = df.groupby("x")
    agg = getattr(g, api)(numeric_only=numeric_only)["y"]

    expect = getattr(pdf.groupby("x"), api)(numeric_only=numeric_only)["y"]
    assert_eq(agg, expect)

    pdf = pdf.set_index("x")
    df = from_pandas(pdf, npartitions=10, sort=False)
    g = df.groupby("x")
    agg = getattr(g, api)()
    expect = getattr(pdf.groupby("x"), api)(numeric_only=numeric_only)
    assert_eq(agg, expect)

    g = df.groupby(["x", "z"])
    agg = getattr(g, api)()
    expect = getattr(pdf.groupby(["x", "z"]), api)(numeric_only=numeric_only)
    assert_eq(agg, expect)


@pytest.mark.parametrize(
    "func",
    [
        "count",
        pytest.param(
            "value_counts", marks=xfail_gpu("value_counts not supported by cudf")
        ),
        "size",
    ],
)
def test_groupby_no_numeric_only(pdf, func):
    pdf = pdf.drop(columns="z")
    df = from_pandas(pdf, npartitions=10)
    g = df.groupby("x")
    agg = getattr(g, func)()

    expect = getattr(pdf.groupby("x"), func)()
    assert_eq(agg, expect)

    g = df.y.groupby(df.x)
    agg = getattr(g, func)()

    expect = getattr(pdf.y.groupby(pdf.x), func)()
    assert_eq(agg, expect)


def test_groupby_mean_slice(pdf, df):
    g = df.groupby("x")
    agg = g.y.mean()

    expect = pdf.groupby("x").y.mean()
    assert_eq(agg, expect)


def test_groupby_nunique(df, pdf):
    with pytest.raises(AssertionError):
        df.groupby("x").nunique()

    assert_eq(df.groupby("x").y.nunique(split_out=1), pdf.groupby("x").y.nunique())
    assert_eq(df.groupby("x").y.nunique(split_out=True), pdf.groupby("x").y.nunique())
    assert df.groupby("x").y.nunique().npartitions == df.npartitions
    assert_eq(df.y.groupby(df.x).nunique(split_out=1), pdf.y.groupby(pdf.x).nunique())


def test_groupby_series(pdf, df):
    pdf_result = pdf.groupby(pdf.x).sum()
    result = df.groupby(df.x).sum()
    assert_eq(result, pdf_result)
    result = df.groupby("x").sum()
    assert_eq(result, pdf_result)

    df2 = from_pandas(lib.DataFrame({"a": [1, 2, 3]}), npartitions=2)

    with pytest.raises(ValueError, match="DataFrames columns"):
        df.groupby(df2.a)


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


@pytest.mark.parametrize(
    "spec",
    [
        "sum",
        ["sum"],
        ["sum", "mean"],
    ],
)
def test_series_groupby_agg(pdf, df, spec):
    g = df.y.groupby(df.x)
    agg = g.agg(spec)

    expect = pdf.y.groupby(pdf.x).agg(spec)
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


def test_groupby_split_every(pdf):
    df = from_pandas(pdf, npartitions=16)
    query = df.groupby("x").sum()
    tree_reduce_node = list(query.optimize(fuse=False).find_operations(TreeReduce))
    assert len(tree_reduce_node) == 1
    assert tree_reduce_node[0].split_every == 8

    query = df.groupby("x").aggregate({"y": "sum"})
    tree_reduce_node = list(query.optimize(fuse=False).find_operations(TreeReduce))
    assert len(tree_reduce_node) == 1
    assert tree_reduce_node[0].split_every == 8


def test_groupby_index(pdf):
    pdf = pdf.set_index("x")
    df = from_pandas(pdf, npartitions=10)
    result = df.groupby(df.index).sum()
    expected = pdf.groupby(pdf.index).sum()
    assert_eq(result, expected)
    assert_eq(result["y"], expected["y"])

    result = df.groupby(df.index).var()
    expected = pdf.groupby(pdf.index).var()
    assert_eq(result, expected)
    assert_eq(result["y"], expected["y"])

    result = df.groupby(df.index).agg({"y": "sum"})
    expected = pdf.groupby(pdf.index).agg({"y": "sum"})
    assert_eq(result, expected)


def test_split_out_automatically():
    pdf = lib.DataFrame({"a": [1, 2, 3] * 1_000, "b": 1, "c": 1, "d": 1})
    df = from_pandas(pdf, npartitions=500)
    q = df.groupby("a").sum()
    assert q.optimize().npartitions == 1
    expected = pdf.groupby("a").sum()
    assert_eq(q, expected)

    q = df.groupby(["a", "b"]).sum()
    assert q.optimize().npartitions == 5
    expected = pdf.groupby(["a", "b"]).sum()
    assert_eq(q, expected)

    q = df.groupby(["a", "b", "c"]).sum()
    assert q.optimize().npartitions == 10
    expected = pdf.groupby(["a", "b", "c"]).sum()
    assert_eq(q, expected)


def test_groupby_apply(df, pdf):
    def test(x):
        x["new"] = x.sum().sum()
        return x

    assert_eq(df.groupby(df.x).apply(test), pdf.groupby(pdf.x).apply(test))
    assert_eq(
        df.groupby(df.x, group_keys=False).apply(test),
        pdf.groupby(pdf.x, group_keys=False).apply(test),
    )
    assert_eq(df.groupby("x").apply(test), pdf.groupby("x").apply(test))
    assert_eq(
        df.groupby("x").apply(test, meta=pdf.groupby("x").apply(test).head(0)),
        pdf.groupby("x").apply(test),
    )

    query = df.groupby("x").apply(test).optimize(fuse=False)
    assert query.expr.find_operations(Shuffle)
    assert query.expr.find_operations(GroupByUDFBlockwise)

    query = df.groupby("x")[["y"]].apply(test).simplify()
    expected = df[["x", "y"]].groupby("x")[["y"]].apply(test).simplify()
    assert query._name == expected._name
    assert_eq(query, pdf.groupby("x")[["y"]].apply(test))


def test_groupby_transform(df, pdf):
    def test(x):
        return x

    assert_eq(df.groupby(df.x).transform(test), pdf.groupby(pdf.x).transform(test))
    assert_eq(df.groupby("x").transform(test), pdf.groupby("x").transform(test))
    assert_eq(
        df.groupby("x").transform(test, meta=pdf.groupby("x").transform(test).head(0)),
        pdf.groupby("x").transform(test),
    )

    query = df.groupby("x").transform(test).optimize(fuse=False)
    assert query.expr.find_operations(Shuffle)
    assert query.expr.find_operations(GroupByUDFBlockwise)

    query = df.groupby("x")[["y"]].transform(test).simplify()
    expected = df[["x", "y"]].groupby("x")[["y"]].transform(test).simplify()
    assert query._name == expected._name
    assert_eq(query, pdf.groupby("x")[["y"]].transform(test))


def test_groupby_shift(df, pdf):
    assert_eq(df.groupby(df.x).shift(periods=1), pdf.groupby(pdf.x).shift(periods=1))
    assert_eq(df.groupby("x").shift(periods=1), pdf.groupby("x").shift(periods=1))
    assert_eq(
        df.groupby("x").shift(
            periods=1, meta=pdf.groupby("x").shift(periods=1).head(0)
        ),
        pdf.groupby("x").shift(periods=1),
    )

    query = df.groupby("x").shift(periods=1).optimize(fuse=False)
    assert query.expr.find_operations(Shuffle)
    assert query.expr.find_operations(GroupByUDFBlockwise)

    query = df.groupby("x")[["y"]].shift(periods=1).simplify()
    expected = df[["x", "y"]].groupby("x")[["y"]].shift(periods=1).simplify()
    assert query._name == expected._name
    assert_eq(query, pdf.groupby("x")[["y"]].shift(periods=1))


@pytest.mark.parametrize("api", ["sum", "mean", "min", "max", "prod", "var", "std"])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("split_out", [1, 2])
def test_groupby_single_agg_split_out(pdf, df, api, sort, split_out):
    g = df.groupby("x", sort=sort)
    agg = getattr(g, api)(split_out=split_out)

    expect = getattr(pdf.groupby("x", sort=sort), api)()
    assert_eq(agg, expect, sort_results=not sort)

    g = df.y.groupby(df.x, sort=sort)
    agg = getattr(g, api)(split_out=split_out)
    expect = getattr(pdf.y.groupby(pdf.x, sort=sort), api)()
    assert_eq(agg, expect, sort_results=not sort)


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
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("split_out", [1, 2])
def test_groupby_agg_split_out(pdf, df, spec, sort, split_out):
    g = df.groupby("x", sort=sort)
    agg = g.agg(spec, split_out=split_out)

    expect = pdf.groupby("x", sort=sort).agg(spec)
    assert_eq(agg, expect, sort_results=not sort)


def test_groupby_reduction_shuffle(df, pdf):
    q = df.groupby("x").sum(split_out=True)
    assert q.optimize().npartitions == df.npartitions
    expected = pdf.groupby("x").sum()
    assert_eq(q, expected)


def test_groupby_projection_split_out(df, pdf):
    pdf_result = pdf.groupby("x")["y"].sum()
    result = df.groupby("x")["y"].sum(split_out=2)
    assert_eq(result, pdf_result)

    pdf_result = pdf.groupby("y")["x"].sum()
    df = from_pandas(pdf, npartitions=50)
    result = df.groupby("y")["x"].sum(split_out=2)
    assert_eq(result, pdf_result)


def test_groupby_co_aligned_grouper(df, pdf):
    assert_eq(
        df[["y"]].groupby(df["x"]).sum(),
        pdf[["y"]].groupby(pdf["x"]).sum(),
    )


@pytest.mark.parametrize("func", ["var", "std"])
@pytest.mark.parametrize("observed", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
def test_groupby_var_dropna_observed(dropna, observed, func):
    df = lib.DataFrame(
        {
            "a": [11, 12, 31, 1, 2, 3, 4, 5, 6, 10],
            "b": lib.Categorical(values=[1] * 9 + [np.nan], categories=[1, 2]),
        }
    )
    ddf = from_pandas(df, npartitions=3)
    dd_result = getattr(ddf.groupby("b", observed=observed, dropna=dropna), func)()
    pdf_result = getattr(df.groupby("b", observed=observed, dropna=dropna), func)()
    assert_eq(dd_result, pdf_result)


def test_groupby_median(df, pdf):
    assert_eq(df.groupby("x").median(), pdf.groupby("x").median())
    q = df.groupby("x").median(split_out=2)
    assert q.optimize().npartitions == 2
    assert_eq(q, pdf.groupby("x").median())
    assert_eq(df.groupby("x")["y"].median(), pdf.groupby("x")["y"].median())
    assert_eq(df.groupby("x").median()["y"], pdf.groupby("x").median()["y"])


def test_groupby_rolling():
    df = lib.DataFrame(
        {
            "column1": range(600),
            "group1": 5 * ["g" + str(i) for i in range(120)],
        },
        index=lib.date_range("20190101", periods=60).repeat(10),
    )

    ddf = from_pandas(df, npartitions=8)

    expected = df.groupby("group1").rolling("1D").sum()
    actual = ddf.groupby("group1").rolling("1D").sum()

    assert_eq(expected, actual, check_divisions=False)

    expected = df.groupby("group1").column1.rolling("1D").mean()
    actual = ddf.groupby("group1").column1.rolling("1D").mean()

    assert_eq(expected, actual, check_divisions=False)


def test_rolling_groupby_projection():
    df = lib.DataFrame(
        {
            "column1": range(600),
            "a": 1,
            "group1": 5 * ["g" + str(i) for i in range(120)],
        },
        index=lib.date_range("20190101", periods=60).repeat(10),
    )

    ddf = from_pandas(df, npartitions=8)

    actual = ddf.groupby("group1").rolling("1D").sum()["column1"]
    expected = df.groupby("group1").rolling("1D").sum()["column1"]

    assert_eq(expected, actual, check_divisions=False)

    optimal = (
        ddf[["group1", "column1"]].groupby("group1").rolling("1D").sum()["column1"]
    )

    assert actual.optimize()._name == (optimal.optimize()._name)
