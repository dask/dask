from __future__ import annotations

import numpy as np
import pytest
from dask.utils import M

from dask_expr import from_pandas
from dask_expr._util import DASK_GT_20231201
from dask_expr.tests._util import _backend_library, assert_eq, xfail_gpu

# Set DataFrame backend for this module
pd = _backend_library()


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x // 7  # Not unique; duplicates span different partitions
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_median(pdf, df):
    assert_eq(df.repartition(npartitions=1).median(axis=0), pdf.median(axis=0))
    assert_eq(df.repartition(npartitions=1).x.median(), pdf.x.median())
    assert_eq(df.x.median_approximate(), 49.0, atol=1)
    assert_eq(df.median_approximate(), pdf.median(), atol=1)

    # Ensure `median` redirects to `median_approximate` appropriately
    for axis in [None, 0, "rows"]:
        with pytest.raises(
            NotImplementedError, match="See the `median_approximate` method instead"
        ):
            df.median(axis=axis)

    with pytest.raises(
        NotImplementedError, match="See the `median_approximate` method instead"
    ):
        df.x.median()


def test_min_dt(pdf):
    pdf["dt"] = "a"
    df = from_pandas(pdf, npartitions=10)
    assert_eq(df.min(numeric_only=True), pdf.min(numeric_only=True))
    assert_eq(df.max(numeric_only=True), pdf.max(numeric_only=True))
    assert_eq(df.count(numeric_only=True), pdf.count(numeric_only=True))
    assert_eq(df.mean(numeric_only=True), pdf.mean(numeric_only=True))


@pytest.mark.parametrize(
    "series",
    [
        [0, 1, 0, 0, 1, 0],  # not monotonic
        [0, 1, 1, 2, 2, 3],  # monotonic increasing
        [0, 1, 2, 3, 4, 5],  # strictly monotonic increasing
        [0, 0, 0, 0, 0, 0],  # both monotonic increasing and monotonic decreasing
        [0, 1, 2, 0, 1, 2],  # Partitions are individually monotonic; whole series isn't
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("cls", ["Series", "Index"])
def test_monotonic(series, reverse, cls):
    if reverse:
        series = series[::-1]
    pds = pd.Series(series, index=series)
    ds = from_pandas(pds, 2, sort=False)
    if cls == "Index":
        pds = pds.index
        ds = ds.index

    # assert_eq fails due to numpy.bool vs. plain bool mismatch
    # See https://github.com/dask/dask/pull/10671
    assert ds.is_monotonic_increasing.compute() == pds.is_monotonic_increasing
    assert ds.is_monotonic_decreasing.compute() == pds.is_monotonic_decreasing


@pytest.mark.parametrize("split_every", [False, None, 5])
@pytest.mark.parametrize("split_out", [1, True])
def test_drop_duplicates(pdf, df, split_every, split_out):
    assert_eq(
        df.drop_duplicates(split_every=split_every, split_out=split_out),
        pdf.drop_duplicates(),
        check_index=split_out is not True,
    )
    assert_eq(
        df.x.drop_duplicates(split_every=split_every, split_out=split_out),
        pdf.x.drop_duplicates(),
        check_index=split_out is not True,
    )


@pytest.mark.parametrize("split_every", [False, None, 5])
@pytest.mark.parametrize("split_out", [1, True])
def test_value_counts(pdf, df, split_every, split_out):
    assert_eq(
        df.x.value_counts(split_every=split_every, split_out=split_out),
        pdf.x.value_counts(),
        check_index=split_out is not True,
    )


@pytest.mark.parametrize("split_every", [None, 5])
@pytest.mark.parametrize("split_out", [1, True])
def test_unique(pdf, df, split_every, split_out):
    assert_eq(
        df.x.unique(split_every=split_every, split_out=split_out),
        pd.Series(pdf.x.unique(), name="x"),
        check_index=split_out is not True,
    )


@pytest.mark.parametrize(
    "reduction", ["sum", "prod", "min", "max", "any", "all", "count"]
)
@pytest.mark.parametrize(
    "split_every,expect_tasks", [(False, 22), (None, 24), (5, 24), (2, 32)]
)
def test_dataframe_split_every(pdf, df, split_every, expect_tasks, reduction):
    assert_eq(
        getattr(df, reduction)(split_every=split_every),
        getattr(pdf, reduction)(),
    )
    q = getattr(df, reduction)(split_every=split_every).optimize(fuse=False)
    assert len(q.__dask_graph__()) == expect_tasks


@pytest.mark.parametrize(
    "split_every,expect_tasks", [(False, 55), (None, 59), (5, 59), (2, 75)]
)
def test_dataframe_mode_split_every(pdf, df, split_every, expect_tasks):
    assert_eq(df.mode(split_every=split_every), pdf.mode())
    q = df.mode(split_every=split_every).optimize(fuse=False)
    assert len(q.__dask_graph__()) == expect_tasks


@pytest.mark.parametrize(
    "reduction", ["sum", "prod", "min", "max", "any", "all", "mode", "count"]
)
@pytest.mark.parametrize(
    "split_every,expect_tasks", [(False, 32), (None, 34), (5, 34), (2, 42)]
)
def test_series_split_every(pdf, df, split_every, expect_tasks, reduction):
    assert_eq(
        getattr(df.x, reduction)(split_every=split_every),
        getattr(pdf.x, reduction)(),
    )
    q = getattr(df.x, reduction)(split_every=split_every).optimize(fuse=False)
    assert len(q.__dask_graph__()) == expect_tasks


@pytest.mark.parametrize("split_every", [-1, 0, 1])
@pytest.mark.parametrize(
    "reduction",
    [
        "sum",
        "prod",
        "min",
        "max",
        "any",
        "all",
        "mode",
        "count",
        "nunique_approx",
    ],
)
def test_split_every_lt2(df, reduction, split_every):
    with pytest.raises(ValueError, match="split_every must be greater than 1 or False"):
        # TODO validate parameters before graph materialization
        getattr(df.x, reduction)(split_every=split_every).__dask_graph__()


@pytest.mark.parametrize("split_every", [-1, 0, 1])
@pytest.mark.parametrize("reduction", ["drop_duplicates", "unique", "value_counts"])
def test_split_every_lt2_split_out(df, reduction, split_every):
    """split_out=True ignores split_every; force split_out=1"""
    with pytest.raises(ValueError, match="split_every must be greater than 1 or False"):
        # TODO validate parameters before graph materialization
        getattr(df.x, reduction)(split_out=1, split_every=split_every).__dask_graph__()


@pytest.mark.parametrize("split_every", [None, False, 2, 10])
def test_nunique_approx(split_every, pdf, df):
    approx = df.nunique_approx(split_every=split_every).compute()
    exact = len(df.drop_duplicates())
    assert abs(approx - exact) <= 2 or abs(approx - exact) / exact < 0.05


def test_unique_base(df, pdf):
    with pytest.raises(
        AttributeError, match="'DataFrame' object has no attribute 'unique'"
    ):
        df.unique()

    # pandas returns a numpy array while we return a Series/Index
    assert_eq(df.x.unique(), pd.Series(pdf.x.unique(), name="x"), check_index=False)
    assert_eq(df.index.unique(split_out=1), pd.Index(pdf.index.unique()))
    np.testing.assert_array_equal(
        df.index.unique().compute().sort_values().values,
        pd.Index(pdf.index.unique()).values,
    )


@pytest.mark.skipif(not DASK_GT_20231201, reason="needed updates in dask")
def test_value_counts_split_out_normalize(df, pdf):
    result = df.x.value_counts(split_out=2, normalize=True)
    expected = pdf.x.value_counts(normalize=True)
    assert_eq(result, expected)


@pytest.mark.parametrize("method", ["sum", "prod", "product"])
@pytest.mark.parametrize("min_count", [0, 9])
def test_series_agg_with_min_count(method, min_count):
    df = pd.DataFrame([[1]], columns=["a"])
    ddf = from_pandas(df, npartitions=1)
    func = getattr(ddf["a"], method)
    result = func(min_count=min_count).compute()
    if min_count == 0:
        assert result == 1
    else:
        # TODO: dtype is wrong
        assert_eq(result, np.nan, check_dtype=False)

    assert_eq(
        getattr(ddf, method)(min_count=min_count),
        getattr(df, method)(min_count=min_count),
    )


@pytest.mark.parametrize(
    "func",
    [
        M.max,
        M.min,
        M.any,
        M.all,
        M.sum,
        M.prod,
        M.product,
        M.count,
        M.mean,
        M.std,
        M.var,
        pytest.param(
            M.idxmin, marks=xfail_gpu("https://github.com/rapidsai/cudf/issues/9602")
        ),
        pytest.param(
            M.idxmax, marks=xfail_gpu("https://github.com/rapidsai/cudf/issues/9602")
        ),
        pytest.param(
            lambda df: df.size,
            marks=pytest.mark.xfail(reason="scalars don't work yet"),
        ),
    ],
)
def test_reductions(func, pdf, df):
    result = func(df)
    assert result.known_divisions
    assert_eq(result, func(pdf))
    result = func(df.x)
    assert not result.known_divisions
    assert_eq(result, func(pdf.x))
    # check_dtype False because sub-selection of columns that is pushed through
    # is not reflected in the meta calculation
    assert_eq(func(df)["x"], func(pdf)["x"], check_dtype=False)


@pytest.mark.parametrize(
    "func",
    [
        M.max,
        M.min,
        M.any,
        M.all,
        lambda idx: idx.size,
    ],
)
def test_index_reductions(func, pdf, df):
    result = func(df.index)
    assert not result.known_divisions
    assert_eq(result, func(pdf.index))


@pytest.mark.parametrize(
    "func",
    [
        lambda idx: idx.index,
        M.sum,
        M.prod,
        M.count,
        M.mean,
        M.std,
        M.var,
        M.idxmin,
        M.idxmax,
    ],
)
def test_unimplemented_on_index(func, pdf, df):
    # Methods/properties of Series that don't exist on Index
    with pytest.raises(AttributeError):
        func(pdf.index)
    with pytest.raises(AttributeError, match="'Index' object has no attribute '"):
        func(df.index)


def test_reduction_on_empty_df():
    pdf = pd.DataFrame()
    df = from_pandas(pdf)
    assert_eq(df.sum(), pdf.sum())


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize(
    "skipna",
    [
        True,
        pytest.param(
            False, marks=xfail_gpu("cudf requires skipna=True when nulls are present.")
        ),
    ],
)
@pytest.mark.parametrize("ddof", [1, 2])
def test_std_kwargs(axis, skipna, ddof):
    pdf = pd.DataFrame(
        {"x": range(30), "y": [1, 2, None] * 10, "z": ["dog", "cat"] * 15}
    )
    df = from_pandas(pdf, npartitions=3)
    assert_eq(
        pdf.std(axis=axis, skipna=skipna, ddof=ddof, numeric_only=True),
        df.std(axis=axis, skipna=skipna, ddof=ddof, numeric_only=True),
    )
