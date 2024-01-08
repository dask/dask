from __future__ import annotations

import numpy as np
import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame({"x": range(100)})
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
    pds = lib.Series(series, index=series)
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
        lib.Series(pdf.x.unique(), name="x"),
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
    assert_eq(df.x.unique(), lib.Series(pdf.x.unique(), name="x"), check_index=False)
    assert_eq(df.index.unique(split_out=1), lib.Index(pdf.index.unique()))
    np.testing.assert_array_equal(
        df.index.unique().compute().sort_values().values,
        lib.Index(pdf.index.unique()).values,
    )
