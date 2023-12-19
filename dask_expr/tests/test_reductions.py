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


def test_monotonic():
    pdf = lib.DataFrame(
        {
            "a": range(20),
            "b": list(range(20))[::-1],
            "c": [0] * 20,
            "d": [0] * 5 + [1] * 5 + [0] * 10,
        }
    )
    df = from_pandas(pdf, 4)

    for c in df.columns:
        assert assert_eq(
            df[c].is_monotonic_increasing,
            pdf[c].is_monotonic_increasing,
            # https://github.com/dask/dask/pull/10671
            check_dtype=False,
        )
        assert assert_eq(
            df[c].is_monotonic_decreasing,
            pdf[c].is_monotonic_decreasing,
            # https://github.com/dask/dask/pull/10671
            check_dtype=False,
        )


@pytest.mark.parametrize("reduction", ["drop_duplicates", "value_counts"])
@pytest.mark.parametrize("split_every", [None, 5])
@pytest.mark.parametrize("split_out", [1, True])
def test_reductions_split_every_split_out(pdf, df, split_every, split_out, reduction):
    assert_eq(
        getattr(df.x, reduction)(split_every=split_every, split_out=split_out),
        getattr(pdf.x, reduction)(),
        check_index=split_out is not True,
    )

    if reduction == "drop_duplicates":
        assert_eq(
            getattr(df, reduction)(split_every=split_every, split_out=split_out),
            getattr(pdf, reduction)(),
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
    "reduction", ["sum", "prod", "min", "max", "any", "all", "mode", "count"]
)
@pytest.mark.parametrize("split_every", [False, 5])
def test_reductions_split_every_split_out(pdf, df, split_every, reduction):
    assert_eq(
        getattr(df.x, reduction)(split_every=split_every),
        getattr(pdf.x, reduction)(),
    )
    q = getattr(df.x, reduction)(split_every=split_every).optimize(fuse=False)
    if split_every is False:
        assert len(q.__dask_graph__()) == 32
    else:
        assert len(q.__dask_graph__()) == 34
    assert_eq(
        getattr(df, reduction)(split_every=split_every),
        getattr(pdf, reduction)(),
    )


@pytest.mark.parametrize("split_every", [None, 2, 10])
@pytest.mark.parametrize("npartitions", [2, 20])
def test_split_every(split_every, npartitions, pdf, df):
    approx = df.nunique_approx(split_every=split_every).compute(scheduler="sync")
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
