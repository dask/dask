from __future__ import annotations

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
