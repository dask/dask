from __future__ import annotations

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


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
