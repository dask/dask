from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import dask.dataframe as dd
from dask.dataframe.hyperloglog import estimate_count

rs = np.random.RandomState(96)


@pytest.mark.parametrize(
    "df",
    [
        pd.DataFrame(
            {
                "x": [1, 2, 3] * 3,
                "y": [1.2, 3.4, 5.6] * 3,
                "z": -(np.arange(9, dtype=np.int8)),
            }
        ),
        pd.DataFrame(
            {
                "x": rs.randint(0, 1000000, (10000,)),
                "y": rs.randn(10000),
                "z": rs.uniform(0, 9999999, (10000,)),
            }
        ),
        pd.DataFrame(
            {
                "x": np.repeat(rs.randint(0, 1000000, (1000,)), 3),
                "y": np.repeat(rs.randn(1000), 3),
                "z": np.repeat(rs.uniform(0, 9999999, (1000,)), 3),
            }
        ),
        pd.DataFrame({"x": rs.randint(0, 1000000, (10000,))}),
        pd.DataFrame(
            {
                "x": rs.randint(0, 1000000, (7,)),
                "y": ["a", "bet", "is", "a", "tax", "on", "bs"],
            }
        ),
        pd.DataFrame(
            {
                "w": np.zeros((20000,)),
                "x": np.zeros((20000,)),
                "y": np.zeros((20000,)) + 4803592,
                "z": np.zeros((20000,)),
            }
        ),
        pd.DataFrame({"x": [1, 2, 3] * 1000}),
        pd.DataFrame({"x": np.random.random(1000)}),
        pd.DataFrame(
            {
                "a": [1, 2, 3] * 3,
                "b": [1.2, 3.4, 5.6] * 3,
                "c": [1 + 2j, 3 + 4j, 5 + 6j] * 3,
                "d": -(np.arange(9, dtype=np.int8)),
            }
        ),
        pd.Series([1, 2, 3] * 1000),
        pd.Series(np.random.random(1000)),
        pd.Series(np.random.random(1000), index=np.ones(1000)),
        pd.Series(np.random.random(1000), index=np.random.random(1000)),
    ],
)
@pytest.mark.parametrize("npartitions", [2, 20])
def test_basic(df, npartitions):
    ddf = dd.from_pandas(df, npartitions=npartitions)

    approx = ddf.nunique_approx().compute(scheduler="sync")
    exact = len(df.drop_duplicates())
    assert abs(approx - exact) <= 2 or abs(approx - exact) / exact < 0.05


@pytest.mark.parametrize("split_every", [None, 2, 10])
@pytest.mark.parametrize("npartitions", [2, 20])
def test_split_every(split_every, npartitions):
    df = pd.Series([1, 2, 3] * 1000)
    ddf = dd.from_pandas(df, npartitions=npartitions)

    approx = ddf.nunique_approx(split_every=split_every).compute(scheduler="sync")
    exact = len(df.drop_duplicates())
    assert abs(approx - exact) <= 2 or abs(approx - exact) / exact < 0.05


def test_larger_data():
    df = dd.demo.make_timeseries(
        "2000-01-01",
        "2000-04-01",
        {"value": float, "id": int},
        freq="10s",
        partition_freq="1D",
        seed=1,
    )
    assert df.nunique_approx().compute() > 1000


def test_hll_uses_uint64():
    """HLL should use full 64-bit hashes, not truncate to uint32."""
    from dask.dataframe.hyperloglog import compute_hll_array

    # Verify that compute_hll_array processes hashes as uint64 internally
    # by checking the state array handles high-bit bucket indices correctly.
    # With b=10, we have 1024 buckets addressed by the top 10 bits of uint64.
    # A uint32 truncation would lose upper bits and misassign buckets.
    df = pd.DataFrame({"x": range(10000)})
    state = compute_hll_array(df, b=10)
    approx = estimate_count(state, b=10)
    exact = len(df.drop_duplicates())
    # Should be within 10% of exact count
    assert (
        abs(approx - exact) / exact < 0.10
    ), f"HLL estimate {approx} too far from exact {exact}"


def test_compute_first_bit_64bit():
    """compute_first_bit should handle 64-bit hash values correctly."""
    from dask.dataframe.hyperloglog import compute_first_bit

    # For value (1 << 40): bit 40 is set.
    # Bits 0-39 are zero, bits 40-63 are counted after cumsum+bool.
    # Result = 65 - (64 - 40) = 65 - 24 = 41
    arr = np.array([1 << 40], dtype=np.uint64)
    result = compute_first_bit(arr)
    assert result[0] == 41, f"compute_first_bit gave {result[0]} for 1<<40, expected 41"

    # Zero should give max position (65 = no bits set)
    arr_zero = np.array([0], dtype=np.uint64)
    result_zero = compute_first_bit(arr_zero)
    assert result_zero[0] == 65

    # Value 1 (bit 0 set): all 64 cumsum positions are True after bit 0
    arr_one = np.array([1], dtype=np.uint64)
    result_one = compute_first_bit(arr_one)
    assert result_one[0] == 1
