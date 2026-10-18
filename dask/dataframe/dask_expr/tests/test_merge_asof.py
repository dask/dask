from __future__ import annotations

import pytest

from dask.dataframe.dask_expr import from_pandas, merge_asof
from dask.dataframe.dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


@pytest.mark.parametrize("direction", ["backward", "forward", "nearest"])
@pytest.mark.parametrize("indexed", [False, True])
def test_merge_asof_tail(direction: str, indexed: bool) -> None:
    left = pd.DataFrame({"a": list(range(12)), "left_val": list(range(12))})
    right = pd.DataFrame({"a": [0, 3, 6, 9, 12], "right_val": [10, 20, 30, 40, 50]})
    kwargs = {"direction": direction}
    if indexed:
        left = left.set_index("a")
        right = right.set_index("a")
        kwargs.update(left_index=True, right_index=True)
    else:
        kwargs["on"] = "a"

    result = merge_asof(
        from_pandas(left, npartitions=3),
        from_pandas(right, npartitions=2),
        **kwargs,
    )
    expected = pd.merge_asof(left, right, **kwargs).tail(2)
    assert_eq(result.tail(2, compute=False), expected, check_index=indexed)
    assert_eq(result.tail(2), expected, check_index=indexed)


def test_merge_asof_indexed():
    A = pd.DataFrame(
        {"left_val": list("abcd" * 3)},
        index=[1, 3, 7, 9, 10, 13, 14, 17, 20, 24, 25, 28],
    )
    a = from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {"right_val": list("xyz" * 4)},
        index=[1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26],
    )
    b = from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)

    assert_eq(c, C)


def test_merge_asof_indexed_optimiser():
    A = pd.DataFrame(
        {"left_val": list("abcd" * 3), "foo": 1},
        index=[1, 3, 7, 9, 10, 13, 14, 17, 20, 24, 25, 28],
    )
    a = from_pandas(A, npartitions=4)
    B = pd.DataFrame(
        {"right_val": list("xyz" * 4), "bar": 2},
        index=[1, 2, 3, 6, 7, 10, 12, 14, 16, 19, 23, 26],
    )
    b = from_pandas(B, npartitions=3)

    C = pd.merge_asof(A, B, left_index=True, right_index=True)
    c = merge_asof(a, b, left_index=True, right_index=True)
    c = c.optimize(fuse=False)
    c = c[["left_val", "right_val"]]

    assert_eq(c, C[["left_val", "right_val"]])


def test_merge_asof_on_basic():
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = from_pandas(B, npartitions=2)

    C = pd.merge_asof(A, B, on="a")
    c = merge_asof(a, b, on="a")
    # merge_asof does not preserve index
    assert_eq(c, C, check_index=False)


def test_merge_asof_one_partition():
    left = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    right = pd.DataFrame({"a": [1, 2, 3], "c": [4, 5, 6]})

    ddf_left = from_pandas(left, npartitions=1)
    ddf_left = ddf_left.set_index("a", sort=True)
    ddf_right = from_pandas(right, npartitions=1)
    ddf_right = ddf_right.set_index("a", sort=True)

    result = merge_asof(
        ddf_left, ddf_right, left_index=True, right_index=True, direction="nearest"
    )
    expected = pd.merge_asof(
        left.set_index("a"),
        right.set_index("a"),
        left_index=True,
        right_index=True,
        direction="nearest",
    )
    assert_eq(result, expected)
