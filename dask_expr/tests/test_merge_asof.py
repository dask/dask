from dask_expr import from_pandas, merge_asof
from dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


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


def test_merge_asof_on_basic():
    A = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    a = from_pandas(A, npartitions=2)
    B = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    b = from_pandas(B, npartitions=2)

    C = pd.merge_asof(A, B, on="a")
    c = merge_asof(a, b, on="a")
    # merge_asof does not preserve index
    assert_eq(c, C, check_index=False)
