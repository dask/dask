import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import Merge, from_pandas
from dask_expr._expr import Projection
from dask_expr._shuffle import Shuffle
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.mark.parametrize("how", ["left", "right", "inner", "outer"])
@pytest.mark.parametrize("shuffle_backend", ["tasks", "disk"])
def test_merge(how, shuffle_backend):
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)})
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"x": range(0, 20, 2), "z": range(10)})
    df2 = from_pandas(pdf2, 2)

    # Partition-wise merge with map_partitions
    df3 = df1.merge(df2, on="x", how=how, shuffle_backend=shuffle_backend)

    # Check result with/without fusion
    expect = pdf1.merge(pdf2, on="x", how=how)
    assert_eq(df3, expect, check_index=False)
    assert_eq(df3.optimize(), expect, check_index=False)


@pytest.mark.parametrize("how", ["left", "right", "inner", "outer"])
@pytest.mark.parametrize("pass_name", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("shuffle_backend", ["tasks", "disk"])
def test_merge_indexed(how, pass_name, sort, shuffle_backend):
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)}).set_index("x")
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"x": range(0, 20, 2), "z": range(10)}).set_index("x")
    df2 = from_pandas(pdf2, 2, sort=sort)

    if pass_name:
        left_on = right_on = "x"
        left_index = right_index = False
    else:
        left_on = right_on = None
        left_index = right_index = True

    df3 = df1.merge(
        df2,
        left_index=left_index,
        left_on=left_on,
        right_index=right_index,
        right_on=right_on,
        how=how,
        shuffle_backend=shuffle_backend,
    )

    # Check result with/without fusion
    expect = pdf1.merge(
        pdf2,
        left_index=left_index,
        left_on=left_on,
        right_index=right_index,
        right_on=right_on,
        how=how,
    )
    assert_eq(df3, expect)
    assert_eq(df3.optimize(), expect)


@pytest.mark.parametrize("how", ["left", "right", "inner", "outer"])
def test_broadcast_merge(how):
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)})
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"x": range(0, 20, 2), "z": range(10)})
    df2 = from_pandas(pdf2, 1)

    df3 = df1.merge(df2, on="x", how=how)

    # Check that we avoid the shuffle when allowed
    if how in ("left", "inner"):
        assert all(["Shuffle" not in str(op) for op in df3.simplify().operands[:2]])

    # Check result with/without fusion
    expect = pdf1.merge(pdf2, on="x", how=how)
    assert_eq(df3, expect, check_index=False)
    assert_eq(df3.optimize(), expect, check_index=False)


def test_merge_column_projection():
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20), "z": range(20)})
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"x": range(0, 20, 2), "z": range(10)})
    df2 = from_pandas(pdf2, 2)

    # Partition-wise merge with map_partitions
    df3 = df1.merge(df2, on="x")["z_x"].simplify()

    assert "y" not in df3.expr.operands[0].columns


@pytest.mark.parametrize("how", ["left", "right", "inner", "outer"])
@pytest.mark.parametrize("shuffle_backend", ["tasks", "disk"])
def test_join(how, shuffle_backend):
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)})
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"z": range(10)}, index=lib.Index(range(10), name="a"))
    df2 = from_pandas(pdf2, 2)

    # Partition-wise merge with map_partitions
    df3 = df1.join(df2, on="x", how=how, shuffle_backend=shuffle_backend)

    # Check result with/without fusion
    expect = pdf1.join(pdf2, on="x", how=how)
    assert_eq(df3, expect, check_index=False)
    assert_eq(df3.optimize(), expect, check_index=False)

    df3 = df1.join(df2.z, on="x", how=how, shuffle_backend=shuffle_backend)
    assert_eq(df3, expect, check_index=False)
    assert_eq(df3.optimize(), expect, check_index=False)


def test_join_recursive():
    pdf = lib.DataFrame({"x": [1, 2, 3], "y": 1}, index=lib.Index([1, 2, 3], name="a"))
    df = from_pandas(pdf, npartitions=2)

    pdf2 = lib.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6], "b": 1}, index=lib.Index([1, 2, 3, 4, 5, 6], name="a")
    )
    df2 = from_pandas(pdf2, npartitions=2)

    pdf3 = lib.DataFrame({"c": [1, 2, 3], "d": 1}, index=lib.Index([1, 2, 3], name="a"))
    df3 = from_pandas(pdf3, npartitions=2)

    result = df.join([df2, df3], how="outer")
    assert_eq(result, pdf.join([pdf2, pdf3], how="outer"))

    result = df.join([df2, df3], how="left")
    # The nature of our join might cast ints to floats
    assert_eq(result, pdf.join([pdf2, pdf3], how="left"), check_dtype=False)


def test_join_recursive_raises():
    pdf = lib.DataFrame({"x": [1, 2, 3], "y": 1}, index=lib.Index([1, 2, 3], name="a"))
    df = from_pandas(pdf, npartitions=2)
    with pytest.raises(ValueError, match="other must be DataFrame"):
        df.join(["dummy"])

    with pytest.raises(ValueError, match="only supports left or outer"):
        df.join([df], how="inner")
    with pytest.raises(ValueError, match="only supports left or outer"):
        df.join([df], how="right")


def test_merge_len():
    pdf = lib.DataFrame({"x": [1, 2, 3], "y": 1})
    df = from_pandas(pdf, npartitions=2)
    pdf2 = lib.DataFrame({"x": [1, 2, 3], "z": 1})
    df2 = from_pandas(pdf2, npartitions=2)

    assert_eq(len(df.merge(df2)), len(pdf.merge(pdf2)))
    query = df.merge(df2).index.optimize(fuse=False)
    expected = df[["x"]].merge(df2[["x"]]).index.optimize(fuse=False)
    assert query._name == expected._name


def test_merge_optimize_subset_strings():
    pdf = lib.DataFrame({"a": [1, 2], "aaa": 1})
    pdf2 = lib.DataFrame({"b": [1, 2], "aaa": 1})
    df = from_pandas(pdf)
    df2 = from_pandas(pdf2)

    query = df.merge(df2, on="aaa")[["aaa"]].optimize(fuse=False)
    exp = df[["aaa"]].merge(df2[["aaa"]], on="aaa").optimize(fuse=False)
    assert query._name == exp._name
    assert_eq(query, pdf.merge(pdf2, on="aaa")[["aaa"]])


@pytest.mark.parametrize("npartitions_left, npartitions_right", [(2, 3), (1, 1)])
def test_merge_combine_similar(npartitions_left, npartitions_right):
    pdf = lib.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "b": 1,
            "c": 1,
            "d": 1,
            "e": 1,
            "f": 1,
        }
    )
    pdf2 = lib.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "x": 1})

    df = from_pandas(pdf, npartitions=npartitions_left)
    df2 = from_pandas(pdf2, npartitions=npartitions_right)

    query = df.merge(df2)
    query["new"] = query.b + query.c
    query = query.groupby(["a", "e", "x"]).new.sum()
    assert (
        len(query.optimize().__dask_graph__()) <= 25
    )  # 45 is the non-combined version

    expected = pdf.merge(pdf2)
    expected["new"] = expected.b + expected.c
    expected = expected.groupby(["a", "e", "x"]).new.sum()
    assert_eq(query, expected)


def test_merge_combine_similar_intermediate_projections():
    pdf = lib.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "b": 1,
            "c": 1,
        }
    )
    pdf2 = lib.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "x": 1})
    pdf3 = lib.DataFrame({"d": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "e": 1, "y": 1})

    df = from_pandas(pdf, npartitions=2)
    df2 = from_pandas(pdf2, npartitions=3)
    df3 = from_pandas(pdf3, npartitions=3)

    q = df.merge(df2).merge(df3, left_on="b", right_on="d")[["b", "x", "y"]]
    q["new"] = q.b + q.x
    result = q.optimize(fuse=False)
    # Check that we have intermediate projections dropping unnecessary columns
    assert isinstance(result.expr.frame, Projection)
    assert isinstance(result.expr.frame.frame, Merge)
    assert isinstance(result.expr.frame.frame.left, Projection)
    assert isinstance(result.expr.frame.frame.left.frame, Shuffle)

    pd_result = pdf.merge(pdf2).merge(pdf3, left_on="b", right_on="d")[["b", "x", "y"]]
    pd_result["new"] = pd_result.b + pd_result.x

    assert sorted(result.expr.frame.frame.left.operand("columns")) == ["b", "x"]
    assert_eq(result, pd_result, check_index=False)
