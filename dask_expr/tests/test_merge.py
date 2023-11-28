import pytest

from dask_expr import Merge, from_pandas, merge
from dask_expr._expr import Projection
from dask_expr._shuffle import Shuffle
from dask_expr.tests._util import _backend_library, assert_eq

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

    df3 = merge(df1, df2, on="x", how=how, shuffle_backend=shuffle_backend)
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


def test_merge_combine_similar_hangs():
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"
    region_ds = from_pandas(
        lib.DataFrame.from_dict(
            {
                "r_regionkey": {0: 0, 1: 1},
                "r_name": {0: "AFRICA", 1: "AMERICA"},
                "r_comment": {0: "a", 1: "s "},
            }
        )
    )
    nation_filtered = from_pandas(
        lib.DataFrame.from_dict(
            {
                "n_nationkey": {0: 0, 1: 1},
                "n_name": {0: "ALGERIA", 1: "ARGENTINA"},
                "n_regionkey": {0: 0, 1: 1},
                "n_comment": {0: "fu", 1: "i"},
            }
        )
    )

    supplier_filtered = from_pandas(
        lib.DataFrame.from_dict(
            {
                "s_suppkey": {0: 1, 1: 2},
                "s_name": {0: "a#1", 1: "a#2"},
                "s_address": {0: "sdrGnX", 1: "T"},
                "s_nationkey": {0: 17, 1: 5},
                "s_phone": {0: "27-918-335-1736", 1: "15-679-861-2259"},
                "s_acctbal": {0: 5755, 1: 4032},
                "s_comment": {0: " inst", 1: " th"},
            }
        )
    )
    part_filtered = from_pandas(
        lib.DataFrame.from_dict(
            {
                "p_partkey": {0: 1, 1: 2},
                "p_name": {0: "gol", 1: "bl"},
                "p_mfgr": {0: "Manufacturer#1", 1: "Manufacturer#1"},
                "p_brand": {0: "Brand#13", 1: "Brand#13"},
                "p_type": {0: "PROM", 1: "LARG"},
                "p_size": {0: 7, 1: 1},
                "p_container": {0: "J", 1: "LG"},
                "p_retailprice": {0: 901, 1: 902},
                "p_comment": {0: "ir", 1: "ack"},
            }
        )
    )
    #
    partsupp_filtered = from_pandas(
        lib.DataFrame.from_dict(
            {
                "ps_partkey": {0: 1, 1: 1},
                "ps_suppkey": {0: 2, 1: 2502},
                "ps_availqty": {0: 3325, 1: 8076},
                "ps_supplycost": {0: 771, 1: 993},
                "ps_comment": {0: "bli", 1: "ts boo"},
            }
        )
    )

    region_filtered = region_ds[(region_ds["r_name"] == var3)]
    r_n_merged = nation_filtered.merge(
        region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
    )
    s_r_n_merged = r_n_merged.merge(
        supplier_filtered,
        left_on="n_nationkey",
        right_on="s_nationkey",
        how="inner",
    )
    ps_s_r_n_merged = s_r_n_merged.merge(
        partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
    )
    part_filtered = part_filtered[
        (part_filtered["p_size"] == var1)
        & (part_filtered["p_type"].astype(str).str.endswith(var2))
    ]
    merged_df = part_filtered.merge(
        ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
    )
    min_values = merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
    min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
    merged_df = merged_df.merge(
        min_values,
        left_on=["p_partkey", "ps_supplycost"],
        right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
        how="inner",
    )
    out = merged_df[
        [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]
    ]
    expected = lib.DataFrame(
        columns=[
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]
    )
    assert_eq(out, expected, check_dtype=False)

    # Double check that these don't hang
    out.optimize(fuse=False)
    out.optimize()


def test_recursive_join():
    dfs_to_merge = []
    for i in range(10):
        df = lib.DataFrame(
            {
                f"{i}A": [5, 6, 7, 8],
                f"{i}B": [4, 3, 2, 1],
            },
            index=lib.Index([0, 1, 2, 3], name="a"),
        )
        ddf = from_pandas(df, 2)
        dfs_to_merge.append(ddf)

    ddf_loop = from_pandas(lib.DataFrame(index=lib.Index([0, 1, 3], name="a")), 3)
    for ddf in dfs_to_merge:
        ddf_loop = ddf_loop.join(ddf, how="left")

    ddf_pairwise = from_pandas(lib.DataFrame(index=lib.Index([0, 1, 3], name="a")), 3)

    ddf_pairwise = ddf_pairwise.join(dfs_to_merge, how="left")

    # TODO: divisions is None for recursive join for now
    assert_eq(ddf_pairwise, ddf_loop, check_divisions=False)


def test_merge_repartition():
    pdf = lib.DataFrame({"a": [1, 2, 3]})
    pdf2 = lib.DataFrame({"b": [1, 2, 3]}, index=[1, 2, 3])

    df = from_pandas(pdf, npartitions=2)
    df2 = from_pandas(pdf2, npartitions=3)
    assert_eq(df.join(df2), pdf.join(pdf2))


def test_merge_reparititon_divisions():
    pdf = lib.DataFrame({"a": [1, 2, 3, 4, 5, 6]})
    pdf2 = lib.DataFrame({"b": [1, 2, 3, 4, 5, 6]}, index=[1, 2, 3, 4, 5, 6])
    pdf3 = lib.DataFrame({"c": [1, 2, 3, 4, 5, 6]}, index=[1, 2, 3, 4, 5, 6])

    df = from_pandas(pdf, npartitions=2)
    df2 = from_pandas(pdf2, npartitions=3)
    df3 = from_pandas(pdf3, npartitions=3)

    assert_eq(df.join(df2).join(df3), pdf.join(pdf2).join(pdf3))


def test_merge_npartitions():
    pdf = lib.DataFrame({"a": [1, 2, 3, 4, 5, 6]})
    pdf2 = lib.DataFrame({"b": [1, 2, 3, 4, 5, 6]}, index=[1, 2, 3, 4, 5, 6])
    df = from_pandas(pdf, npartitions=1)
    df2 = from_pandas(pdf2, npartitions=3)

    result = df.join(df2, npartitions=6)
    # Ignore npartitions when broadcasting
    assert result.npartitions == 4
    assert_eq(result, pdf.join(pdf2))

    df = from_pandas(pdf, npartitions=2)
    result = df.join(df2, npartitions=6)
    # Ignore npartitions for repartition-join
    assert result.npartitions == 4
    assert_eq(result, pdf.join(pdf2))

    pdf = lib.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6]}, index=lib.Index([6, 5, 4, 3, 2, 1], name="a")
    )
    pdf2 = lib.DataFrame(
        {"b": [1, 2, 3, 4, 5, 6]}, index=lib.Index([1, 2, 7, 4, 5, 6], name="a")
    )
    df = from_pandas(pdf, npartitions=2, sort=False)
    df2 = from_pandas(pdf2, npartitions=3, sort=False)

    result = df.join(df2, npartitions=6)
    assert result.npartitions == 6
    assert_eq(result, pdf.join(pdf2))


def test_merge_pandas_object():
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)})
    df1 = from_pandas(pdf1, 4)
    pdf2 = lib.DataFrame({"x": range(20), "z": range(20)})

    assert_eq(merge(df1, pdf2, on="x"), pdf1.merge(pdf2, on="x"), check_index=False)
    assert_eq(merge(pdf2, df1, on="x"), pdf2.merge(pdf1, on="x"), check_index=False)

    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)}).set_index("x")
    df1 = from_pandas(pdf1, 4)
    assert_eq(
        merge(df1, pdf2, left_index=True, right_on="x"),
        pdf1.merge(pdf2, left_index=True, right_on="x"),
        check_index=False,
    )
    assert_eq(
        merge(pdf2, df1, left_on="x", right_index=True),
        pdf2.merge(pdf1, left_on="x", right_index=True),
        check_index=False,
    )
