import glob
import os

import dask.dataframe as dd
import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import (
    from_dask_dataframe,
    from_map,
    from_pandas,
    optimize,
    read_csv,
    read_parquet,
    repartition,
)
from dask_expr._expr import Expr, Lengths, Literal, Replace
from dask_expr._reductions import Len
from dask_expr.io import FromMap, ReadCSV, ReadParquet
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
lib = _backend_library()


def _make_file(dir, format="parquet", df=None):
    fn = os.path.join(str(dir), f"myfile.{format}")
    if df is None:
        df = lib.DataFrame({c: range(10) for c in "abcde"})
    if format == "csv":
        df.to_csv(fn)
    elif format == "parquet":
        df.to_parquet(fn)
    else:
        ValueError(f"{format} not a supported format")
    return fn


def df(fn):
    return read_parquet(fn, columns=["a", "b", "c"])


def df_bc(fn):
    return read_parquet(fn, columns=["b", "c"])


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            # Add -> Mul
            lambda fn: df(fn) + df(fn),
            lambda fn: 2 * df(fn),
        ),
        (
            # Column projection
            lambda fn: df(fn)[["b", "c"]],
            lambda fn: read_parquet(fn, columns=["b", "c"]),
        ),
        (
            # Compound
            lambda fn: 3 * (df(fn) + df(fn))[["b", "c"]],
            lambda fn: 6 * df_bc(fn),
        ),
        (
            # Traverse Sum
            lambda fn: df(fn).sum()[["b", "c"]],
            lambda fn: df_bc(fn).sum(),
        ),
        (
            # Respect Sum keywords
            lambda fn: df(fn).sum(numeric_only=True)[["b", "c"]],
            lambda fn: df_bc(fn).sum(numeric_only=True),
        ),
    ],
)
def test_simplify(tmpdir, input, expected):
    fn = _make_file(tmpdir, format="parquet")
    result = input(fn).simplify()
    assert str(result.expr) == str(expected(fn).expr)


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_io_fusion(tmpdir, fmt):
    fn = _make_file(tmpdir, format=fmt)
    if fmt == "parquet":
        df = read_parquet(fn)
    else:
        df = read_csv(fn)
    df2 = optimize(df[["a", "b"]] + 1, fuse=True)

    # All tasks should be fused for each partition
    assert len(df2.dask) == df2.npartitions
    assert_eq(df2, df[["a", "b"]] + 1)


@pytest.mark.skip()
def test_predicate_pushdown(tmpdir):
    original = lib.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, format="parquet", df=original)
    df = read_parquet(fn)
    assert_eq(df, original)
    x = df[df.a == 5][df.c > 20]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    assert ("a", "==", 5) in y.expr.operand("filters")[0]
    assert ("c", ">", 20) in y.expr.operand("filters")[0]
    assert list(y.columns) == ["b"]

    # Check computed result
    y_result = y.compute()
    assert y_result.name == "b"
    assert len(y_result) == 6
    assert (y_result == 4).all()


@pytest.mark.skip()
def test_predicate_pushdown_compound(tmpdir):
    pdf = lib.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, format="parquet", df=pdf)
    df = read_parquet(fn)

    # Test AND
    x = df[(df.a == 5) & (df.c > 20)]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    assert {("c", ">", 20), ("a", "==", 5)} == set(y.filters[0])
    assert_eq(
        y,
        pdf[(pdf.a == 5) & (pdf.c > 20)]["b"],
        check_index=False,
    )

    # Test OR
    x = df[(df.a == 5) | (df.c > 20)]
    x = x[x.b != 0]["b"]
    y = optimize(x, fuse=False)
    assert isinstance(y.expr, ReadParquet)
    filters = [set(y.filters[0]), set(y.filters[1])]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    expect = pdf[(pdf.a == 5) | (pdf.c > 20)]
    expect = expect[expect.b != 0]["b"]
    assert_eq(
        y,
        expect,
        check_index=False,
    )

    # Test OR and AND
    x = df[((df.a == 5) | (df.c > 20)) & (df.b != 0)]["b"]
    z = optimize(x, fuse=False)
    assert isinstance(z.expr, ReadParquet)
    filters = [set(z.filters[0]), set(z.filters[1])]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    assert_eq(y, z)


def test_io_fusion_blockwise(tmpdir):
    pdf = lib.DataFrame({c: range(10) for c in "abcdefghijklmn"})
    dd.from_pandas(pdf, 3).to_parquet(tmpdir)
    df = read_parquet(tmpdir)["a"].fillna(10).optimize()
    assert df.npartitions == 2
    assert len(df.__dask_graph__()) == 2
    graph = (
        read_parquet(tmpdir)["a"].repartition(npartitions=4).optimize().__dask_graph__()
    )
    assert any("readparquet-fused" in key[0] for key in graph.keys())


def test_repartition_io_fusion_blockwise(tmpdir):
    pdf = lib.DataFrame({c: range(10) for c in "ab"})
    dd.from_pandas(pdf, 10).to_parquet(tmpdir)
    df = read_parquet(tmpdir)["a"]
    df = df.repartition(npartitions=lambda x: max(x // 2, 1)).optimize()
    assert df.npartitions == 2


def test_io_fusion_merge(tmpdir):
    pdf = lib.DataFrame({c: range(10) for c in "ab"})
    pdf2 = lib.DataFrame({c: range(10) for c in "uvwxyz"})
    dd.from_pandas(pdf, 10).to_parquet(tmpdir)
    dd.from_pandas(pdf2, 10).to_parquet(tmpdir + "x")
    df = read_parquet(tmpdir)
    df2 = read_parquet(tmpdir + "x")
    result = df.merge(df2, left_on="a", right_on="w")[["a", "b", "u"]]
    assert_eq(
        result,
        pdf.merge(pdf2, left_on="a", right_on="w")[["a", "b", "u"]],
        check_index=False,
    )


@pytest.mark.parametrize("fmt", ["parquet", "csv", "pandas"])
def test_io_culling(tmpdir, fmt):
    pdf = lib.DataFrame({c: range(10) for c in "abcde"})
    if fmt == "parquet":
        dd.from_pandas(pdf, 2).to_parquet(tmpdir)
        df = read_parquet(tmpdir)
    elif fmt == "csv":
        dd.from_pandas(pdf, 2).to_csv(tmpdir)
        df = read_csv(tmpdir + "/*")
    else:
        df = from_pandas(pdf, 2)
    df = (df[["a", "b"]] + 1).partitions[1]
    df2 = optimize(df)

    # All tasks should be fused for the single output partition
    assert df2.npartitions == 1
    assert len(df2.dask) == df2.npartitions
    expected = pdf.iloc[5:][["a", "b"]] + 1
    assert_eq(df2, expected, check_index=False)

    def _check_culling(expr, partitions):
        """CHeck that _partitions is set to the expected value"""
        for dep in expr.dependencies():
            _check_culling(dep, partitions)
        if "_partitions" in expr._parameters:
            assert expr._partitions == partitions

    # Check that we still get culling without fusion
    df3 = optimize(df, fuse=False)
    _check_culling(df3.expr, [1])
    assert_eq(df3, expected, check_index=False)


@pytest.mark.parametrize("sort", [True, False])
def test_from_pandas(sort):
    pdf = lib.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2, sort=sort)

    assert df.divisions == (0, 3, 5) if sort else (None,) * 3
    assert_eq(df, pdf)


def test_from_pandas_empty():
    pdf = lib.DataFrame(columns=["a", "b"])
    df = from_pandas(pdf, npartitions=2)
    assert_eq(pdf, df)


def test_from_pandas_immutable():
    pdf = lib.DataFrame({"x": [1, 2, 3, 4]})
    expected = pdf.copy()
    df = from_pandas(pdf)
    pdf["z"] = 100
    assert_eq(df, expected)


def test_parquet_complex_filters(tmpdir):
    df = read_parquet(_make_file(tmpdir))
    pdf = df.compute()
    got = df["a"][df["b"] > df["b"].mean()]
    expect = pdf["a"][pdf["b"] > pdf["b"].mean()]

    assert_eq(got, expect)
    assert_eq(got.optimize(), expect)


def test_parquet_len(tmpdir):
    df = read_parquet(_make_file(tmpdir))
    pdf = df.compute()

    assert len(df[df.a > 5]) == len(pdf[pdf.a > 5])

    s = (df["b"] + 1).astype("Int32")
    assert len(s) == len(pdf)

    assert isinstance(Len(s.expr).optimize(), Literal)
    assert isinstance(Lengths(s.expr).optimize(), Literal)


def test_parquet_len_filter(tmpdir):
    df = read_parquet(_make_file(tmpdir))
    expr = Len(df[df.c > 0].expr)
    result = expr.simplify()
    for rp in result.find_operations(ReadParquet):
        assert rp.operand("columns") == ["c"] or rp.operand("columns") == []


@pytest.mark.parametrize("optimize", [True, False])
def test_from_dask_dataframe(optimize):
    ddf = dd.from_dict({"a": range(100)}, npartitions=10)
    df = from_dask_dataframe(ddf, optimize=optimize)
    assert isinstance(df.expr, Expr)
    assert_eq(df, ddf)


@pytest.mark.parametrize("optimize", [True, False])
def test_to_dask_dataframe(optimize):
    pdf = lib.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)
    ddf = df.to_dask_dataframe(optimize=optimize)
    assert isinstance(ddf, dd.DataFrame)
    assert_eq(df, ddf)


@pytest.mark.parametrize("write_metadata_file", [True, False])
def test_to_parquet(tmpdir, write_metadata_file):
    pdf = lib.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)

    # Check basic parquet round trip
    df.to_parquet(tmpdir, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check overwrite behavior
    df["new"] = df["x"] + 1
    df.to_parquet(tmpdir, overwrite=True, write_metadata_file=write_metadata_file)
    df2 = read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df2)

    # Check that we cannot overwrite a path we are
    # reading from in the same graph
    with pytest.raises(ValueError, match="Cannot overwrite"):
        df2.to_parquet(tmpdir, overwrite=True)


def test_to_parquet_engine(tmpdir):
    pdf = lib.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)
    with pytest.raises(NotImplementedError, match="not supported"):
        df.to_parquet(tmpdir + "engine.parquet", engine="fastparquet")


@pytest.mark.parametrize(
    "fmt,read_func,read_cls",
    [("parquet", read_parquet, ReadParquet), ("csv", read_csv, ReadCSV)],
)
def test_combine_similar(tmpdir, fmt, read_func, read_cls):
    pdf = lib.DataFrame(
        {"x": [0, 1, 2, 3] * 4, "y": range(16), "z": [None, 1, 2, 3] * 4}
    )
    fn = _make_file(tmpdir, format=fmt, df=pdf)
    df = read_func(fn)
    df = df.replace(1, 100)
    df["xx"] = df.x != 0
    df["yy"] = df.y != 0
    got = df[["xx", "yy", "x"]].sum()

    pdf = pdf.replace(1, 100)
    pdf["xx"] = pdf.x != 0
    pdf["yy"] = pdf.y != 0
    expect = pdf[["xx", "yy", "x"]].sum()

    # Check correctness
    assert_eq(got, expect)
    assert_eq(got.optimize(fuse=False), expect)
    assert_eq(got.optimize(fuse=True), expect)

    # We should only have one ReadParquet/ReadCSV node,
    # and it should not include "z" in the column projection
    read_nodes = list(got.optimize(fuse=False).find_operations(read_cls))
    assert len(read_nodes) == 1
    assert set(read_nodes[0].columns) == {"x", "y"}

    # All Replace operations should also be the same
    replace_nodes = list(got.optimize(fuse=False).find_operations(Replace))
    assert len(replace_nodes) == 1


def test_combine_similar_no_projection_on_one_branch(tmpdir):
    pdf = lib.DataFrame(
        {"x": [0, 1, 2, 3] * 4, "y": range(16), "z": [None, 1, 2, 3] * 4}
    )
    fn = _make_file(tmpdir, format="parquet", df=pdf)
    df = read_parquet(fn)
    df["xx"] = df.x != 0

    pdf["xx"] = pdf.x != 0
    assert_eq(df, pdf)


@pytest.mark.parametrize("meta", [True, False])
@pytest.mark.parametrize("label", [None, "foo"])
@pytest.mark.parametrize("allow_projection", [True, False])
@pytest.mark.parametrize("enforce_metadata", [True, False])
def test_from_map(tmpdir, meta, label, allow_projection, enforce_metadata):
    pdf = lib.DataFrame({c: range(10) for c in "abcdefghijklmn"})
    dd.from_pandas(pdf, 3).to_parquet(tmpdir, write_index=False)
    files = sorted(glob.glob(str(tmpdir) + "/*.parquet"))
    if allow_projection:
        func = lib.read_parquet
    else:
        func = lambda *args, **kwargs: lib.read_parquet(*args, **kwargs)
    options = {
        "enforce_metadata": enforce_metadata,
        "label": label,
    }
    if meta:
        options["meta"] = pdf.iloc[:0]

    df = from_map(func, files, **options)
    assert_eq(df, pdf, check_index=False)
    assert_eq(df["a"], pdf["a"], check_index=False)
    assert_eq(df[["a"]], pdf[["a"]], check_index=False)
    assert_eq(df[["a", "b"]], pdf[["a", "b"]], check_index=False)

    if label:
        assert df.expr._name.startswith(label)

    if allow_projection:
        got = df[["a", "b"]].optimize(fuse=False)
        assert isinstance(got.expr, FromMap)
        assert got.expr.operand("columns") == ["a", "b"]

    # Check that we can always pass columns up front
    if meta:
        options["meta"] = options["meta"][["a", "b"]]
    result = from_map(func, files, columns=["a", "b"], **options)
    assert_eq(result, pdf[["a", "b"]], check_index=False)
    if meta:
        options["meta"] = options["meta"][["a"]]
    result = from_map(func, files, columns="a", **options)
    assert_eq(result, pdf[["a"]], check_index=False)

    # Check the case that func returns a Series
    if meta:
        options["meta"] = options["meta"]["a"]
    result = from_map(lambda x: lib.read_parquet(x)["a"], files, **options)
    assert_eq(result, pdf["a"], check_index=False)


def test_from_pandas_sort():
    pdf = lib.DataFrame({"a": [1, 2, 3, 1, 2, 2]}, index=[6, 5, 4, 3, 2, 1])
    df = from_pandas(pdf, npartitions=2)
    assert_eq(df, pdf.sort_index(), sort_results=False)


def test_from_pandas_divisions():
    pdf = lib.DataFrame({"a": [1, 2, 3, 1, 2, 2]}, index=[7, 6, 4, 3, 2, 1])
    df = repartition(pdf, (1, 5, 8))
    assert_eq(df, pdf.sort_index())

    pdf = lib.DataFrame({"a": [1, 2, 3, 1, 2, 2]}, index=[7, 6, 4, 3, 2, 1])
    df = repartition(pdf, (1, 4, 8))
    assert_eq(df.partitions[1], lib.DataFrame({"a": [3, 2, 1]}, index=[4, 6, 7]))

    df = repartition(df, divisions=(1, 3, 8), force=True)
    assert_eq(df, pdf.sort_index())


def test_from_pandas_empty_projection():
    pdf = lib.DataFrame({"a": [1, 2, 3], "b": 1})
    df = from_pandas(pdf)
    assert_eq(df[[]], pdf[[]])


def test_from_pandas_divisions_duplicated():
    pdf = lib.DataFrame({"a": 1}, index=[1, 2, 3, 4, 5, 5, 5, 6, 8])
    df = repartition(pdf, (1, 5, 7, 10))
    assert_eq(df, pdf)
    assert_eq(df.partitions[0], pdf.loc[1:4])
    assert_eq(df.partitions[1], pdf.loc[5:6])
    assert_eq(df.partitions[2], pdf.loc[8:])
