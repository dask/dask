import os
import pickle

import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from dask.utils import key_split
from distributed.utils_test import gen_cluster
from pyarrow import fs

from dask_expr import from_graph, from_pandas, read_parquet
from dask_expr._expr import Filter, Lengths, Literal
from dask_expr._reductions import Len
from dask_expr.io import FusedIO, ReadParquet
from dask_expr.io.parquet import (
    _aggregate_statistics_to_file,
    _combine_stats,
    _extract_stats,
)


def _make_file(dir, df=None):
    fn = os.path.join(str(dir), "myfile.parquet")
    if df is None:
        df = pd.DataFrame({c: range(10) for c in "abcde"})
    df.to_parquet(fn)
    return fn


@pytest.fixture
def parquet_file(tmpdir):
    return _make_file(tmpdir)


@pytest.fixture(params=["arrow", "fsspec"])
def filesystem(request):
    return request.param


def test_parquet_len(tmpdir, filesystem):
    df = read_parquet(_make_file(tmpdir), filesystem=filesystem)
    pdf = df.compute()

    assert len(df[df.a > 5]) == len(pdf[pdf.a > 5])

    s = (df["b"] + 1).astype("Int32")
    assert len(s) == len(pdf)

    assert isinstance(Len(s.expr).optimize(), Literal)
    assert isinstance(Lengths(s.expr).optimize(), Literal)


def test_parquet_len_filter(tmpdir, filesystem):
    df = read_parquet(_make_file(tmpdir), filesystem=filesystem)
    expr = Len(df[df.c > 0].expr)
    result = expr.simplify()
    for rp in result.find_operations(ReadParquet):
        assert rp.operand("columns") == ["c"] or rp.operand("columns") == []


@pytest.mark.parametrize("write_metadata_file", [True, False])
def test_to_parquet(tmpdir, write_metadata_file):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
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
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = from_pandas(pdf, npartitions=2)
    with pytest.raises(NotImplementedError, match="not supported"):
        df.to_parquet(tmpdir + "engine.parquet", engine="fastparquet")


def test_pyarrow_filesystem(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)
    df = read_parquet(parquet_file)
    assert assert_eq(df, df_pa)


@pytest.mark.parametrize("dtype_backend", ["pyarrow", "numpy_nullable", None])
def test_pyarrow_filesystem_dtype_backend(parquet_file, dtype_backend):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(
        parquet_file, filesystem=filesystem, dtype_backend=dtype_backend
    )
    df = read_parquet(parquet_file, dtype_backend=dtype_backend)
    assert assert_eq(df, df_pa)


@pytest.mark.parametrize("types_mapper", [None, lambda x: None])
def test_pyarrow_filesystem_types_mapper(parquet_file, types_mapper):
    # This test isn't doing much other than ensuring the stuff is not raising
    # anywhere
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(
        parquet_file,
        filesystem=filesystem,
        arrow_to_pandas={"types_mapper": types_mapper},
    )
    df = read_parquet(parquet_file, arrow_to_pandas={"types_mapper": types_mapper})
    assert assert_eq(df, df_pa)


def test_pyarrow_filesystem_serialize(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)

    roundtripped = pickle.loads(pickle.dumps(df_pa.optimize().dask))
    roundtripped_df = from_graph(
        roundtripped,
        df_pa._meta,
        df_pa.divisions,
        df_pa.__dask_keys__(),
        key_split(df_pa._name),
    )
    assert assert_eq(df_pa, roundtripped_df)


def test_pyarrow_filesystem_filters(parquet_file):
    filesystem = fs.LocalFileSystem()

    df_pa = read_parquet(parquet_file, filesystem=filesystem)
    df_pa = df_pa[df_pa.c == 1]
    expected = read_parquet(
        parquet_file, filesystem=filesystem, filters=[[("c", "==", 1)]]
    )
    assert df_pa.optimize()._name == expected.optimize()._name
    assert len(df_pa.compute()) == 1


def test_partition_pruning(tmpdir):
    filesystem = fs.LocalFileSystem()
    df = from_pandas(
        pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5] * 10,
                "b": range(50),
            }
        ),
        npartitions=2,
    )
    df.to_parquet(tmpdir, partition_on=["a"])
    ddf = read_parquet(tmpdir, filesystem=filesystem)
    ddf_filtered = read_parquet(
        tmpdir, filters=[[("a", "==", 1)]], filesystem=filesystem
    )
    assert ddf_filtered.npartitions == ddf.npartitions // 5

    ddf_optimize = read_parquet(tmpdir, filesystem=filesystem)
    ddf_optimize = ddf_optimize[ddf_optimize.a == 1].optimize()
    assert ddf_optimize.npartitions == ddf.npartitions // 5
    assert_eq(
        ddf_filtered,
        ddf_optimize,
        # FIXME ?
        check_names=False,
    )


def test_predicate_pushdown(tmpdir):
    original = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, df=original)
    df = read_parquet(fn, filesystem="arrow")
    assert_eq(df, original)
    x = df[df.a == 5][df.c > 20]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedIO)
    assert ("a", "==", 5) in y.expr.frame.operands[0].operand("filters")[0]
    assert ("c", ">", 20) in y.expr.frame.operands[0].operand("filters")[0]
    assert list(y.columns) == ["b"]

    # Check computed result
    y_result = y.compute()
    assert y_result.name == "b"
    assert len(y_result) == 6
    assert (y_result == 4).all()

    # Don't push down if replace is in there
    x = df[df.replace(5, 50).a == 5]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr, Filter)
    assert len(y.compute()) == 0


def test_predicate_pushdown_compound(tmpdir):
    pdf = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5] * 10,
            "b": [0, 1, 2, 3, 4] * 10,
            "c": range(50),
            "d": [6, 7] * 25,
            "e": [8, 9] * 25,
        }
    )
    fn = _make_file(tmpdir, df=pdf)
    df = read_parquet(fn, filesystem="arrow")

    # Test AND
    x = df[(df.a == 5) & (df.c > 20)]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedIO)
    assert {("c", ">", 20), ("a", "==", 5)} == set(y.expr.frame.operands[0].filters[0])
    assert_eq(
        y,
        pdf[(pdf.a == 5) & (pdf.c > 20)]["b"],
        check_index=False,
    )

    # Test OR
    x = df[(df.a == 5) | (df.c > 20)]
    x = x[x.b != 0]["b"]
    y = x.optimize(fuse=False)
    assert isinstance(y.expr.frame, FusedIO)
    filters = [
        set(y.expr.frame.operands[0].filters[0]),
        set(y.expr.frame.operands[0].filters[1]),
    ]
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
    z = x.optimize(fuse=False)
    assert isinstance(z.expr.frame, FusedIO)
    filters = [
        set(z.expr.frame.operands[0].filters[0]),
        set(z.expr.frame.operands[0].filters[1]),
    ]
    assert {("c", ">", 20), ("b", "!=", 0)} in filters
    assert {("a", "==", 5), ("b", "!=", 0)} in filters
    assert_eq(y, z)


def test_aggregate_rg_stats_to_file(tmpdir):
    filesystem = fs.LocalFileSystem()
    fn = str(tmpdir)
    ddf = from_pandas(pd.DataFrame({"a": range(10)}), npartitions=1)
    ddf.to_parquet(fn)
    ddf = read_parquet(fn, filesystem=filesystem)
    frag = ddf._expr.fragments[0]
    # Make sure this doesn't raise. We'll test the actual aggregation below
    _aggregate_statistics_to_file([frag.metadata.to_dict()])
    # In reality, we'll strip the metadata down
    assert (
        len(_aggregate_statistics_to_file([_extract_stats(frag.metadata.to_dict())]))
        > 0
    )


def test_aggregate_statistics_to_file():
    file_in = {
        "top-level-file-stat": "not-interested",
        "row_groups": [
            # RG 1
            {
                "num_rows": 100,
                "total_byte_size": 1000,
                "columns": [
                    {
                        "total_compressed_size": 10,
                        "total_uncompressed_size": 15,
                        "statistics": {
                            "min": 0,
                            "max": 10,
                            "null_count": 5,
                            "distinct_count": None,
                            "new_powerful_yet_uknown_stat": 42,
                        },
                        "path_in_schema": "a",
                    },
                    {
                        "total_compressed_size": 7,
                        "total_uncompressed_size": 23,
                        "statistics": {
                            "min": 11,
                            "max": 20,
                            "null_count": 1,
                            "distinct_count": 12,
                            "new_powerful_yet_uknown_stat": 42,
                        },
                        "path_in_schema": "b",
                    },
                ],
            },
            # RG 2
            {
                "num_rows": 50,
                "total_byte_size": 500,
                "columns": [
                    {
                        "total_compressed_size": 5,
                        "total_uncompressed_size": 7,
                        "statistics": {
                            "min": 40,
                            "max": 50,
                            "null_count": 0,
                            "distinct_count": None,
                            "new_powerful_yet_uknown_stat": 42,
                        },
                        "path_in_schema": "a",
                    },
                    {
                        "total_compressed_size": 7,
                        "total_uncompressed_size": 23,
                        "statistics": {
                            "min": 0,
                            "max": 20,
                            "null_count": 0,
                            "distinct_count": None,
                            "new_powerful_yet_uknown_stat": 42,
                        },
                        "path_in_schema": "b",
                    },
                ],
            },
        ],
    }

    expected = {
        "top-level-file-stat": "not-interested",
        "num_rows": 150,
        "total_byte_size": 1500,
        "columns": [
            {
                "path_in_schema": "a",
                "total_compressed_size": 15,
                "total_uncompressed_size": 22,
                "statistics": {
                    "min": 0,
                    "max": 50,
                },
            },
            {
                "path_in_schema": "b",
                "total_compressed_size": 14,
                "total_uncompressed_size": 46,
                "statistics": {
                    "min": 0,
                    "max": 20,
                },
            },
        ],
    }
    actual = _aggregate_statistics_to_file([file_in])
    assert len(actual) == 1
    assert actual[0] == expected


def test_combine_statistics():
    file_in = [
        # File 1
        {
            "top-level-file-stat": "not-interested",
            "num_row_groups": 2,
            "row_groups": [
                # RG 1
                {
                    "num_rows": 200,
                    "total_byte_size": 1000,
                    "columns": [
                        {
                            "total_compressed_size": 10,
                            "total_uncompressed_size": 15,
                            "statistics": {
                                "min": 0,
                                "max": 10,
                                "null_count": 5,
                                "distinct_count": None,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 11,
                                "max": 20,
                                "null_count": 1,
                                "distinct_count": 12,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
                # RG 2
                {
                    "num_rows": 50,
                    "total_byte_size": 500,
                    "columns": [
                        {
                            "total_compressed_size": 5,
                            "total_uncompressed_size": 7,
                            "statistics": {
                                "min": 40,
                                "max": 50,
                                "null_count": 0,
                                "distinct_count": None,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 0,
                                "max": 20,
                                "null_count": 0,
                                "distinct_count": None,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
            ],
        },
        # File 2
        {
            "top-level-file-stat": "not-interested",
            "num_row_groups": 1,
            "row_groups": [
                # RG 1
                {
                    "num_rows": 100,
                    "total_byte_size": 2000,
                    "columns": [
                        {
                            "total_compressed_size": 10,
                            "total_uncompressed_size": 15,
                            "statistics": {
                                "min": 0,
                                "max": 10,
                                "null_count": 5,
                                "distinct_count": None,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "a",
                        },
                        {
                            "total_compressed_size": 7,
                            "total_uncompressed_size": 23,
                            "statistics": {
                                "min": 11,
                                "max": 20,
                                "null_count": 1,
                                "distinct_count": 12,
                                "new_powerful_yet_uknown_stat": 42,
                            },
                            "path_in_schema": "b",
                        },
                    ],
                },
            ],
        },
    ]
    actual = _combine_stats(file_in)
    expected = {
        "num_rows": ((200 + 50) + 100) // 2,
        "total_byte_size": ((1000 + 500) + 2000) // 2,
        "num_row_groups": 1.5,
        "columns": [
            {
                "total_compressed_size": ((10 + 5) + 10) / 2,
                "total_uncompressed_size": ((15 + 7) + 15) / 2,
                "path_in_schema": "a",
            },
            {
                "total_compressed_size": ((7 + 7) + 7) / 2,
                "total_uncompressed_size": ((23 + 23) + 23) / 2,
                "path_in_schema": "b",
            },
        ],
    }
    assert actual == expected


@pytest.mark.filterwarnings("error")
@gen_cluster(client=True)
async def test_parquet_distriuted(c, s, a, b, tmpdir, filesystem):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = read_parquet(_make_file(tmpdir, df=pdf), filesystem=filesystem)
    assert_eq(await c.gather(c.compute(df.optimize())), pdf)
