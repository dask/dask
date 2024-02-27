import os

import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from pyarrow import fs

from dask_expr import from_pandas, read_parquet
from dask_expr._expr import Lengths, Literal
from dask_expr._reductions import Len
from dask_expr.io import ReadParquet


def _make_file(dir, df=None):
    fn = os.path.join(str(dir), "myfile.parquet")
    if df is None:
        df = pd.DataFrame({c: range(10) for c in "abcde"})
    df.to_parquet(fn)
    return fn


@pytest.fixture
def parquet_file(tmpdir):
    return _make_file(tmpdir)


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
