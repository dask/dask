from __future__ import annotations

import contextlib
import glob
import math
import os
import warnings
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from packaging.version import parse as parse_version

import dask
import dask.dataframe as dd
import dask.multiprocessing
from dask.array.numpy_compat import _numpy_124
from dask.blockwise import Blockwise, optimize_blockwise
from dask.dataframe._compat import PANDAS_GE_150, PANDAS_GE_200, PANDAS_GE_202
from dask.dataframe.io.parquet.core import get_engine
from dask.dataframe.io.parquet.utils import _parse_pandas_metadata
from dask.dataframe.optimize import optimize_dataframe_getitem
from dask.dataframe.utils import assert_eq, pyarrow_strings_enabled
from dask.layers import DataFrameIOLayer
from dask.utils import natural_sort_key
from dask.utils_test import hlg_layer

try:
    import fastparquet
except ImportError:
    fastparquet = False
    fastparquet_version = parse_version("0")
else:
    fastparquet_version = parse_version(fastparquet.__version__)


try:
    import pyarrow as pa

    pyarrow_version = parse_version(pa.__version__)
except ImportError:
    pa = False
    pyarrow_version = parse_version("0")

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = False


SKIP_FASTPARQUET = not fastparquet
FASTPARQUET_MARK = pytest.mark.skipif(
    SKIP_FASTPARQUET, reason="fastparquet not found or pyarrow strings are enabled"
)

SKIP_PYARROW = not pq
SKIP_PYARROW_REASON = "pyarrow not found"
PYARROW_MARK = pytest.mark.skipif(SKIP_PYARROW, reason=SKIP_PYARROW_REASON)

nrows = 40
npartitions = 15
df = pd.DataFrame(
    {
        "x": [i * 7 % 5 for i in range(nrows)],  # Not sorted
        "y": [i * 2.5 for i in range(nrows)],  # Sorted
    },
    index=pd.Index([10 * i for i in range(nrows)], name="myindex"),
)

ddf = dd.from_pandas(df, npartitions=npartitions)

_engine_fixture = pytest.fixture(
    params=[
        pytest.param(
            {"engine": "fastparquet"},
            id="fastparquet",
            marks=[
                FASTPARQUET_MARK,
                pytest.mark.filterwarnings("ignore::FutureWarning"),
            ],
        ),
        pytest.param({}, id="pyarrow", marks=[PYARROW_MARK]),
    ]
)


@_engine_fixture
def engine(request):
    return request.param


@_engine_fixture
def write_engine(request):
    return request.param


@_engine_fixture
def read_engine(request):
    return request.param


@PYARROW_MARK
def test_get_engine_pyarrow():
    from dask.dataframe.io.parquet.arrow import ArrowDatasetEngine

    assert get_engine(None) is ArrowDatasetEngine
    for engine in ("auto", "pyarrow", "pyarrow-dataset", "arrow"):
        with pytest.warns(FutureWarning, match="The `engine=` parameter is deprecated"):
            assert get_engine(engine) is ArrowDatasetEngine


@FASTPARQUET_MARK
def test_get_engine_fastparquet():
    from dask.dataframe.io.parquet.fastparquet import FastParquetEngine

    with pytest.warns(FutureWarning) as warns:
        assert get_engine("fastparquet") is FastParquetEngine

    assert len(warns) == 2
    assert "The `engine=` parameter is deprecated" in str(warns[0])
    assert "The fastparquet engine is deprecated" in str(warns[1])


@pytest.mark.skipif(
    pa or not fastparquet, reason="Must have fastparquet but not pyarrow installed"
)
def test_get_engine_fastparquet_only():
    from dask.dataframe.io.parquet.fastparquet import FastParquetEngine

    with pytest.warns(
        FutureWarning, match="Could not find pyarrow; falling back to fastparquet"
    ):
        assert get_engine(None) is FastParquetEngine


@pytest.mark.skipif(pa or fastparquet, reason="fastparquet or pyarrow are installed")
def test_get_engine_no_engine():
    with pytest.raises(RuntimeError, match="`pyarrow` not installed"):
        get_engine(None)

    with (
        pytest.warns(FutureWarning, match="The `engine=` parameter is deprecated"),
        pytest.raises(RuntimeError, match="`pyarrow` not installed"),
    ):
        get_engine("auto")


def test_get_engine_invalid():
    with (
        pytest.warns(FutureWarning, match="The `engine=` parameter is deprecated"),
        pytest.raises(ValueError),
    ):
        get_engine(123)


@pytest.mark.parametrize("has_metadata", [False, True])
def test_local(tmpdir, write_engine, read_engine, has_metadata):
    tmp = str(tmpdir)
    data = pd.DataFrame(
        {
            "i32": np.arange(1000, dtype=np.int32),
            "i64": np.arange(1000, dtype=np.int64),
            "f": np.arange(1000, dtype=np.float64),
            "bhello": np.random.choice(["hello", "yo", "people"], size=1000).astype(
                "O"
            ),
        }
    )
    df = dd.from_pandas(data, chunksize=500)

    kwargs = {"write_metadata_file": True} if has_metadata else {}
    df.to_parquet(tmp, write_index=False, **write_engine, **kwargs)

    files = os.listdir(tmp)
    assert ("_common_metadata" in files) == has_metadata
    assert ("_metadata" in files) == has_metadata
    assert "part.0.parquet" in files

    df2 = dd.read_parquet(tmp, index=False, **read_engine)

    assert len(df2.divisions) > 1

    out = df2.compute(scheduler="sync").reset_index()

    for column in df.columns:
        assert (data[column] == out[column]).all()


@pytest.mark.parametrize("index", [False, True])
def test_empty(tmpdir, write_engine, read_engine, index):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})[:0]
    if index:
        df = df.set_index("a", drop=True)
    ddf = dd.from_pandas(df, npartitions=2)

    ddf.to_parquet(fn, write_index=index, write_metadata_file=True, **write_engine)
    read_df = dd.read_parquet(fn, **read_engine)
    assert_eq(ddf, read_df)


def test_simple(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})
    df = df.set_index("a", drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, **write_engine)
    read_df = dd.read_parquet(fn, index=["a"], calculate_divisions=True, **read_engine)
    assert_eq(ddf, read_df)


def test_delayed_no_metadata(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})
    df = df.set_index("a", drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(
        fn, compute=False, write_metadata_file=False, **write_engine
    ).compute()
    files = os.listdir(fn)
    assert "_metadata" not in files
    # Fastparquet doesn't currently handle a directory without "_metadata"
    read_df = dd.read_parquet(
        os.path.join(fn, "*.parquet"),
        index=["a"],
        calculate_divisions=True,
        **read_engine,
    )
    assert_eq(ddf, read_df)


def test_read_glob(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, **write_engine)
    if os.path.exists(os.path.join(tmp_path, "_metadata")):
        os.unlink(os.path.join(tmp_path, "_metadata"))
    files = os.listdir(tmp_path)
    assert "_metadata" not in files

    ddf2 = dd.read_parquet(
        os.path.join(tmp_path, "*.parquet"),
        index="myindex",  # Must specify index without _metadata
        calculate_divisions=True,
        **read_engine,
    )
    assert_eq(ddf, ddf2)


def test_calculate_divisions_false(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, write_index=False, **write_engine)

    ddf2 = dd.read_parquet(
        tmp_path,
        index=False,
        calculate_divisions=False,
        **read_engine,
    )
    assert_eq(ddf, ddf2, check_index=False, check_divisions=False)


def test_read_list(tmpdir, write_engine, read_engine):
    if write_engine == read_engine == {"engine": "fastparquet"} and os.name == "nt":
        # fastparquet or dask is not normalizing filepaths correctly on
        # windows.
        pytest.skip("filepath bug.")

    tmpdir = str(tmpdir)
    ddf.to_parquet(tmpdir, **write_engine)
    files = sorted(
        (
            os.path.join(tmpdir, f)
            for f in os.listdir(tmpdir)
            if not f.endswith("_metadata")
        ),
        key=natural_sort_key,
    )

    ddf2 = dd.read_parquet(
        files, index="myindex", calculate_divisions=True, **read_engine
    )
    assert_eq(ddf, ddf2)


def test_columns_auto_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **write_engine)

    # ### Empty columns ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(fn, columns=[], calculate_divisions=True, **read_engine),
        ddf[[]],
    )

    # No divisions
    assert_eq(
        dd.read_parquet(fn, columns=[], calculate_divisions=False, **read_engine),
        ddf[[]].clear_divisions(),
        check_divisions=True,
    )

    # ### Single column, auto select index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(fn, columns=["x"], calculate_divisions=True, **read_engine),
        ddf[["x"]],
    )

    # No divisions
    assert_eq(
        dd.read_parquet(fn, columns=["x"], calculate_divisions=False, **read_engine),
        ddf[["x"]].clear_divisions(),
        check_divisions=True,
    )


def test_columns_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **write_engine)

    # With Index
    # ----------
    # ### Empty columns, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(
            fn,
            columns=[],
            index="myindex",
            calculate_divisions=True,
            **read_engine,
        ),
        ddf[[]],
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn,
            columns=[],
            index="myindex",
            calculate_divisions=False,
            **read_engine,
        ),
        ddf[[]].clear_divisions(),
        check_divisions=True,
    )

    # ### Single column, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x"],
            calculate_divisions=True,
            **read_engine,
        ),
        ddf[["x"]],
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x"],
            calculate_divisions=False,
            **read_engine,
        ),
        ddf[["x"]].clear_divisions(),
        check_divisions=True,
    )

    # ### Two columns, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x", "y"],
            calculate_divisions=True,
            **read_engine,
        ),
        ddf,
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x", "y"],
            calculate_divisions=False,
            **read_engine,
        ),
        ddf.clear_divisions(),
        check_divisions=True,
    )


def test_nonsense_column(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **engine)
    with pytest.raises((ValueError, KeyError)):
        dd.read_parquet(fn, columns=["nonsense"], **engine)
    with pytest.raises((Exception, KeyError)):
        dd.read_parquet(fn, columns=["nonsense"] + list(ddf.columns), **engine)


def test_columns_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **write_engine)
    ddf2 = ddf.reset_index()

    # No Index
    # --------
    # All columns, none as index
    assert_eq(
        dd.read_parquet(fn, index=False, calculate_divisions=True, **read_engine),
        ddf2,
        check_index=False,
        check_divisions=True,
    )

    # Two columns, none as index
    assert_eq(
        dd.read_parquet(
            fn,
            index=False,
            columns=["x", "y"],
            calculate_divisions=True,
            **read_engine,
        ),
        ddf2[["x", "y"]],
        check_index=False,
        check_divisions=True,
    )

    # One column and one index, all as columns
    assert_eq(
        dd.read_parquet(
            fn,
            index=False,
            columns=["myindex", "x"],
            calculate_divisions=True,
            **read_engine,
        ),
        ddf2[["myindex", "x"]],
        check_index=False,
        check_divisions=True,
    )


def test_calculate_divisions_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, write_index=False, **write_engine)

    df = dd.read_parquet(fn, index=False, **read_engine)
    assert df.index.name is None
    assert not df.known_divisions


def test_columns_index_with_multi_index(tmpdir, engine):
    fn = os.path.join(str(tmpdir), "test.parquet")
    index = pd.MultiIndex.from_arrays(
        [np.arange(10), np.arange(10) + 1], names=["x0", "x1"]
    )
    df = pd.DataFrame(np.random.randn(10, 2), columns=["a", "b"], index=index)
    df2 = df.reset_index(drop=False)

    if engine == {"engine": "fastparquet"}:
        fastparquet.write(fn, df.reset_index(), write_index=False)

    else:
        pq.write_table(pa.Table.from_pandas(df.reset_index(), preserve_index=False), fn)

    ddf = dd.read_parquet(fn, index=index.names, **engine)
    assert_eq(ddf, df)

    d = dd.read_parquet(fn, columns="a", index=index.names, **engine)
    assert_eq(d, df["a"])

    d = dd.read_parquet(fn, index=["a", "b"], columns=["x0", "x1"], **engine)
    assert_eq(d, df2.set_index(["a", "b"])[["x0", "x1"]])

    # Just index
    d = dd.read_parquet(fn, index=False, **engine)
    assert_eq(d, df2)

    d = dd.read_parquet(fn, columns=["b"], index=["a"], **engine)
    assert_eq(d, df2.set_index("a")[["b"]])

    d = dd.read_parquet(fn, columns=["a", "b"], index=["x0"], **engine)
    assert_eq(d, df2.set_index("x0")[["a", "b"]])

    # Just columns
    d = dd.read_parquet(fn, columns=["x0", "a"], index=["x1"], **engine)
    assert_eq(d, df2.set_index("x1")[["x0", "a"]])

    # Both index and columns
    d = dd.read_parquet(fn, index=False, columns=["x0", "b"], **engine)
    assert_eq(d, df2[["x0", "b"]])

    for index in ["x1", "b"]:
        d = dd.read_parquet(fn, index=index, columns=["x0", "a"], **engine)
        assert_eq(d, df2.set_index(index)[["x0", "a"]])

    # Columns and index intersect
    for index in ["a", "x0"]:
        with pytest.raises(ValueError):
            d = dd.read_parquet(fn, index=index, columns=["x0", "a"], **engine)

    # Series output
    for ind, col, sol_df in [
        ("x1", "x0", df2.set_index("x1")),
        (False, "b", df2),
        (False, "x0", df2[["x0"]]),
        ("a", "x0", df2.set_index("a")[["x0"]]),
        ("a", "b", df2.set_index("a")),
    ]:
        d = dd.read_parquet(fn, index=ind, columns=col, **engine)
        assert_eq(d, sol_df[col])


def test_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, **write_engine)
    ddf2 = dd.read_parquet(fn, **read_engine)
    assert_eq(df, ddf2, check_index=False)


def test_read_series(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **engine)
    ddf2 = dd.read_parquet(
        fn, columns=["x"], index="myindex", calculate_divisions=True, **engine
    )
    assert_eq(ddf[["x"]], ddf2)

    ddf2 = dd.read_parquet(
        fn, columns="x", index="myindex", calculate_divisions=True, **engine
    )
    assert_eq(ddf.x, ddf2)


def test_names(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, **engine)

    def read(fn, **kwargs):
        return dd.read_parquet(fn, **engine, **kwargs)

    assert set(read(fn).dask) == set(read(fn).dask)

    assert set(read(fn).dask) != set(read(fn, columns=["x"]).dask)

    assert set(read(fn, columns=("x",)).dask) == set(read(fn, columns=["x"]).dask)


def test_roundtrip_from_pandas(tmpdir, write_engine, read_engine):
    fn = str(tmpdir.join("test.parquet"))
    dfp = df.copy()
    dfp.index.name = "index"
    dfp.to_parquet(fn, **write_engine)
    ddf = dd.read_parquet(fn, index="index", **read_engine)
    assert_eq(dfp, ddf)


@PYARROW_MARK
def test_roundtrip_nullable_dtypes(tmp_path):
    """Test round-tripping nullable extension dtypes. Parquet engines will
    typically add dtype metadata for this.
    """
    df = pd.DataFrame(
        {
            "a": pd.Series([1, 2, pd.NA, 3, 4], dtype="Int64"),
            "b": pd.Series([True, pd.NA, False, True, False], dtype="boolean"),
            "c": pd.Series([0.1, 0.2, 0.3, pd.NA, 0.4], dtype="Float64"),
            "d": pd.Series(["a", "b", "c", "d", pd.NA], dtype="string"),
        }
    )
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(tmp_path)
    ddf2 = dd.read_parquet(tmp_path)
    assert_eq(df, ddf2)


@PYARROW_MARK
@pytest.mark.parametrize(
    "dtype_backend",
    [
        "pandas",
        pytest.param(
            "pyarrow",
            marks=pytest.mark.skipif(
                not PANDAS_GE_150, reason="Requires pyarrow-backed nullable dtypes"
            ),
        ),
    ],
)
def test_use_nullable_dtypes(tmp_path, dtype_backend, engine):
    """
    Test reading a parquet file without pandas metadata,
    but forcing use of nullable dtypes where appropriate
    """
    dtype_extra = "" if dtype_backend == "pandas" else "[pyarrow]"
    df = pd.DataFrame(
        {
            "a": pd.Series([1, 2, pd.NA, 3, 4], dtype=f"Int64{dtype_extra}"),
            "b": pd.Series(
                [True, pd.NA, False, True, False], dtype=f"boolean{dtype_extra}"
            ),
            "c": pd.Series([0.1, 0.2, 0.3, pd.NA, 0.4], dtype=f"Float64{dtype_extra}"),
            "d": pd.Series(["a", "b", "c", "d", pd.NA], dtype=f"string{dtype_extra}"),
        }
    )
    ddf = dd.from_pandas(df, npartitions=2)

    @dask.delayed
    def write_partition(df, i):
        """Write a parquet file without the pandas metadata"""
        table = pa.Table.from_pandas(df).replace_schema_metadata({})
        pq.write_table(table, tmp_path / f"part.{i}.parquet")

    # Create a pandas-metadata-free partitioned parquet. By default it will
    # not read into nullable extension dtypes
    partitions = ddf.to_delayed()
    dask.compute([write_partition(p, i) for i, p in enumerate(partitions)])

    # Not supported by fastparquet
    if engine == {"engine": "fastparquet"}:
        with pytest.raises(ValueError, match="`use_nullable_dtypes` is not supported"):
            dd.read_parquet(tmp_path, use_nullable_dtypes=True, **engine)

    # Works in pyarrow
    else:
        # Doesn't round-trip by default when we aren't using nullable dtypes
        with dask.config.set({"dataframe.dtype_backend": dtype_backend}):
            with pytest.raises(AssertionError):
                ddf2 = dd.read_parquet(tmp_path, **engine)
                assert_eq(df, ddf2)

            # Round trip works when we use nullable dtypes
            with pytest.warns(FutureWarning, match="use_nullable_dtypes"):
                ddf2 = dd.read_parquet(tmp_path, use_nullable_dtypes=True, **engine)
                assert_eq(df, ddf2, check_index=False)


@PYARROW_MARK
def test_use_nullable_dtypes_with_types_mapper(tmp_path, engine):
    # Read in dataset with `dtype_backend=numpy_nullable` and a custom pyarrow `types_mapper`.
    # Ensure `types_mapper` takes priority.
    df = pd.DataFrame(
        {
            "a": pd.Series([1, 2, pd.NA, 3, 4], dtype="Int64"),
            "b": pd.Series([True, pd.NA, False, True, False], dtype="boolean"),
            "c": pd.Series([0.1, 0.2, 0.3, pd.NA, 0.4], dtype="Float64"),
            "d": pd.Series(["a", "b", "c", "d", pd.NA], dtype="string"),
        }
    )
    ddf = dd.from_pandas(df, npartitions=3)
    ddf.to_parquet(tmp_path, **engine)

    types_mapper = {
        pa.int64(): pd.Float32Dtype(),
    }
    result = dd.read_parquet(
        tmp_path,
        dtype_backend="numpy_nullable",
        arrow_to_pandas={"types_mapper": types_mapper.get},
    )
    expected = df.astype({"a": pd.Float32Dtype()})
    if pyarrow_version.major >= 12:
        # types_mapper impacts index
        # https://github.com/apache/arrow/issues/34283
        expected.index = expected.index.astype(pd.Float32Dtype())
    assert_eq(result, expected)


def test_categorical(tmpdir, write_engine, read_engine):
    if write_engine == {"engine": "fastparquet"} and read_engine == {}:
        pytest.xfail("Known limitation")

    tmp = str(tmpdir)
    df = pd.DataFrame({"x": ["a", "b", "c"] * 100}, dtype="category")
    ddf = dd.from_pandas(df, npartitions=3)
    dd.to_parquet(ddf, tmp, **write_engine)

    ddf2 = dd.read_parquet(tmp, categories="x", **read_engine)
    assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

    ddf2 = dd.read_parquet(tmp, categories=["x"], **read_engine)
    assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

    # autocat
    if read_engine == {"engine": "fastparquet"}:
        ddf2 = dd.read_parquet(tmp, **read_engine)
        assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

        ddf2.loc[:1000].compute()
        assert assert_eq(df, ddf2)

    # dereference cats
    ddf2 = dd.read_parquet(tmp, categories=[], **read_engine)

    ddf2.loc[:1000].compute()
    assert (df.x == ddf2.x.compute()).all()


@pytest.mark.parametrize("metadata_file", [False, True])
def test_append(tmpdir, engine, metadata_file):
    """Test that appended parquet equal to the original one."""
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {
            "i32": np.arange(1000, dtype=np.int32),
            "i64": np.arange(1000, dtype=np.int64),
            "f": np.arange(1000, dtype=np.float64),
            "bhello": np.random.choice(["hello", "yo", "people"], size=1000).astype(
                "O"
            ),
        }
    )
    df.index.name = "index"

    half = len(df) // 2
    ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
    ddf2 = dd.from_pandas(df.iloc[half:], chunksize=100)
    ddf1.to_parquet(tmp, write_metadata_file=metadata_file, **engine)
    if metadata_file:
        with open(str(tmpdir.join("_metadata")), "rb") as f:
            metadata1 = f.read()
    ddf2.to_parquet(tmp, append=True, **engine)
    if metadata_file:
        with open(str(tmpdir.join("_metadata")), "rb") as f:
            metadata2 = f.read()
        assert metadata2 != metadata1  # 2nd write updated the metadata file

    ddf3 = dd.read_parquet(tmp, **engine)
    assert_eq(df, ddf3)


def test_append_create(tmpdir, engine):
    """Test that appended parquet equal to the original one."""
    tmp_path = str(tmpdir)
    df = pd.DataFrame(
        {
            "i32": np.arange(1000, dtype=np.int32),
            "i64": np.arange(1000, dtype=np.int64),
            "f": np.arange(1000, dtype=np.float64),
            "bhello": np.random.choice(["hello", "yo", "people"], size=1000).astype(
                "O"
            ),
        }
    )
    df.index.name = "index"

    half = len(df) // 2
    ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
    ddf2 = dd.from_pandas(df.iloc[half:], chunksize=100)
    ddf1.to_parquet(tmp_path, append=True, **engine)
    ddf2.to_parquet(tmp_path, append=True, **engine)

    ddf3 = dd.read_parquet(tmp_path, **engine)
    assert_eq(df, ddf3)


def test_append_with_partition(tmpdir, engine):
    tmp = str(tmpdir)
    df0 = pd.DataFrame(
        {
            "lat": np.arange(0, 10, dtype="int64"),
            "lon": np.arange(10, 20, dtype="int64"),
            "value": np.arange(100, 110, dtype="int64"),
        }
    )
    df0.index.name = "index"
    df1 = pd.DataFrame(
        {
            "lat": np.arange(10, 20, dtype="int64"),
            "lon": np.arange(10, 20, dtype="int64"),
            "value": np.arange(120, 130, dtype="int64"),
        }
    )
    df1.index.name = "index"

    # Check that nullable dtypes work
    # (see: https://github.com/dask/dask/issues/8373)
    df0["lat"] = df0["lat"].astype("Int64")
    df1.loc[df1.index[0], "lat"] = np.nan
    df1["lat"] = df1["lat"].astype("Int64")

    dd_df0 = dd.from_pandas(df0, npartitions=1)
    dd_df1 = dd.from_pandas(df1, npartitions=1)
    dd.to_parquet(dd_df0, tmp, partition_on=["lon"], **engine)
    dd.to_parquet(
        dd_df1,
        tmp,
        partition_on=["lon"],
        append=True,
        ignore_divisions=True,
        **engine,
    )

    out = dd.read_parquet(
        tmp, index="index", calculate_divisions=True, **engine
    ).compute()
    # convert categorical to plain int just to pass assert
    out["lon"] = out.lon.astype("int64")
    # sort required since partitioning breaks index order
    assert_eq(
        out.sort_values("value"), pd.concat([df0, df1])[out.columns], check_index=False
    )


def test_partition_on_cats(tmpdir, engine):
    tmp = str(tmpdir)
    d = pd.DataFrame(
        {
            "a": np.random.rand(50),
            "b": np.random.choice(["x", "y", "z"], size=50),
            "c": np.random.choice(["x", "y", "z"], size=50),
        }
    )
    d = dd.from_pandas(d, 2)
    d.to_parquet(tmp, partition_on=["b"], **engine)
    df = dd.read_parquet(tmp, **engine)
    assert set(df.b.cat.categories) == {"x", "y", "z"}


@PYARROW_MARK
@pytest.mark.parametrize("meta", [False, True])
@pytest.mark.parametrize("stats", [False, True])
def test_partition_on_cats_pyarrow(tmpdir, stats, meta):
    tmp = str(tmpdir)
    d = pd.DataFrame(
        {
            "a": np.random.rand(50),
            "b": np.random.choice(["x", "y", "z"], size=50),
            "c": np.random.choice(["x", "y", "z"], size=50),
        }
    )
    d = dd.from_pandas(d, 2)
    d.to_parquet(tmp, partition_on=["b"], write_metadata_file=meta)
    df = dd.read_parquet(tmp, calculate_divisions=stats)
    assert set(df.b.cat.categories) == {"x", "y", "z"}


def test_partition_parallel_metadata(tmpdir, engine):
    # Check that parallel metadata collection works
    # for hive-partitioned data
    tmp = str(tmpdir)
    d = pd.DataFrame(
        {
            "a": np.random.rand(50),
            "b": np.random.choice(["x", "y", "z"], size=50),
            "c": np.random.choice(["x", "y", "z"], size=50),
        }
    )
    d = dd.from_pandas(d, 2)
    d.to_parquet(tmp, partition_on=["b"], write_metadata_file=False, **engine)
    df = dd.read_parquet(tmp, calculate_divisions=True, metadata_task_size=1, **engine)
    assert set(df.b.cat.categories) == {"x", "y", "z"}


def test_partition_on_cats_2(tmpdir, engine):
    tmp = str(tmpdir)
    d = pd.DataFrame(
        {
            "a": np.random.rand(50),
            "b": np.random.choice(["x", "y", "z"], size=50),
            "c": np.random.choice(["x", "y", "z"], size=50),
        }
    )
    d = dd.from_pandas(d, 2)
    d.to_parquet(tmp, partition_on=["b", "c"], **engine)
    df = dd.read_parquet(tmp, **engine)
    assert set(df.b.cat.categories) == {"x", "y", "z"}
    assert set(df.c.cat.categories) == {"x", "y", "z"}

    df = dd.read_parquet(tmp, columns=["a", "c"], **engine)
    assert set(df.c.cat.categories) == {"x", "y", "z"}
    assert "b" not in df.columns
    assert_eq(df, df.compute())
    df = dd.read_parquet(tmp, index="c", **engine)
    assert set(df.index.categories) == {"x", "y", "z"}
    assert "c" not in df.columns
    # series
    df = dd.read_parquet(tmp, columns="b", **engine)
    assert set(df.cat.categories) == {"x", "y", "z"}


@pytest.mark.parametrize("metadata_file", [False, True])
def test_append_wo_index(tmpdir, engine, metadata_file):
    """Test append with write_index=False."""
    tmp = str(tmpdir.join("tmp1.parquet"))
    df = pd.DataFrame(
        {
            "i32": np.arange(1000, dtype=np.int32),
            "i64": np.arange(1000, dtype=np.int64),
            "f": np.arange(1000, dtype=np.float64),
            "bhello": np.random.choice(["hello", "yo", "people"], size=1000).astype(
                "O"
            ),
        }
    )
    half = len(df) // 2
    ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
    ddf2 = dd.from_pandas(df.iloc[half:], chunksize=100)
    ddf1.to_parquet(tmp, write_metadata_file=metadata_file, **engine)

    with pytest.raises(ValueError) as excinfo:
        ddf2.to_parquet(tmp, write_index=False, append=True, **engine)
    assert "Appended columns" in str(excinfo.value)

    tmp = str(tmpdir.join("tmp2.parquet"))
    ddf1.to_parquet(tmp, write_index=False, write_metadata_file=metadata_file, **engine)
    ddf2.to_parquet(tmp, write_index=False, append=True, **engine)

    ddf3 = dd.read_parquet(tmp, index="f", **engine)
    assert_eq(df.set_index("f"), ddf3)


@pytest.mark.parametrize("metadata_file", [False, True])
@pytest.mark.parametrize(
    ("index", "offset"),
    [
        (
            # There is some odd behavior with date ranges and pyarrow in some cirucmstances!
            # https://github.com/pandas-dev/pandas/issues/48573
            pd.date_range("2022-01-01", "2022-01-31", freq="D"),
            pd.Timedelta(days=1),
        ),
        (pd.RangeIndex(0, 500, 1), 499),
    ],
)
def test_append_overlapping_divisions(tmpdir, engine, metadata_file, index, offset):
    """Test raising of error when divisions overlapping."""
    tmp = str(tmpdir)

    df = pd.DataFrame(
        {
            "i32": np.arange(len(index), dtype=np.int32),
            "i64": np.arange(len(index), dtype=np.int64),
            "f": np.arange(len(index), dtype=np.float64),
            "bhello": np.random.choice(
                ["hello", "yo", "people"], size=len(index)
            ).astype("O"),
        },
        index=index,
    )
    ddf1 = dd.from_pandas(df, chunksize=100)
    ddf2 = dd.from_pandas(df.set_index(df.index + offset), chunksize=100)
    ddf1.to_parquet(tmp, write_metadata_file=metadata_file, **engine)

    with pytest.raises(ValueError, match="overlap with previously written divisions"):
        ddf2.to_parquet(tmp, append=True, **engine)

    ddf2.to_parquet(tmp, append=True, ignore_divisions=True, **engine)


def test_append_known_divisions_to_unknown_divisions_works(tmpdir, engine):
    tmp = str(tmpdir)

    df1 = pd.DataFrame(
        {"x": np.arange(100), "y": np.arange(100, 200)}, index=np.arange(100, 0, -1)
    )
    ddf1 = dd.from_pandas(df1, npartitions=3, sort=False)

    df2 = pd.DataFrame({"x": np.arange(100, 200), "y": np.arange(200, 300)})

    # index type should match
    df2.index = df2.index.astype(df1.index.dtype)

    ddf2 = dd.from_pandas(df2, npartitions=3)

    # fastparquet always loads all metadata when appending, pyarrow only does
    # if a `_metadata` file exists. If we know the existing divisions aren't
    # sorted, then we want to skip erroring for overlapping divisions. Setting
    # `write_metadata_file=True` ensures this test works the same across both
    # engines.
    ddf1.to_parquet(tmp, write_metadata_file=True, **engine)
    ddf2.to_parquet(tmp, append=True, **engine)

    res = dd.read_parquet(tmp, **engine)
    sol = pd.concat([df1, df2])
    assert_eq(res, sol)


@pytest.mark.parametrize("metadata_file", [False, True])
def test_append_different_columns(tmpdir, engine, metadata_file):
    """Test raising of error when non equal columns."""
    tmp = str(tmpdir)
    df1 = pd.DataFrame({"i32": np.arange(100, dtype=np.int32)})
    df2 = pd.DataFrame({"i64": np.arange(100, dtype=np.int64)})
    df3 = pd.DataFrame({"i32": np.arange(100, dtype=np.int64)})

    ddf1 = dd.from_pandas(df1, chunksize=2)
    ddf2 = dd.from_pandas(df2, chunksize=2)
    ddf3 = dd.from_pandas(df3, chunksize=2)

    ddf1.to_parquet(tmp, write_metadata_file=metadata_file, **engine)

    with pytest.raises(ValueError) as excinfo:
        ddf2.to_parquet(tmp, append=True, **engine)
    assert "Appended columns" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        ddf3.to_parquet(tmp, append=True, **engine)
    assert "Appended dtypes" in str(excinfo.value)


@PYARROW_MARK
@pytest.mark.skip_with_pyarrow_strings  # need an object to store a dict
def test_append_dict_column(tmpdir):
    """See: https://github.com/dask/dask/issues/7492
    Note: fastparquet engine is missing dict-column support
    """
    tmp = str(tmpdir)
    dts = pd.date_range("2020-01-01", "2021-01-01")
    df = pd.DataFrame(
        {"value": [{"x": x} for x in range(len(dts))]},
        index=dts,
    )
    ddf1 = dd.from_pandas(df, npartitions=1)

    schema = {"value": pa.struct([("x", pa.int32())])}

    # Write ddf1 to tmp, and then append it again
    ddf1.to_parquet(tmp, append=True, schema=schema)
    ddf1.to_parquet(tmp, append=True, schema=schema, ignore_divisions=True)

    # Read back all data (ddf1 + ddf1)
    ddf2 = dd.read_parquet(tmp)

    # Check computed result
    expect = pd.concat([df, df])
    result = ddf2.compute()
    assert_eq(expect, result)


def test_ordering(tmpdir, write_engine, read_engine):
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]},
        index=pd.Index([-1, -2, -3], name="myindex"),
        columns=["c", "a", "b"],
    )
    ddf = dd.from_pandas(df, npartitions=2)
    dd.to_parquet(ddf, tmp, **write_engine)

    ddf2 = dd.read_parquet(tmp, index="myindex", **read_engine)
    assert_eq(ddf, ddf2, check_divisions=False)


def test_read_parquet_custom_columns(tmpdir, engine):
    tmp = str(tmpdir)
    data = pd.DataFrame(
        {"i32": np.arange(1000, dtype=np.int32), "f": np.arange(1000, dtype=np.float64)}
    )
    df = dd.from_pandas(data, chunksize=50)
    df.to_parquet(tmp, **engine)

    df2 = dd.read_parquet(tmp, columns=["i32", "f"], calculate_divisions=True, **engine)
    assert_eq(df[["i32", "f"]], df2, check_index=False)

    fns = glob.glob(os.path.join(tmp, "*.parquet"))
    df2 = dd.read_parquet(fns, columns=["i32"], **engine).compute()
    df2.sort_values("i32", inplace=True)
    assert_eq(df[["i32"]], df2, check_index=False, check_divisions=False)

    df3 = dd.read_parquet(tmp, columns=["f", "i32"], calculate_divisions=True, **engine)
    assert_eq(df[["f", "i32"]], df3, check_index=False)


@pytest.mark.parametrize(
    "df,write_kwargs,read_kwargs",
    [
        (pd.DataFrame({"x": [3, 2, 1]}), {}, {}),
        (pd.DataFrame({"x": ["c", "a", "b"]}), {}, {}),
        (pd.DataFrame({"x": ["cc", "a", "bbb"]}), {}, {}),
        (
            pd.DataFrame({"x": [b"a", b"b", b"c"]}),
            {"object_encoding": "bytes", "schema": {"x": pa.binary()} if pa else None},
            {},
        ),
        (
            pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])}),
            {},
            {"categories": ["x"]},
        ),
        (pd.DataFrame({"x": pd.Categorical([1, 2, 1])}), {}, {"categories": ["x"]}),
        (pd.DataFrame({"x": list(map(pd.Timestamp, [3000, 2000, 1000]))}), {}, {}),
        (pd.DataFrame({"x": [3000, 2000, 1000]}).astype("M8[ns]"), {}, {}),
        (pd.DataFrame({"x": [3, 2, 1]}).astype("M8[ns]"), {}, {}),
        (pd.DataFrame({"x": [3, 2, 1]}).astype("M8[us]"), {}, {}),
        (pd.DataFrame({"x": [3, 2, 1]}).astype("M8[ms]"), {}, {}),
        (pd.DataFrame({"x": [3000, 2000, 1000]}).astype("datetime64[ns]"), {}, {}),
        (pd.DataFrame({"x": [3000, 2000, 1000]}).astype("datetime64[ns, UTC]"), {}, {}),
        (pd.DataFrame({"x": [3000, 2000, 1000]}).astype("datetime64[ns, CET]"), {}, {}),
        (pd.DataFrame({"x": [3, 2, 1]}).astype("uint16"), {}, {}),
        (pd.DataFrame({"x": [3, 2, 1]}).astype("float32"), {}, {}),
        (pd.DataFrame({"x": [3, 1, 2]}, index=[3, 2, 1]), {}, {}),
        (pd.DataFrame({"x": [3, 1, 5]}, index=pd.Index([1, 2, 3], name="foo")), {}, {}),
        (pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]}), {}, {}),
        (pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]}, columns=["y", "x"]), {}, {}),
        (pd.DataFrame({"0": [3, 2, 1]}), {}, {}),
        (pd.DataFrame({"x": [3, 2, None]}), {}, {}),
        (pd.DataFrame({"-": [3.0, 2.0, None]}), {}, {}),
        (pd.DataFrame({".": [3.0, 2.0, None]}), {}, {}),
        (pd.DataFrame({" ": [3.0, 2.0, None]}), {}, {}),
    ],
)
@pytest.mark.skip_with_pyarrow_strings  # don't want to convert binary data to pyarrow strings
def test_roundtrip(tmpdir, df, write_kwargs, read_kwargs, engine):
    if "x" in df and df.x.dtype == "M8[ns]" and engine == {}:
        pytest.xfail(reason="Parquet pyarrow v1 doesn't support nanosecond precision")
    if (
        "x" in df
        and df.x.dtype == "M8[ns]"
        and engine == {"engine": "fastparquet"}
        and fastparquet_version <= parse_version("0.6.3")
    ):
        pytest.xfail(reason="fastparquet doesn't support nanosecond precision yet")
    # non-ns times
    if (
        PANDAS_GE_200
        and "x" in df
        and (df.x.dtype == "M8[ms]" or df.x.dtype == "M8[us]")
    ):
        if engine == {}:  # pyarrow
            pytest.xfail("https://github.com/apache/arrow/issues/15079")
        elif engine == {
            "engine": "fastparquet"
        } and fastparquet_version <= parse_version("2022.12.0"):
            pytest.xfail(reason="https://github.com/dask/fastparquet/issues/837")

    if (
        read_kwargs.get("categories", None)
        and engine == {"engine": "fastparquet"}
        and fastparquet_version <= parse_version("0.6.3")
    ):
        pytest.xfail("https://github.com/dask/fastparquet/issues/577")

    tmp = str(tmpdir)
    if df.index.name is None:
        df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)

    oe = write_kwargs.pop("object_encoding", None)
    if oe and engine == {"engine": "fastparquet"}:
        dd.to_parquet(ddf, tmp, object_encoding=oe, **engine, **write_kwargs)
    else:
        dd.to_parquet(ddf, tmp, **engine, **write_kwargs)
    ddf2 = dd.read_parquet(
        tmp, index=df.index.name, calculate_divisions=True, **engine, **read_kwargs
    )
    if str(ddf2.dtypes.get("x")) == "UInt16" and engine == {"engine": "fastparquet"}:
        # fastparquet choooses to use masked type to be able to get true repr of
        # 16-bit int
        assert_eq(ddf.astype("UInt16"), ddf2, check_divisions=False)
    else:
        assert_eq(ddf, ddf2, check_divisions=False)


@pytest.mark.xfail(
    pyarrow_strings_enabled() and pyarrow_version < parse_version("12.0.0"),
    reason="Known failure with pyarrow strings: https://github.com/apache/arrow/issues/33727",
)
def test_categories(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": list("caaab")})

    if engine == {"engine": "fastparquet"}:
        with dask.config.set({"dataframe.convert-string": False}):
            ddf = dd.from_pandas(df, npartitions=2)
            ddf["y"] = ddf.y.astype("category")
    else:
        ddf = dd.from_pandas(df, npartitions=2)
        ddf["y"] = ddf.y.astype("category")

    ddf.to_parquet(fn, **engine)
    ddf2 = dd.read_parquet(fn, categories=["y"], calculate_divisions=True, **engine)

    # Shouldn't need to specify categories explicitly
    ddf3 = dd.read_parquet(fn, calculate_divisions=True, **engine)
    assert_eq(ddf3, ddf2)

    with pytest.raises(NotImplementedError):
        ddf2.y.cat.categories
    assert set(ddf2.y.compute().cat.categories) == {"a", "b", "c"}
    cats_set = ddf2.map_partitions(lambda x: x.y.cat.categories.sort_values()).compute()
    assert cats_set.tolist() == ["a", "c", "a", "b"]

    if engine == {"engine": "fastparquet"}:
        assert_eq(ddf.y, ddf2.y, check_names=False)
        with pytest.raises(TypeError):
            # attempt to load as category that which is not so encoded
            dd.read_parquet(fn, categories=["x"], **engine).compute()

    with pytest.raises((ValueError, FutureWarning)):
        # attempt to load as category unknown column
        dd.read_parquet(fn, categories=["foo"], **engine)


@pytest.mark.xfail_with_pyarrow_strings  # https://github.com/apache/arrow/issues/33727
def test_categories_unnamed_index(tmpdir, engine):
    # Check that we can handle an unnamed categorical index
    # https://github.com/dask/dask/issues/6885

    tmpdir = str(tmpdir)

    df = pd.DataFrame(
        data={"A": [1, 2, 3], "B": ["a", "a", "b"]}, index=["x", "y", "y"]
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = ddf.categorize(columns=["B"])

    ddf.to_parquet(tmpdir, **engine)
    ddf2 = dd.read_parquet(tmpdir, **engine)

    assert_eq(ddf.index, ddf2.index, check_divisions=False)


def test_empty_partition(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    ddf = dd.from_pandas(df, npartitions=5)

    ddf2 = ddf[ddf.a <= 5]
    ddf2.to_parquet(fn, **engine)

    # Pyarrow engine will not filter out emtpy
    # partitions unless calculate_divisions=True
    ddf3 = dd.read_parquet(fn, calculate_divisions=True, **engine)
    assert ddf3.npartitions < 5
    sol = ddf2.compute()
    assert_eq(sol, ddf3, check_names=False, check_index=False)


@pytest.mark.parametrize("write_metadata", [True, False])
def test_timestamp_index(tmpdir, engine, write_metadata):
    fn = str(tmpdir)
    df = dd._compat.makeTimeDataFrame()
    df.index.name = "foo"
    ddf = dd.from_pandas(df, npartitions=5)
    ddf.to_parquet(fn, write_metadata_file=write_metadata, **engine)
    ddf2 = dd.read_parquet(fn, calculate_divisions=True, **engine)
    assert_eq(ddf, ddf2)


@PYARROW_MARK
@FASTPARQUET_MARK
def test_to_parquet_fastparquet_default_writes_nulls(tmpdir):
    fn = str(tmpdir.join("test.parquet"))

    df = pd.DataFrame({"c1": [1.0, np.nan, 2, np.nan, 3]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.warns(FutureWarning):
        ddf.to_parquet(fn, engine="fastparquet")
    table = pq.read_table(fn)
    assert table[1].null_count == 2


@PYARROW_MARK
@pytest.mark.skip_with_pyarrow_strings  # need object columns to store arrays
def test_to_parquet_pyarrow_w_inconsistent_schema_by_partition_succeeds_w_manual_schema(
    tmpdir,
):
    # Data types to test: strings, arrays, ints, timezone aware timestamps
    in_arrays = [[0, 1, 2], [3, 4], np.nan, np.nan]
    out_arrays = [[0, 1, 2], [3, 4], None, None]
    in_strings = ["a", "b", np.nan, np.nan]
    out_strings = ["a", "b", None, None]
    tstamp = pd.Timestamp(1513393355, unit="s")
    in_tstamps = [tstamp, tstamp, pd.NaT, pd.NaT]
    out_tstamps = [
        # Timestamps come out in numpy.datetime64 format
        tstamp.to_datetime64(),
        tstamp.to_datetime64(),
        np.datetime64("NaT"),
        np.datetime64("NaT"),
    ]
    timezone = "US/Eastern"
    tz_tstamp = pd.Timestamp(1513393355, unit="s", tz=timezone)
    in_tz_tstamps = [tz_tstamp, tz_tstamp, pd.NaT, pd.NaT]
    out_tz_tstamps = [
        # Timezones do not make it through a write-read cycle.
        tz_tstamp.tz_convert(None).to_datetime64(),
        tz_tstamp.tz_convert(None).to_datetime64(),
        np.datetime64("NaT"),
        np.datetime64("NaT"),
    ]

    df = pd.DataFrame(
        {
            "partition_column": [0, 0, 1, 1],
            "arrays": in_arrays,
            "strings": in_strings,
            "tstamps": in_tstamps,
            "tz_tstamps": in_tz_tstamps,
        }
    )

    ddf = dd.from_pandas(df, npartitions=2)
    schema = pa.schema(
        [
            ("arrays", pa.list_(pa.int64())),
            ("strings", pa.string()),
            ("tstamps", pa.timestamp("ns")),
            ("tz_tstamps", pa.timestamp("ns", timezone)),
            ("partition_column", pa.int64()),
        ]
    )
    ddf.to_parquet(str(tmpdir), partition_on="partition_column", schema=schema)
    ddf_after_write = (
        dd.read_parquet(str(tmpdir), calculate_divisions=False)
        .compute()
        .reset_index(drop=True)
    )

    # Check array support
    arrays_after_write = ddf_after_write.arrays.values
    for i in range(len(df)):
        assert np.array_equal(arrays_after_write[i], out_arrays[i]), type(out_arrays[i])

    # Check datetime support
    tstamps_after_write = ddf_after_write.tstamps.values
    for i in range(len(df)):
        # Need to test NaT separately
        if np.isnat(tstamps_after_write[i]):
            assert np.isnat(out_tstamps[i])
        else:
            assert tstamps_after_write[i] == out_tstamps[i]

    # Check timezone aware datetime support
    tz_tstamps_after_write = ddf_after_write.tz_tstamps.values
    for i in range(len(df)):
        # Need to test NaT separately
        if np.isnat(tz_tstamps_after_write[i]):
            assert np.isnat(out_tz_tstamps[i])
        else:
            assert tz_tstamps_after_write[i] == out_tz_tstamps[i]

    # Check string support
    assert np.array_equal(ddf_after_write.strings.values, out_strings)

    # Check partition column
    assert np.array_equal(ddf_after_write.partition_column, df.partition_column)


@PYARROW_MARK
@pytest.mark.parametrize("index", [False, True])
@pytest.mark.parametrize("schema", ["infer", "complex"])
def test_pyarrow_schema_inference(tmpdir, index, schema):
    if schema == "complex":
        schema = {"index": pa.string(), "amount": pa.int64()}

    tmpdir = str(tmpdir)
    df = pd.DataFrame(
        {
            "index": ["1", "2", "3", "2", "3", "1", "4"],
            "date": pd.to_datetime(
                [
                    "2017-01-01",
                    "2017-01-01",
                    "2017-01-01",
                    "2017-01-02",
                    "2017-01-02",
                    "2017-01-06",
                    "2017-01-09",
                ]
            ),
            "amount": [100, 200, 300, 400, 500, 600, 700],
        },
        index=range(7, 14),
    )
    if index:
        df = dd.from_pandas(df, npartitions=2).set_index("index")
    else:
        df = dd.from_pandas(df, npartitions=2)

    df.to_parquet(tmpdir, schema=schema)
    df_out = dd.read_parquet(tmpdir, calculate_divisions=True)
    assert_eq(df, df_out)


@PYARROW_MARK
def test_pyarrow_schema_mismatch_error(tmpdir):
    df1 = pd.DataFrame({"x": [1, 2, 3], "y": [4.5, 6, 7]})
    df2 = pd.DataFrame({"x": [4, 5, 6], "y": ["a", "b", "c"]})

    ddf = dd.from_delayed(
        [dask.delayed(df1), dask.delayed(df2)], meta=df1, verify_meta=False
    )

    with pytest.raises(ValueError) as rec:
        ddf.to_parquet(str(tmpdir))

    msg = str(rec.value)
    assert "Failed to convert partition to expected pyarrow schema" in msg
    assert "y: double" in str(rec.value)
    assert "y: string" in str(rec.value)


@PYARROW_MARK
def test_pyarrow_schema_mismatch_explicit_schema_none(tmpdir):
    df1 = pd.DataFrame({"x": [1, 2, 3], "y": [4.5, 6, 7]})
    df2 = pd.DataFrame({"x": [4, 5, 6], "y": ["a", "b", "c"]})
    ddf = dd.from_delayed(
        [dask.delayed(df1), dask.delayed(df2)], meta=df1, verify_meta=False
    )
    ddf.to_parquet(str(tmpdir), schema=None)
    res = dd.read_parquet(tmpdir)
    sol = pd.concat([df1, df2])
    # Only checking that the data was written correctly, we don't care about
    # the incorrect _meta from read_parquet
    assert_eq(res, sol, check_dtype=False)


def test_partition_on(tmpdir, engine):
    tmpdir = str(tmpdir)
    df = pd.DataFrame(
        {
            "a1": np.random.choice(["A", "B", "C"], size=100),
            "a2": np.random.choice(["X", "Y", "Z"], size=100),
            "b": np.random.random(size=100),
            "c": np.random.randint(1, 5, size=100),
            "d": np.arange(0, 100),
        }
    )
    d = dd.from_pandas(df, npartitions=2)
    d.to_parquet(tmpdir, partition_on=["a1", "a2"], **engine)
    # Note #1: Cross-engine functionality is missing
    # Note #2: The index is not preserved in pyarrow when partition_on is used
    out = dd.read_parquet(
        tmpdir, index=False, calculate_divisions=False, **engine
    ).compute()
    for val in df.a1.unique():
        assert set(df.d[df.a1 == val]) == set(out.d[out.a1 == val])

    # Now specify the columns and allow auto-index detection
    out = dd.read_parquet(tmpdir, columns=["d", "a2"], **engine).compute()
    for val in df.a2.unique():
        assert set(df.d[df.a2 == val]) == set(out.d[out.a2 == val])


def test_partition_on_duplicates(tmpdir, engine):
    # https://github.com/dask/dask/issues/6445
    tmpdir = str(tmpdir)
    df = pd.DataFrame(
        {
            "a1": np.random.choice(["A", "B", "C"], size=100),
            "a2": np.random.choice(["X", "Y", "Z"], size=100),
            "data": np.random.random(size=100),
        }
    )
    d = dd.from_pandas(df, npartitions=2)

    for _ in range(2):
        d.to_parquet(tmpdir, partition_on=["a1", "a2"], **engine)

    out = dd.read_parquet(tmpdir, **engine).compute()

    assert len(df) == len(out)
    for _, _, files in os.walk(tmpdir):
        for file in files:
            assert file in (
                "part.0.parquet",
                "part.1.parquet",
                "_common_metadata",
                "_metadata",
            )


@PYARROW_MARK
@pytest.mark.parametrize("partition_on", ["aa", ["aa"]])
def test_partition_on_string(tmpdir, partition_on):
    tmpdir = str(tmpdir)
    with dask.config.set(scheduler="single-threaded"):
        tmpdir = str(tmpdir)
        df = pd.DataFrame(
            {
                "aa": np.random.choice(["A", "B", "C"], size=100),
                "bb": np.random.random(size=100),
                "cc": np.random.randint(1, 5, size=100),
            }
        )
        d = dd.from_pandas(df, npartitions=2)
        d.to_parquet(tmpdir, partition_on=partition_on, write_index=False)
        out = dd.read_parquet(tmpdir, index=False, calculate_divisions=False)

    out = out.compute()
    for val in df.aa.unique():
        assert set(df.bb[df.aa == val]) == set(out.bb[out.aa == val])


def test_filters_categorical(tmpdir, write_engine, read_engine):
    tmpdir = str(tmpdir)
    cats = ["2018-01-01", "2018-01-02", "2018-01-03", "2018-01-04"]
    dftest = pd.DataFrame(
        {
            "dummy": [1, 1, 1, 1],
            "DatePart": pd.Categorical(cats, categories=cats, ordered=True),
        }
    )
    ddftest = dd.from_pandas(dftest, npartitions=4).set_index("dummy")
    ddftest.to_parquet(tmpdir, partition_on="DatePart", **write_engine)
    ddftest_read = dd.read_parquet(
        tmpdir,
        index="dummy",
        filters=[(("DatePart", "<=", "2018-01-02"))],
        calculate_divisions=True,
        **read_engine,
    )
    assert len(ddftest_read) == 2


def test_filters(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    df = pd.DataFrame({"x": range(10), "y": list("aabbccddee")})
    ddf = dd.from_pandas(df, npartitions=5)
    assert ddf.npartitions == 5

    ddf.to_parquet(tmp_path, write_metadata_file=True, **write_engine)

    a = dd.read_parquet(tmp_path, filters=[("x", ">", 4)], **read_engine)
    assert a.npartitions == 3
    assert (a.x > 3).all().compute()

    b = dd.read_parquet(tmp_path, filters=[("y", "==", "c")], **read_engine)
    assert b.npartitions == 1
    assert (b.y == "c").all().compute()

    c = dd.read_parquet(
        tmp_path, filters=[("y", "==", "c"), ("x", ">", 6)], **read_engine
    )
    assert c.npartitions <= 1
    assert not len(c)
    assert_eq(c, c)

    d = dd.read_parquet(
        tmp_path,
        filters=[
            # Select two overlapping ranges
            [("x", ">", 1), ("x", "<", 6)],
            [("x", ">", 3), ("x", "<", 8)],
        ],
        **read_engine,
    )
    assert d.npartitions == 3
    assert ((d.x > 1) & (d.x < 8)).all().compute()

    e = dd.read_parquet(tmp_path, filters=[("x", "in", (0, 9))], **read_engine)
    assert e.npartitions == 2
    assert ((e.x < 2) | (e.x > 7)).all().compute()

    f = dd.read_parquet(tmp_path, filters=[("y", "=", "c")], **read_engine)
    assert f.npartitions == 1
    assert len(f)
    assert (f.y == "c").all().compute()

    g = dd.read_parquet(tmp_path, filters=[("x", "!=", 1)], **read_engine)
    assert g.npartitions == 5


def test_filters_v0(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"at": ["ab", "aa", "ba", "da", "bb"]})
    ddf = dd.from_pandas(df, npartitions=1)

    # Ok with 1 partition and filters
    ddf.repartition(npartitions=1, force=True).to_parquet(
        fn, write_index=False, **write_engine
    )
    ddf2 = dd.read_parquet(
        fn, index=False, filters=[("at", "==", "aa")], **read_engine
    ).compute()
    ddf3 = dd.read_parquet(
        fn, index=False, filters=[("at", "=", "aa")], **read_engine
    ).compute()

    # Recent versions of pyarrow support full row-wise filtering
    # (fastparquet and older pyarrow versions do not)
    if read_engine == {}:  # pyarrow
        assert_eq(ddf2, ddf[ddf["at"] == "aa"], check_index=False)
        assert_eq(ddf3, ddf[ddf["at"] == "aa"], check_index=False)
    else:
        assert_eq(ddf2, ddf)
        assert_eq(ddf3, ddf)

    # with >1 partition and no filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn, **write_engine)
    ddf2 = dd.read_parquet(fn, **read_engine).compute()
    assert_eq(ddf2, ddf)

    # with >1 partition and filters using base fastparquet
    if read_engine == {"engine": "fastparquet"}:
        ddf.repartition(npartitions=2, force=True).to_parquet(fn, **write_engine)
        df2 = fastparquet.ParquetFile(fn).to_pandas(filters=[("at", "==", "aa")])
        df3 = fastparquet.ParquetFile(fn).to_pandas(filters=[("at", "=", "aa")])
        assert len(df2) > 0
        assert len(df3) > 0

    # with >1 partition and filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn, **write_engine)
    ddf2 = dd.read_parquet(fn, filters=[("at", "==", "aa")], **read_engine).compute()
    ddf3 = dd.read_parquet(fn, filters=[("at", "=", "aa")], **read_engine).compute()
    assert len(ddf2) > 0
    assert len(ddf3) > 0
    assert_eq(ddf2, ddf3)


@PYARROW_MARK
def test_filtering_pyarrow_dataset(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"aa": range(100), "bb": ["cat", "dog"] * 50})
    ddf = dd.from_pandas(df, npartitions=10)
    ddf.to_parquet(fn, write_index=False, write_metadata_file=True, **engine)

    # Filtered read
    aa_lim = 40
    bb_val = "dog"
    filters = [[("aa", "<", aa_lim), ("bb", "==", bb_val)]]
    ddf2 = dd.read_parquet(fn, index=False, filters=filters)

    # Check that partitions are filtered for "aa" filter
    nonempty = 0
    for part in ddf[ddf["aa"] < aa_lim].partitions:
        nonempty += int(len(part.compute()) > 0)
    assert ddf2.npartitions == nonempty

    # Check that rows are filtered for "aa" and "bb" filters
    df = df[df["aa"] < aa_lim]
    df = df[df["bb"] == bb_val]
    assert_eq(df, ddf2.compute(), check_index=False)


def test_filters_file_list(tmpdir, engine):
    df = pd.DataFrame({"x": range(10), "y": list("aabbccddee")})
    ddf = dd.from_pandas(df, npartitions=5)

    ddf.to_parquet(str(tmpdir), **engine)
    files = str(tmpdir.join("*.parquet"))
    ddf_out = dd.read_parquet(
        files, calculate_divisions=True, filters=[("x", ">", 3)], **engine
    )

    assert ddf_out.npartitions == 3
    assert_eq(df[df["x"] > 3], ddf_out.compute(), check_index=False)

    # Check that first partition gets filtered for single-path input
    ddf2 = dd.read_parquet(
        str(tmpdir.join("part.0.parquet")),
        calculate_divisions=True,
        filters=[("x", ">", 3)],
        **engine,
    )
    assert len(ddf2) == 0

    # Make sure files list can be read with filters when they don't have the same columns order
    pd.read_parquet(os.path.join(tmpdir, "part.4.parquet"), **engine)[
        reversed(df.columns)
    ].to_parquet(os.path.join(tmpdir, "part.4.parquet"), **engine)
    ddf3 = dd.read_parquet(
        str(tmpdir.join("*.parquet")),
        calculate_divisions=True,
        filters=[("x", ">", 3)],
        **engine,
    )
    assert ddf3.npartitions == 3
    assert_eq(df[df["x"] > 3], ddf3, check_index=False)


@PYARROW_MARK
def test_pyarrow_filter_divisions(tmpdir):
    # Write simple dataset with an index that will only
    # have a sorted index if certain row-groups are filtered out.
    # In this case, we filter "a" <= 3 to get a sorted
    # index. Otherwise, "a" is NOT monotonically increasing.
    df = pd.DataFrame({"a": [0, 1, 10, 12, 2, 3, 8, 9], "b": range(8)}).set_index("a")
    df.iloc[:4].to_parquet(str(tmpdir.join("file.0.parquet")), row_group_size=2)
    df.iloc[4:].to_parquet(str(tmpdir.join("file.1.parquet")), row_group_size=2)

    # Only works for ArrowDatasetEngine.
    # Legacy code will not apply filters on individual row-groups
    # when `split_row_groups=False`.
    ddf = dd.read_parquet(
        str(tmpdir),
        split_row_groups=False,
        calculate_divisions=True,
        filters=[("a", "<=", 3)],
    )
    assert ddf.divisions == (0, 2, 3)

    ddf = dd.read_parquet(
        str(tmpdir),
        split_row_groups=True,
        calculate_divisions=True,
        filters=[("a", "<=", 3)],
    )
    assert ddf.divisions == (0, 2, 3)


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_divisions_read_with_filters(tmpdir):
    tmpdir = str(tmpdir)
    # generate dataframe
    size = 100
    categoricals = []
    for value in ["a", "b", "c", "d"]:
        categoricals += [value] * int(size / 4)
    df = pd.DataFrame(
        {
            "a": categoricals,
            "b": np.random.random(size=size),
            "c": np.random.randint(1, 5, size=size),
        }
    )
    d = dd.from_pandas(df, npartitions=4)
    # save it
    d.to_parquet(tmpdir, write_index=True, partition_on=["a"], engine="fastparquet")
    # read it
    out = dd.read_parquet(
        tmpdir,
        engine="fastparquet",
        filters=[("a", "==", "b")],
        calculate_divisions=True,
    )
    # test it
    expected_divisions = (25, 49)
    assert out.divisions == expected_divisions


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_divisions_are_known_read_with_filters(tmpdir):
    tmpdir = str(tmpdir)
    # generate dataframe
    df = pd.DataFrame(
        {
            "unique": [0, 0, 1, 1, 2, 2, 3, 3],
            "id": ["id1", "id2", "id1", "id2", "id1", "id2", "id1", "id2"],
        },
        index=[0, 0, 1, 1, 2, 2, 3, 3],
    )
    d = dd.from_pandas(df, npartitions=2)
    # save it
    d.to_parquet(tmpdir, partition_on=["id"], engine="fastparquet")
    # read it
    out = dd.read_parquet(
        tmpdir,
        engine="fastparquet",
        filters=[("id", "==", "id1")],
        calculate_divisions=True,
    )
    # test it
    assert out.known_divisions
    expected_divisions = (0, 2, 3)
    assert out.divisions == expected_divisions


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_to_parquet_lazy(tmpdir, scheduler, engine):
    tmpdir = str(tmpdir)
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [1.0, 2.0, 3.0, 4.0]})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    value = ddf.to_parquet(tmpdir, compute=False, **engine)

    assert hasattr(value, "dask")
    value.compute(scheduler=scheduler)
    assert os.path.exists(tmpdir)

    ddf2 = dd.read_parquet(tmpdir, calculate_divisions=True, **engine)

    assert_eq(ddf, ddf2, check_divisions=False)


@PYARROW_MARK
@pytest.mark.parametrize("compute", [False, True])
def test_to_parquet_calls_invalidate_cache(tmpdir, monkeypatch, compute):
    from fsspec.implementations.local import LocalFileSystem

    invalidate_cache = MagicMock()
    monkeypatch.setattr(LocalFileSystem, "invalidate_cache", invalidate_cache)
    ddf.to_parquet(tmpdir, compute=compute)
    path = LocalFileSystem._strip_protocol(str(tmpdir))
    assert invalidate_cache.called
    assert invalidate_cache.call_args.args[0] == path


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_timestamp96(tmpdir):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [pd.to_datetime("now", utc=True)]})
    ddf = dd.from_pandas(df, 1)
    ddf.to_parquet(fn, engine="fastparquet", write_index=False, times="int96")
    pf = fastparquet.ParquetFile(fn)
    assert pf._schema[1].type == fastparquet.parquet_thrift.Type.INT96
    out = dd.read_parquet(fn, engine="fastparquet", index=False).compute()
    assert_eq(out, df)


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_drill_scheme(tmpdir):
    fn = str(tmpdir)
    N = 5
    df1 = pd.DataFrame({c: np.random.random(N) for i, c in enumerate(["a", "b", "c"])})
    df2 = pd.DataFrame({c: np.random.random(N) for i, c in enumerate(["a", "b", "c"])})
    files = []
    for d in ["test_data1", "test_data2"]:
        dn = os.path.join(fn, d)
        if not os.path.exists(dn):
            os.mkdir(dn)
        files.append(os.path.join(dn, "data1.parq"))

    fastparquet.write(files[0], df1)
    fastparquet.write(files[1], df2)

    df = dd.read_parquet(files, engine="fastparquet")
    assert "dir0" in df.columns
    out = df.compute()
    assert "dir0" in out
    assert (np.unique(out.dir0) == ["test_data1", "test_data2"]).all()


def test_parquet_select_cats(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame(
        {
            "categories": pd.Series(
                np.random.choice(["a", "b", "c", "d", "e", "f"], size=100),
                dtype="category",
            ),
            "ints": pd.Series(list(range(0, 100)), dtype="int"),
            "floats": pd.Series(list(range(0, 100)), dtype="float"),
        }
    )

    ddf = dd.from_pandas(df, 1)
    ddf.to_parquet(fn, **engine)
    rddf = dd.read_parquet(fn, columns=["ints"], **engine)
    assert list(rddf.columns) == ["ints"]
    rddf = dd.read_parquet(fn, **engine)
    assert list(rddf.columns) == list(df)


def test_columns_name(tmpdir, engine):
    tmp_path = str(tmpdir)
    df = pd.DataFrame({"A": [1, 2]}, index=pd.Index(["a", "b"], name="idx"))
    df.columns.name = "cols"
    ddf = dd.from_pandas(df, 2)

    ddf.to_parquet(tmp_path, **engine)
    result = dd.read_parquet(tmp_path, index=["idx"], **engine)
    assert_eq(result, df)


def check_compression(filename, compression, engine=None):
    if engine == "fastparquet":
        pf = fastparquet.ParquetFile(filename)
        md = pf.fmd.row_groups[0].columns[0].meta_data
        if compression is None:
            assert md.total_compressed_size == md.total_uncompressed_size
        else:
            assert md.total_compressed_size != md.total_uncompressed_size
    else:  # pyarrow
        metadata = pa.parquet.read_metadata(os.path.join(filename, "_metadata"))
        names = metadata.schema.names
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            for j in range(len(names)):
                column = row_group.column(j)
                if compression is None:
                    assert (
                        column.total_compressed_size == column.total_uncompressed_size
                    )
                else:
                    compress_expect = compression
                    if compression == "default":
                        compress_expect = "snappy"
                    assert compress_expect.lower() == column.compression.lower()
                    assert (
                        column.total_compressed_size != column.total_uncompressed_size
                    )


@pytest.mark.parametrize("compression,", [None, "gzip", "snappy"])
def test_writing_parquet_with_compression(tmpdir, compression, engine):
    fn = str(tmpdir)

    df = pd.DataFrame({"x": ["a", "b", "c"] * 10, "y": [1, 2, 3] * 10})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    ddf.to_parquet(fn, compression=compression, write_metadata_file=True, **engine)
    out = dd.read_parquet(fn, calculate_divisions=True, **engine)
    assert_eq(out, ddf)
    check_compression(fn, compression, **engine)


@pytest.mark.parametrize("compression,", [None, "gzip", "snappy"])
def test_writing_parquet_with_partition_on_and_compression(tmpdir, compression, engine):
    fn = str(tmpdir)

    df = pd.DataFrame({"x": ["a", "b", "c"] * 10, "y": [1, 2, 3] * 10})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    ddf.to_parquet(
        fn,
        compression=compression,
        partition_on=["x"],
        write_metadata_file=True,
        **engine,
    )
    check_compression(fn, compression, **engine)


@pytest.fixture(
    params=[
        # fastparquet 0.1.3
        {
            "columns": [
                {
                    "metadata": None,
                    "name": "idx",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
                {
                    "metadata": None,
                    "name": "A",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
            ],
            "index_columns": ["idx"],
            "pandas_version": "0.21.0",
        },
        # pyarrow 0.7.1
        {
            "columns": [
                {
                    "metadata": None,
                    "name": "A",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
                {
                    "metadata": None,
                    "name": "idx",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
            ],
            "index_columns": ["idx"],
            "pandas_version": "0.21.0",
        },
        # pyarrow 0.8.0
        {
            "column_indexes": [
                {
                    "field_name": None,
                    "metadata": {"encoding": "UTF-8"},
                    "name": None,
                    "numpy_type": "object",
                    "pandas_type": "unicode",
                }
            ],
            "columns": [
                {
                    "field_name": "A",
                    "metadata": None,
                    "name": "A",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
                {
                    "field_name": "__index_level_0__",
                    "metadata": None,
                    "name": "idx",
                    "numpy_type": "int64",
                    "pandas_type": "int64",
                },
            ],
            "index_columns": ["__index_level_0__"],
            "pandas_version": "0.21.0",
        },
        # TODO: fastparquet update
    ]
)
def pandas_metadata(request):
    return request.param


def test_parse_pandas_metadata(pandas_metadata):
    index_names, column_names, mapping, column_index_names = _parse_pandas_metadata(
        pandas_metadata
    )
    assert index_names == ["idx"]
    assert column_names == ["A"]
    assert column_index_names == [None]

    # for new pyarrow
    if pandas_metadata["index_columns"] == ["__index_level_0__"]:
        assert mapping == {"__index_level_0__": "idx", "A": "A"}
    else:
        assert mapping == {"idx": "idx", "A": "A"}

    assert isinstance(mapping, dict)


def test_parse_pandas_metadata_null_index():
    # pyarrow 0.7.1 None for index
    e_index_names = [None]
    e_column_names = ["x"]
    e_mapping = {"__index_level_0__": None, "x": "x"}
    e_column_index_names = [None]

    md = {
        "columns": [
            {
                "metadata": None,
                "name": "x",
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
            {
                "metadata": None,
                "name": "__index_level_0__",
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
        ],
        "index_columns": ["__index_level_0__"],
        "pandas_version": "0.21.0",
    }
    index_names, column_names, mapping, column_index_names = _parse_pandas_metadata(md)
    assert index_names == e_index_names
    assert column_names == e_column_names
    assert mapping == e_mapping
    assert column_index_names == e_column_index_names

    # pyarrow 0.8.0 None for index
    md = {
        "column_indexes": [
            {
                "field_name": None,
                "metadata": {"encoding": "UTF-8"},
                "name": None,
                "numpy_type": "object",
                "pandas_type": "unicode",
            }
        ],
        "columns": [
            {
                "field_name": "x",
                "metadata": None,
                "name": "x",
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
            {
                "field_name": "__index_level_0__",
                "metadata": None,
                "name": None,
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
        ],
        "index_columns": ["__index_level_0__"],
        "pandas_version": "0.21.0",
    }
    index_names, column_names, mapping, column_index_names = _parse_pandas_metadata(md)
    assert index_names == e_index_names
    assert column_names == e_column_names
    assert mapping == e_mapping
    assert column_index_names == e_column_index_names


@PYARROW_MARK
def test_read_no_metadata(tmpdir, engine):
    # use pyarrow.parquet to create a parquet file without
    # pandas metadata
    tmp = str(tmpdir) + "table.parq"

    table = pa.Table.from_arrays(
        [pa.array([1, 2, 3]), pa.array([3, 4, 5])], names=["A", "B"]
    )
    pq.write_table(table, tmp)
    result = dd.read_parquet(tmp, **engine)
    expected = pd.DataFrame({"A": [1, 2, 3], "B": [3, 4, 5]})
    assert_eq(result, expected)


def test_parse_pandas_metadata_duplicate_index_columns():
    md = {
        "column_indexes": [
            {
                "field_name": None,
                "metadata": {"encoding": "UTF-8"},
                "name": None,
                "numpy_type": "object",
                "pandas_type": "unicode",
            }
        ],
        "columns": [
            {
                "field_name": "A",
                "metadata": None,
                "name": "A",
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
            {
                "field_name": "__index_level_0__",
                "metadata": None,
                "name": "A",
                "numpy_type": "object",
                "pandas_type": "unicode",
            },
        ],
        "index_columns": ["__index_level_0__"],
        "pandas_version": "0.21.0",
    }
    (
        index_names,
        column_names,
        storage_name_mapping,
        column_index_names,
    ) = _parse_pandas_metadata(md)
    assert index_names == ["A"]
    assert column_names == ["A"]
    assert storage_name_mapping == {"__index_level_0__": "A", "A": "A"}
    assert column_index_names == [None]


def test_parse_pandas_metadata_column_with_index_name():
    md = {
        "column_indexes": [
            {
                "field_name": None,
                "metadata": {"encoding": "UTF-8"},
                "name": None,
                "numpy_type": "object",
                "pandas_type": "unicode",
            }
        ],
        "columns": [
            {
                "field_name": "A",
                "metadata": None,
                "name": "A",
                "numpy_type": "int64",
                "pandas_type": "int64",
            },
            {
                "field_name": "__index_level_0__",
                "metadata": None,
                "name": "A",
                "numpy_type": "object",
                "pandas_type": "unicode",
            },
        ],
        "index_columns": ["__index_level_0__"],
        "pandas_version": "0.21.0",
    }
    (
        index_names,
        column_names,
        storage_name_mapping,
        column_index_names,
    ) = _parse_pandas_metadata(md)
    assert index_names == ["A"]
    assert column_names == ["A"]
    assert storage_name_mapping == {"__index_level_0__": "A", "A": "A"}
    assert column_index_names == [None]


def test_writing_parquet_with_kwargs(tmpdir, engine):
    fn = str(tmpdir)
    path1 = os.path.join(fn, "normal")
    path2 = os.path.join(fn, "partitioned")

    df = pd.DataFrame(
        {
            "a": np.random.choice(["A", "B", "C"], size=100),
            "b": np.random.random(size=100),
            "c": np.random.randint(1, 5, size=100),
        }
    )
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    if engine == {}:  # pyarrow
        write_kwargs = {
            "compression": "snappy",
            "coerce_timestamps": None,
            "use_dictionary": True,
        }
    else:  # fastparquet
        write_kwargs = {
            "compression": "snappy",
            "times": "int64",
            "fixed_text": None,
            **engine,
        }

    ddf.to_parquet(path1, **write_kwargs)
    out = dd.read_parquet(path1, calculate_divisions=True, **engine)
    assert_eq(out, ddf, check_index=(engine == {}))

    # Avoid race condition in pyarrow 0.8.0 on writing partitioned datasets
    with dask.config.set(scheduler="sync"):
        ddf.to_parquet(path2, partition_on=["a"], **write_kwargs)
    out = dd.read_parquet(path2, **engine).compute()
    for val in df.a.unique():
        assert set(df.b[df.a == val]) == set(out.b[out.a == val])


def test_writing_parquet_with_unknown_kwargs(tmpdir, engine):
    fn = str(tmpdir)

    with pytest.raises(TypeError):
        ddf.to_parquet(fn, unknown_key="unknown_value", **engine)


def test_to_parquet_with_get(tmpdir, engine):
    from dask.multiprocessing import get as mp_get

    tmpdir = str(tmpdir)

    flag = [False]

    def my_get(*args, **kwargs):
        flag[0] = True
        return mp_get(*args, **kwargs)

    df = pd.DataFrame({"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)

    ddf.to_parquet(tmpdir, compute_kwargs={"scheduler": my_get}, **engine)
    assert flag[0]

    result = dd.read_parquet(os.path.join(tmpdir, "*"), **engine)
    assert_eq(result, df, check_index=False)


def test_select_partitioned_column(tmpdir, engine):
    fn = str(tmpdir)
    size = 20
    d = {
        "signal1": np.random.normal(0, 0.3, size=size).cumsum() + 50,
        "fake_categorical1": np.random.choice(["A", "B", "C"], size=size),
        "fake_categorical2": np.random.choice(["D", "E", "F"], size=size),
    }
    df = dd.from_pandas(pd.DataFrame(d), 2)
    df.to_parquet(
        fn,
        compression="snappy",
        write_index=False,
        partition_on=["fake_categorical1", "fake_categorical2"],
        **engine,
    )

    df_partitioned = dd.read_parquet(fn, **engine)
    df_partitioned[df_partitioned.fake_categorical1 == "A"].compute()


@FASTPARQUET_MARK
def test_with_tz(tmpdir):
    with warnings.catch_warnings():
        # fastparquet-442
        warnings.simplefilter("ignore", FutureWarning)  # pandas 0.25
        fn = str(tmpdir)
        df = pd.DataFrame([[0]], columns=["a"], dtype="datetime64[ns, UTC]")
        df = dd.from_pandas(df, 1)
        df.to_parquet(fn, engine="fastparquet")
        df2 = dd.read_parquet(fn, engine="fastparquet")
        assert_eq(df, df2, check_divisions=False, check_index=False)


@PYARROW_MARK
def test_arrow_partitioning(tmpdir):
    # Issue #3518
    path = str(tmpdir)
    data = {
        "p": np.repeat(np.arange(3), 2).astype(np.int8),
        "b": np.repeat(-1, 6).astype(np.int16),
        "c": np.repeat(-2, 6).astype(np.float32),
        "d": np.repeat(-3, 6).astype(np.float64),
    }
    pdf = pd.DataFrame(data)
    ddf = dd.from_pandas(pdf, npartitions=2)
    ddf.to_parquet(path, write_index=False, partition_on="p")

    ddf = dd.read_parquet(path, index=False)

    ddf.astype({"b": np.float32}).compute()


def test_informative_error_messages():
    with (
        pytest.warns(FutureWarning, match="The `engine=` parameter is deprecated"),
        pytest.raises(ValueError, match=r"Unsupported engine: \"foo\""),
    ):
        dd.read_parquet("foo", engine="foo")


def test_append_cat_fp(tmpdir, engine):
    path = str(tmpdir)
    # https://github.com/dask/dask/issues/4120
    df = pd.DataFrame({"x": ["a", "a", "b", "a", "b"]})
    df["x"] = df["x"].astype("category")
    ddf = dd.from_pandas(df, npartitions=1)

    dd.to_parquet(ddf, path, **engine)
    dd.to_parquet(ddf, path, append=True, ignore_divisions=True, **engine)

    d = dd.read_parquet(path, **engine).compute()
    assert d["x"].tolist() == ["a", "a", "b", "a", "b"] * 2


@PYARROW_MARK
@pytest.mark.parametrize(
    "df",
    [
        pd.DataFrame({"x": [4, 5, 6, 1, 2, 3]}),
        pd.DataFrame({"x": ["c", "a", "b"]}),
        pd.DataFrame({"x": ["cc", "a", "bbb"]}),
        pytest.param(pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])})),
        pytest.param(pd.DataFrame({"x": pd.Categorical([1, 2, 1])})),
        pd.DataFrame({"x": list(map(pd.Timestamp, [3000000, 2000000, 1000000]))}),  # ms
        pd.DataFrame({"x": list(map(pd.Timestamp, [3000, 2000, 1000]))}),  # us
        pd.DataFrame({"x": [3000, 2000, 1000]}).astype("M8[ns]"),
        # pd.DataFrame({'x': [3, 2, 1]}).astype('M8[ns]'), # Casting errors
        pytest.param(
            pd.DataFrame({"x": [3, 2, 1]}).astype("M8[us]"),
            marks=pytest.mark.xfail(
                PANDAS_GE_200 and pyarrow_version < parse_version("13.0.0.dev"),
                reason="https://github.com/apache/arrow/issues/15079",
            ),
        ),
        pytest.param(
            pd.DataFrame({"x": [3, 2, 1]}).astype("M8[ms]"),
            marks=pytest.mark.xfail(
                PANDAS_GE_200 and pyarrow_version < parse_version("13.0.0.dev"),
                reason="https://github.com/apache/arrow/issues/15079",
            ),
        ),
        pd.DataFrame({"x": [3, 2, 1]}).astype("uint16"),
        pd.DataFrame({"x": [3, 2, 1]}).astype("float32"),
        pd.DataFrame({"x": [3, 1, 2]}, index=[3, 2, 1]),
        pd.DataFrame(
            {"x": [4, 5, 6, 1, 2, 3]}, index=pd.Index([1, 2, 3, 4, 5, 6], name="foo")
        ),
        pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]}),
        pd.DataFrame({"x": [1, 2, 3], "y": [3, 2, 1]}, columns=["y", "x"]),
        pd.DataFrame({"0": [3, 2, 1]}),
        pd.DataFrame({"x": [3, 2, None]}),
        pd.DataFrame({"-": [3.0, 2.0, None]}),
        pd.DataFrame({".": [3.0, 2.0, None]}),
        pd.DataFrame({" ": [3.0, 2.0, None]}),
    ],
)
def test_roundtrip_arrow(tmpdir, df):
    # Index will be given a name when preserved as index
    tmp_path = str(tmpdir)
    if not df.index.name:
        df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    dd.to_parquet(ddf, tmp_path, write_index=True)
    ddf2 = dd.read_parquet(tmp_path, calculate_divisions=True)
    assert_eq(ddf, ddf2)


def test_datasets_timeseries(tmpdir, engine):
    tmp_path = str(tmpdir)
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-01-10", freq="1d"
    ).persist()
    df.to_parquet(tmp_path, **engine)

    df2 = dd.read_parquet(tmp_path, calculate_divisions=True, **engine)
    assert_eq(df, df2)


def test_pathlib_path(tmpdir, engine):
    import pathlib

    df = pd.DataFrame({"x": [4, 5, 6, 1, 2, 3]})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    path = pathlib.Path(str(tmpdir))
    ddf.to_parquet(path, **engine)
    ddf2 = dd.read_parquet(path, calculate_divisions=True, **engine)
    assert_eq(ddf, ddf2)


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_categories_large(tmpdir, engine):
    # Issue #5112
    fn = str(tmpdir.join("parquet_int16.parq"))
    numbers = np.random.randint(0, 800000, size=1000000)
    df = pd.DataFrame(numbers.T, columns=["name"])
    df.name = df.name.astype("category")

    df.to_parquet(fn, engine="fastparquet", compression="uncompressed")
    ddf = dd.read_parquet(fn, categories={"name": 80000}, **engine)

    assert_eq(sorted(df.name.cat.categories), sorted(ddf.compute().name.cat.categories))


def test_read_glob_no_meta(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, **write_engine)

    ddf2 = dd.read_parquet(
        os.path.join(tmp_path, "*.parquet"),
        calculate_divisions=False,
        **read_engine,
    )
    assert_eq(ddf, ddf2, check_divisions=False)


def test_read_glob_yes_meta(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, write_metadata_file=True, **write_engine)
    paths = glob.glob(os.path.join(tmp_path, "*.parquet"))
    paths.append(os.path.join(tmp_path, "_metadata"))
    ddf2 = dd.read_parquet(paths, calculate_divisions=False, **read_engine)
    assert_eq(ddf, ddf2, check_divisions=False)


@pytest.mark.parametrize("divisions", [True, False])
@pytest.mark.parametrize("remove_common", [True, False])
def test_read_dir_nometa(tmpdir, write_engine, read_engine, divisions, remove_common):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, write_metadata_file=True, **write_engine)
    if os.path.exists(os.path.join(tmp_path, "_metadata")):
        os.unlink(os.path.join(tmp_path, "_metadata"))
    files = os.listdir(tmp_path)
    assert "_metadata" not in files

    if remove_common and os.path.exists(os.path.join(tmp_path, "_common_metadata")):
        os.unlink(os.path.join(tmp_path, "_common_metadata"))

    ddf2 = dd.read_parquet(tmp_path, calculate_divisions=divisions, **read_engine)
    assert_eq(ddf, ddf2, check_divisions=divisions)


def test_statistics_nometa(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, write_metadata_file=False, **write_engine)

    ddf2 = dd.read_parquet(tmp_path, calculate_divisions=True, **read_engine)
    assert_eq(ddf, ddf2)


@pytest.mark.parametrize("schema", ["infer", None])
def test_timeseries_nulls_in_schema(tmpdir, engine, schema):
    # GH#5608: relative path failing _metadata/_common_metadata detection.
    tmp_path = str(tmpdir.mkdir("files"))
    tmp_path = os.path.join(tmp_path, "../", "files")

    ddf2 = (
        dask.datasets.timeseries(start="2000-01-01", end="2000-01-03", freq="1h")
        .reset_index()
        .map_partitions(lambda x: x.loc[:5])
    )
    ddf2 = ddf2.set_index("x").reset_index().persist()
    ddf2.name = ddf2.name.where(ddf2.timestamp == "2000-01-01", None)

    # Note: `append_row_groups` will fail with pyarrow>0.17.1 for _metadata write
    ddf2.to_parquet(tmp_path, write_metadata_file=False, schema=schema, **engine)
    ddf_read = dd.read_parquet(tmp_path, **engine)

    assert_eq(ddf_read, ddf2, check_divisions=False, check_index=False)


def test_graph_size_pyarrow(tmpdir, engine):
    import pickle

    fn = str(tmpdir)

    ddf1 = dask.datasets.timeseries(
        start="2000-01-01", end="2000-01-02", freq="60s", partition_freq="1h"
    )

    ddf1.to_parquet(fn, **engine)
    ddf2 = dd.read_parquet(fn, **engine)

    assert len(pickle.dumps(ddf2.__dask_graph__())) < 25000


@pytest.mark.parametrize("preserve_index", [True, False])
@pytest.mark.parametrize("index", [None, np.random.permutation(2000)])
def test_getitem_optimization(tmpdir, engine, preserve_index, index):
    tmp_path_rd = str(tmpdir.mkdir("read"))
    tmp_path_wt = str(tmpdir.mkdir("write"))
    df = pd.DataFrame(
        {"A": [1, 2] * 1000, "B": [3, 4] * 1000, "C": [5, 6] * 1000}, index=index
    )
    df.index.name = "my_index"
    ddf = dd.from_pandas(df, 2, sort=False)

    ddf.to_parquet(tmp_path_rd, write_index=preserve_index, **engine)
    ddf = dd.read_parquet(tmp_path_rd, **engine)["B"]

    # Write ddf back to disk to check that the round trip
    # preserves the getitem optimization
    out = ddf.to_frame().to_parquet(tmp_path_wt, compute=False, **engine)
    dsk = optimize_dataframe_getitem(out.dask, keys=[out.key])

    subgraph_rd = hlg_layer(dsk, "read-parquet")
    assert isinstance(subgraph_rd, DataFrameIOLayer)
    assert subgraph_rd.columns == ["B"]
    assert next(iter(subgraph_rd.dsk.values()))[0].columns == ["B"]

    subgraph_wt = hlg_layer(dsk, "to-parquet")
    assert isinstance(subgraph_wt, Blockwise)

    assert_eq(ddf.compute(optimize_graph=False), ddf.compute())


def test_getitem_optimization_empty(tmpdir, engine):
    df = pd.DataFrame({"A": [1] * 100, "B": [2] * 100, "C": [3] * 100, "D": [4] * 100})
    ddf = dd.from_pandas(df, 2, sort=False)
    fn = os.path.join(str(tmpdir))
    ddf.to_parquet(fn, **engine)

    ddf2 = dd.read_parquet(fn, **engine)[[]]
    dsk = optimize_dataframe_getitem(ddf2.dask, keys=[ddf2._name])

    subgraph = next(l for l in dsk.layers.values() if isinstance(l, DataFrameIOLayer))
    assert subgraph.columns == []

    assert_eq(ddf2, ddf[[]])


def test_getitem_optimization_multi(tmpdir, engine):
    df = pd.DataFrame({"A": [1] * 100, "B": [2] * 100, "C": [3] * 100, "D": [4] * 100})
    ddf = dd.from_pandas(df, 2)
    fn = os.path.join(str(tmpdir))
    ddf.to_parquet(fn, **engine)

    a = dd.read_parquet(fn, **engine)["B"]
    b = dd.read_parquet(fn, **engine)[["C"]]
    c = dd.read_parquet(fn, **engine)[["C", "A"]]

    a1, a2, a3 = dask.compute(a, b, c)
    b1, b2, b3 = dask.compute(a, b, c, optimize_graph=False)

    assert_eq(a1, b1)
    assert_eq(a2, b2)
    assert_eq(a3, b3)


def test_getitem_optimization_after_filter(tmpdir, engine):
    df = pd.DataFrame({"a": [1, 2, 3] * 5, "b": range(15), "c": range(15)})
    dd.from_pandas(df, npartitions=3).to_parquet(tmpdir, **engine)
    ddf = dd.read_parquet(tmpdir, **engine)

    df2 = df[df["b"] > 10][["a"]]
    ddf2 = ddf[ddf["b"] > 10][["a"]]

    dsk = optimize_dataframe_getitem(ddf2.dask, keys=[ddf2._name])
    subgraph_rd = hlg_layer(dsk, "read-parquet")
    assert isinstance(subgraph_rd, DataFrameIOLayer)
    assert set(subgraph_rd.columns) == {"a", "b"}

    assert_eq(df2, ddf2)


def test_getitem_optimization_after_filter_complex(tmpdir, engine):
    df = pd.DataFrame({"a": [1, 2, 3] * 5, "b": range(15), "c": range(15)})
    dd.from_pandas(df, npartitions=3).to_parquet(tmpdir, **engine)
    ddf = dd.read_parquet(tmpdir, **engine)

    df2 = df[["b"]]
    df2 = df2.assign(d=1)
    df2 = df[df2["d"] == 1][["b"]]

    ddf2 = ddf[["b"]]
    ddf2 = ddf2.assign(d=1)
    ddf2 = ddf[ddf2["d"] == 1][["b"]]

    dsk = optimize_dataframe_getitem(ddf2.dask, keys=[ddf2._name])
    subgraph_rd = hlg_layer(dsk, "read-parquet")
    assert isinstance(subgraph_rd, DataFrameIOLayer)
    assert set(subgraph_rd.columns) == {"b"}

    assert_eq(df2, ddf2)


def test_layer_creation_info(tmpdir, engine):
    df = pd.DataFrame({"a": range(10), "b": ["cat", "dog"] * 5})
    dd.from_pandas(df, npartitions=1).to_parquet(tmpdir, partition_on=["b"], **engine)

    # Apply filters directly in dd.read_parquet
    filters = [("b", "==", "cat")]
    ddf1 = dd.read_parquet(tmpdir, filters=filters, **engine)
    assert "dog" not in ddf1["b"].compute()

    # Results will not match if we use dd.read_parquet
    # without filters
    ddf2 = dd.read_parquet(tmpdir, **engine)
    with pytest.raises(AssertionError):
        assert_eq(ddf1, ddf2)

    # However, we can use `creation_info` to regenerate
    # the same collection with `filters` defined
    info = ddf2.dask.layers[ddf2._name].creation_info
    del info["kwargs"]["engine"]
    info["kwargs"].update(engine)
    info["kwargs"]["filters"] = filters

    ddf3 = info["func"](*info["args"], **info["kwargs"])
    assert_eq(ddf1, ddf3)


def test_blockwise_parquet_annotations(tmpdir, engine):
    df = pd.DataFrame({"a": np.arange(40, dtype=np.int32)})
    expect = dd.from_pandas(df, npartitions=2)
    expect.to_parquet(str(tmpdir), **engine)

    with dask.annotate(foo="bar"):
        ddf = dd.read_parquet(str(tmpdir), **engine)

    # `ddf` should now have ONE Blockwise layer
    layers = ddf.__dask_graph__().layers
    assert len(layers) == 1
    layer = next(iter(layers.values()))
    assert isinstance(layer, DataFrameIOLayer)
    assert layer.annotations == {"foo": "bar"}


def test_optimize_blockwise_parquet(tmpdir, engine):
    size = 40
    npartitions = 2
    tmp = str(tmpdir)
    df = pd.DataFrame({"a": np.arange(size, dtype=np.int32)})
    expect = dd.from_pandas(df, npartitions=npartitions)
    expect.to_parquet(tmp, **engine)
    ddf = dd.read_parquet(tmp, calculate_divisions=True, **engine)

    # `ddf` should now have ONE Blockwise layer
    layers = ddf.__dask_graph__().layers
    assert len(layers) == 1
    assert isinstance(list(layers.values())[0], Blockwise)

    # Check single-layer result
    assert_eq(ddf, expect)

    # Increment by 1
    ddf += 1
    expect += 1

    # Increment by 10
    ddf += 10
    expect += 10

    # `ddf` should now have THREE Blockwise layers
    layers = ddf.__dask_graph__().layers
    assert len(layers) == 3
    assert all(isinstance(layer, Blockwise) for layer in layers.values())

    # Check that `optimize_blockwise` fuses all three
    # `Blockwise` layers together into a singe `Blockwise` layer
    keys = [(ddf._name, i) for i in range(npartitions)]
    graph = optimize_blockwise(ddf.__dask_graph__(), keys)
    layers = graph.layers
    name = list(layers.keys())[0]
    assert len(layers) == 1
    assert isinstance(layers[name], Blockwise)

    # Check final result
    assert_eq(ddf, expect)


@PYARROW_MARK
def test_split_row_groups(tmpdir, engine):
    """Test split_row_groups read_parquet kwarg"""
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"i32": np.arange(800, dtype=np.int32), "f": np.arange(800, dtype=np.float64)}
    )
    df.index.name = "index"

    half = len(df) // 2
    dd.from_pandas(df.iloc[:half], npartitions=2).to_parquet(tmp, row_group_size=100)

    ddf3 = dd.read_parquet(tmp, split_row_groups=True, **engine)
    assert ddf3.npartitions == 4

    ddf3 = dd.read_parquet(
        tmp, calculate_divisions=True, split_row_groups=False, **engine
    )
    assert ddf3.npartitions == 2

    dd.from_pandas(df.iloc[half:], npartitions=2).to_parquet(
        tmp, append=True, row_group_size=50
    )

    ddf3 = dd.read_parquet(
        tmp,
        calculate_divisions=True,
        split_row_groups=True,
        **engine,
    )
    assert ddf3.npartitions == 12

    ddf3 = dd.read_parquet(
        tmp, calculate_divisions=True, split_row_groups=False, **engine
    )
    assert ddf3.npartitions == 4


@PYARROW_MARK
@pytest.mark.parametrize("split_row_groups", [1, 12])
@pytest.mark.parametrize("calculate_divisions", [True, False])
def test_split_row_groups_int(tmpdir, split_row_groups, calculate_divisions, engine):
    tmp = str(tmpdir)
    row_group_size = 10
    npartitions = 4
    half_size = 400
    df = pd.DataFrame(
        {
            "i32": np.arange(2 * half_size, dtype=np.int32),
            "f": np.arange(2 * half_size, dtype=np.float64),
        }
    )
    half = len(df) // 2

    dd.from_pandas(df.iloc[:half], npartitions=npartitions).to_parquet(
        tmp, row_group_size=row_group_size
    )
    dd.from_pandas(df.iloc[half:], npartitions=npartitions).to_parquet(
        tmp, append=True, row_group_size=row_group_size
    )

    ddf2 = dd.read_parquet(
        tmp,
        split_row_groups=split_row_groups,
        calculate_divisions=calculate_divisions,
        **engine,
    )
    expected_rg_cout = int(half_size / row_group_size)
    assert ddf2.npartitions == 2 * math.ceil(expected_rg_cout / split_row_groups)


@PYARROW_MARK
@pytest.mark.parametrize("split_row_groups", [8, 25])
def test_split_row_groups_int_aggregate_files(tmpdir, engine, split_row_groups):
    # Use pyarrow to write a multi-file dataset with
    # multiple row-groups per file
    row_group_size = 10
    size = 800
    df = pd.DataFrame(
        {
            "i32": np.arange(size, dtype=np.int32),
            "f": np.arange(size, dtype=np.float64),
        }
    )
    dd.from_pandas(df, npartitions=4).to_parquet(
        str(tmpdir), row_group_size=row_group_size, write_index=False
    )

    # Read back with both `split_row_groups>1` and
    # `aggregate_files=True`
    ddf2 = dd.read_parquet(
        str(tmpdir),
        split_row_groups=split_row_groups,
        aggregate_files=True,
        **engine,
    )

    # Check that we are aggregating files as expected
    npartitions_expected = math.ceil((size / row_group_size) / split_row_groups)
    assert ddf2.npartitions == npartitions_expected
    assert len(ddf2) == size
    assert_eq(df, ddf2, check_index=False)


@PYARROW_MARK
@pytest.mark.parametrize(
    "filters,op,length",
    [
        pytest.param(
            [("c", "!=", "a")],
            lambda x: x[x["c"] != "a"],
            13,
            marks=pytest.mark.xfail_with_pyarrow_strings,
        ),
        ([("c", "==", "a")], lambda x: x[x["c"] == "a"], 2),
    ],
)
@pytest.mark.parametrize("split_row_groups", [True, False])
def test_filter_nulls(tmpdir, filters, op, length, split_row_groups, engine):
    if engine == {} and parse_version(pa.__version__) < parse_version("8.0.0"):
        # See: https://issues.apache.org/jira/browse/ARROW-15312
        pytest.skip("pyarrow>=8.0.0 needed for correct null filtering")
    path = tmpdir.join("test.parquet")
    df = pd.DataFrame(
        {
            "a": [1, None] * 5 + [None] * 5,
            "b": np.arange(14).tolist() + [None],
            "c": ["a", None] * 2 + [None] * 11,
        }
    )
    df.to_parquet(path, row_group_size=10)

    result = dd.read_parquet(
        path,
        filters=filters,
        split_row_groups=split_row_groups,
        **engine,
    )
    assert len(op(result)) == length
    assert_eq(op(result), op(df), check_index=False)


@PYARROW_MARK
@pytest.mark.parametrize("split_row_groups", [True, False])
def test_filter_isna(tmpdir, split_row_groups):
    if parse_version(pa.__version__) < parse_version("8.0.0"):
        # See: https://issues.apache.org/jira/browse/ARROW-15312
        pytest.skip("pyarrow>=8.0.0 needed for correct null filtering")
    path = tmpdir.join("test.parquet")
    pd.DataFrame({"a": [1, None] * 5 + [None] * 5}).to_parquet(path, row_group_size=10)

    result_isna = dd.read_parquet(
        path,
        filters=[("a", "is", np.nan)],
        split_row_groups=split_row_groups,
    )
    assert len(result_isna) == 10
    assert all(result_isna["a"].compute().isna())

    result_notna = dd.read_parquet(
        path,
        filters=[("a", "is not", np.nan)],
        split_row_groups=split_row_groups,
    )
    assert result_notna["a"].compute().tolist() == [1] * 5


@PYARROW_MARK
def test_split_row_groups_filter(tmpdir, engine):
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"i32": np.arange(800, dtype=np.int32), "f": np.arange(800, dtype=np.float64)}
    )
    df.index.name = "index"
    search_val = 600
    filters = [("f", "==", search_val)]

    dd.from_pandas(df, npartitions=4).to_parquet(tmp, append=True, row_group_size=50)

    ddf2 = dd.read_parquet(tmp, **engine)
    ddf3 = dd.read_parquet(
        tmp,
        calculate_divisions=True,
        split_row_groups=True,
        filters=filters,
        **engine,
    )

    assert (ddf3["i32"] == search_val).any().compute()
    assert_eq(
        ddf2[ddf2["i32"] == search_val].compute(),
        ddf3[ddf3["i32"] == search_val].compute(),
    )


def test_optimize_getitem_and_nonblockwise(tmpdir, engine):
    path = os.path.join(tmpdir, "path.parquet")
    df = pd.DataFrame(
        {"a": [3, 4, 2], "b": [1, 2, 4], "c": [5, 4, 2], "d": [1, 2, 3]},
        index=["a", "b", "c"],
    )
    df.to_parquet(path, **engine)

    df2 = dd.read_parquet(path, **engine)
    df2[["a", "b"]].rolling(3).max().compute()


def test_optimize_and_not(tmpdir, engine):
    path = os.path.join(tmpdir, "path.parquet")
    df = pd.DataFrame(
        {"a": [3, 4, 2], "b": [1, 2, 4], "c": [5, 4, 2], "d": [1, 2, 3]},
        index=["a", "b", "c"],
    )
    df.to_parquet(path, **engine)

    df2 = dd.read_parquet(path, **engine)
    df2a = df2["a"].groupby(df2["c"]).first().to_delayed()
    df2b = df2["b"].groupby(df2["c"]).first().to_delayed()
    df2c = df2[["a", "b"]].rolling(2).max().to_delayed()
    df2d = df2.rolling(2).max().to_delayed()
    (result,) = dask.compute(df2a + df2b + df2c + df2d)

    expected = [
        dask.compute(df2a)[0][0],
        dask.compute(df2b)[0][0],
        dask.compute(df2c)[0][0],
        dask.compute(df2d)[0][0],
    ]
    for a, b in zip(result, expected):
        assert_eq(a, b)


def test_split_adaptive_empty(tmpdir, write_engine, read_engine):
    df = pd.DataFrame({"a": pd.Series(dtype="int"), "b": pd.Series(dtype="float")})
    ddf1 = dd.from_pandas(df, npartitions=1)
    ddf1.to_parquet(tmpdir, write_metadata_file=True, **write_engine)
    ddf2 = dd.read_parquet(tmpdir, split_row_groups="adaptive", **read_engine)
    assert_eq(ddf1, ddf2, check_index=False)


@PYARROW_MARK
@pytest.mark.parametrize("metadata", [True, False])
@pytest.mark.parametrize("partition_on", [None, "a"])
@pytest.mark.parametrize("blocksize", [4096, "1MiB"])
def test_split_adaptive_files(
    tmpdir, blocksize, partition_on, write_engine, read_engine, metadata
):
    df_size = 100
    df1 = pd.DataFrame(
        {
            "a": np.random.choice(["apple", "banana", "carrot"], size=df_size),
            "b": np.random.random(size=df_size),
            "c": np.random.randint(1, 5, size=df_size),
        }
    )
    ddf1 = dd.from_pandas(df1, npartitions=9)

    ddf1.to_parquet(
        str(tmpdir),
        partition_on=partition_on,
        write_metadata_file=metadata,
        write_index=False,
        **write_engine,
    )

    aggregate_files = partition_on if partition_on else True
    if isinstance(aggregate_files, str):
        with pytest.warns(FutureWarning, match="Behavior may change"):
            ddf2 = dd.read_parquet(
                str(tmpdir),
                blocksize=blocksize,
                split_row_groups="adaptive",
                aggregate_files=aggregate_files,
                **read_engine,
            )
    else:
        ddf2 = dd.read_parquet(
            str(tmpdir),
            blocksize=blocksize,
            split_row_groups="adaptive",
            aggregate_files=aggregate_files,
            **read_engine,
        )

    # Check that files where aggregated as expected
    if blocksize == 4096:
        assert ddf2.npartitions < ddf1.npartitions
    elif blocksize == "1MiB":
        if partition_on:
            assert ddf2.npartitions == 3
        else:
            assert ddf2.npartitions == 1

    # Check that the final data is correct
    if partition_on:
        df2 = ddf2.compute().sort_values(["b", "c"])
        df1 = df1.sort_values(["b", "c"])
        assert_eq(df1[["b", "c"]], df2[["b", "c"]], check_index=False)
    else:
        assert_eq(ddf1, ddf2, check_divisions=False, check_index=False)


@pytest.mark.parametrize("aggregate_files", ["a", "b"])
def test_split_adaptive_aggregate_files(
    tmpdir, write_engine, read_engine, aggregate_files
):
    blocksize = "1MiB"
    partition_on = ["a", "b"]
    df_size = 100
    df1 = pd.DataFrame(
        {
            "a": np.random.choice(["apple", "banana", "carrot"], size=df_size),
            "b": np.random.choice(["small", "large"], size=df_size),
            "c": np.random.random(size=df_size),
            "d": np.random.randint(1, 100, size=df_size),
        }
    )
    ddf1 = dd.from_pandas(df1, npartitions=9)

    ddf1.to_parquet(
        str(tmpdir),
        partition_on=partition_on,
        write_index=False,
        **write_engine,
    )
    with pytest.warns(FutureWarning, match="Behavior may change"):
        ddf2 = dd.read_parquet(
            str(tmpdir),
            blocksize=blocksize,
            split_row_groups="adaptive",
            aggregate_files=aggregate_files,
            **read_engine,
        )

    # Check that files where aggregated as expected
    if aggregate_files == "a":
        assert ddf2.npartitions == 3
    elif aggregate_files == "b":
        assert ddf2.npartitions == 6

    # Check that the final data is correct
    df2 = ddf2.compute().sort_values(["c", "d"])
    df1 = df1.sort_values(["c", "d"])
    assert_eq(df1[["c", "d"]], df2[["c", "d"]], check_index=False)


@PYARROW_MARK
@pytest.mark.parametrize("metadata", [True, False])
@pytest.mark.parametrize("blocksize", [None, 1024, 4096, "1MiB"])
def test_split_adaptive_blocksize(tmpdir, blocksize, engine, metadata):
    nparts = 2
    df_size = 100
    row_group_size = 5

    df = pd.DataFrame(
        {
            "a": np.random.choice(["apple", "banana", "carrot"], size=df_size),
            "b": np.random.random(size=df_size),
            "c": np.random.randint(1, 5, size=df_size),
            "index": np.arange(0, df_size),
        }
    ).set_index("index")

    ddf1 = dd.from_pandas(df, npartitions=nparts)
    ddf1.to_parquet(
        str(tmpdir),
        row_group_size=row_group_size,
        write_metadata_file=metadata,
    )

    if metadata:
        path = str(tmpdir)
    else:
        dirname = str(tmpdir)
        files = os.listdir(dirname)
        assert "_metadata" not in files
        path = os.path.join(dirname, "*.parquet")

    ddf2 = dd.read_parquet(
        path,
        blocksize=blocksize,
        split_row_groups="adaptive",
        calculate_divisions=True,
        index="index",
        aggregate_files=True,
        **engine,
    )

    assert_eq(ddf1, ddf2, check_divisions=False)

    num_row_groups = df_size // row_group_size
    if not blocksize:
        assert ddf2.npartitions == ddf1.npartitions
    else:
        # Check that we are really aggregating
        assert ddf2.npartitions < num_row_groups
        if blocksize == "1MiB":
            # Largest blocksize will result in
            # a single output partition
            assert ddf2.npartitions == 1


@PYARROW_MARK
@pytest.mark.parametrize("metadata", [True, False])
@pytest.mark.parametrize("blocksize", ["default", 512, 1024, "1MiB"])
def test_blocksize(tmpdir, blocksize, engine, metadata):
    nparts = 2
    df_size = 25
    row_group_size = 1

    df = pd.DataFrame(
        {
            "a": np.random.choice(["apple", "banana", "carrot"], size=df_size),
            "b": np.random.random(size=df_size),
            "c": np.random.randint(1, 5, size=df_size),
            "index": np.arange(0, df_size),
        }
    ).set_index("index")

    ddf1 = dd.from_pandas(df, npartitions=nparts)
    ddf1.to_parquet(
        str(tmpdir),
        row_group_size=row_group_size,
        write_metadata_file=metadata,
    )

    if metadata:
        path = str(tmpdir)
    else:
        dirname = str(tmpdir)
        files = os.listdir(dirname)
        assert "_metadata" not in files
        path = os.path.join(dirname, "*.parquet")

    # Use (default) split_row_groups="infer"
    # without file aggregation
    ddf2 = dd.read_parquet(
        path,
        blocksize=blocksize,
        index="index",
        **engine,
    )
    assert_eq(df, ddf2)

    if blocksize in (512, 1024):
        # Should get adaptive partitioning
        assert ddf2.npartitions > ddf1.npartitions
        outpath = os.path.join(str(tmpdir), "out")
        ddf2.to_parquet(outpath, **engine)
        for i in range(ddf2.npartitions):
            fn = os.path.join(outpath, f"part.{i}.parquet")
            if engine == {"engine": "fastparquet"}:
                pf = fastparquet.ParquetFile(fn)
                sizep0 = sum([rg.total_byte_size for rg in pf.row_groups])
            else:
                md = pq.ParquetFile(fn).metadata
                sizep0 = sum(
                    [
                        md.row_group(rg).total_byte_size
                        for rg in range(md.num_row_groups)
                    ]
                )
            assert sizep0 <= blocksize
    else:
        # Should get single partition per file
        assert ddf2.npartitions == ddf1.npartitions


def test_roundtrip_pandas_blocksize(tmpdir, write_engine, read_engine):
    path = str(tmpdir.join("test.parquet"))
    pdf = df.copy()
    pdf.index.name = "index"
    pdf.to_parquet(path, **write_engine)

    ddf_read = dd.read_parquet(
        path,
        blocksize="10 kiB",
        calculate_divisions=True,
        split_row_groups=True,
        index="index",
        **read_engine,
    )

    assert_eq(pdf, ddf_read)


@FASTPARQUET_MARK
@pytest.mark.filterwarnings("ignore::FutureWarning")
def test_read_pandas_fastparquet_partitioned(tmpdir, engine):
    pdf = pd.DataFrame(
        [{"str": str(i), "int": i, "group": "ABC"[i % 3]} for i in range(6)]
    )
    path = str(tmpdir)
    pdf.to_parquet(path, partition_cols=["group"], engine="fastparquet")
    ddf_read = dd.read_parquet(path, **engine)

    assert len(ddf_read["group"].compute()) == 6
    assert len(ddf_read.compute().group) == 6


def test_read_parquet_getitem_skip_when_getting_read_parquet(tmpdir, engine):
    # https://github.com/dask/dask/issues/5893
    pdf = pd.DataFrame({"A": [1, 2, 3, 4, 5, 6], "B": ["a", "b", "c", "d", "e", "f"]})
    path = os.path.join(str(tmpdir), "data.parquet")
    pdf.to_parquet(path, engine=engine.get("engine", "pyarrow"))

    ddf = dd.read_parquet(path, **engine)
    a, b = dask.optimize(ddf["A"], ddf)

    # Make sure we are still allowing the getitem optimization
    ddf = ddf["A"]
    dsk = optimize_dataframe_getitem(ddf.dask, keys=[(ddf._name, 0)])
    read = [key for key in dsk.layers if key.startswith("read-parquet")][0]
    subgraph = dsk.layers[read]
    assert isinstance(subgraph, DataFrameIOLayer)
    expected = ["A"]
    assert subgraph.columns == expected


@pytest.mark.parametrize("calculate_divisions", [None, True])
def test_filter_nonpartition_columns(
    tmpdir, write_engine, read_engine, calculate_divisions
):
    tmpdir = str(tmpdir)
    df_write = pd.DataFrame(
        {
            "id": [1, 2, 3, 4] * 4,
            "time": np.arange(16),
            "random": np.random.choice(["cat", "dog"], size=16),
        }
    )
    ddf_write = dd.from_pandas(df_write, npartitions=4)
    ddf_write.to_parquet(tmpdir, write_index=False, partition_on=["id"], **write_engine)
    ddf_read = dd.read_parquet(
        tmpdir,
        index=False,
        calculate_divisions=calculate_divisions,
        filters=[(("time", "<", 5))],
        **read_engine,
    )
    df_read = ddf_read.compute()
    assert len(df_read) == len(df_read[df_read["time"] < 5])
    assert df_read["time"].max() < 5


@PYARROW_MARK
def test_pandas_metadata_nullable_pyarrow(tmpdir):
    tmpdir = str(tmpdir)

    ddf1 = dd.from_pandas(
        pd.DataFrame(
            {
                "A": pd.array([1, None, 2], dtype="Int64"),
                "B": pd.array(["dog", "cat", None], dtype="str"),
            }
        ),
        npartitions=1,
    )
    ddf1.to_parquet(tmpdir)
    ddf2 = dd.read_parquet(tmpdir, calculate_divisions=True)

    assert_eq(ddf1, ddf2, check_index=False)


@PYARROW_MARK
def test_pandas_timestamp_overflow_pyarrow(tmpdir, monkeypatch):
    info = np.iinfo(np.dtype("int64"))
    # In `numpy=1.24.0` NumPy warns when an overflow is encountered when casting from float to int
    # https://numpy.org/doc/stable/release/1.24.0-notes.html#numpy-now-gives-floating-point-errors-in-casts
    if _numpy_124:
        ctx = pytest.warns(RuntimeWarning, match="invalid value encountered in cast")
    else:
        ctx = contextlib.nullcontext()
    with ctx:
        arr_numeric = np.linspace(
            start=info.min + 2, stop=info.max, num=1024, dtype="int64"
        )
    arr_dates = arr_numeric.astype("datetime64[ms]")

    table = pa.Table.from_arrays([pa.array(arr_dates)], names=["ts"])
    pa.parquet.write_table(
        table, f"{tmpdir}/file.parquet", use_deprecated_int96_timestamps=False
    )

    if pyarrow_version < parse_version("13.0.0.dev"):
        # This will raise by default due to overflow
        with pytest.raises(pa.lib.ArrowInvalid) as e:
            dd.read_parquet(str(tmpdir)).compute()
            assert "out of bounds" in str(e.value)
    else:
        dd.read_parquet(str(tmpdir)).compute()

    from dask.dataframe.io.parquet.arrow import ArrowDatasetEngine

    class ArrowEngineWithTimestampClamp(ArrowDatasetEngine):
        @classmethod
        def clamp_arrow_datetimes(cls, arrow_table: pa.Table) -> pa.Table:
            """Constrain datetimes to be valid for pandas

            Since pandas works in ns precision and arrow / parquet defaults to ms
            precision we need to clamp our datetimes to something reasonable"""

            new_columns = []
            for col in arrow_table.columns:
                if pa.types.is_timestamp(col.type) and (
                    col.type.unit in ("s", "ms", "us")
                ):
                    multiplier = {"s": 1_0000_000_000, "ms": 1_000_000, "us": 1_000}[
                        col.type.unit
                    ]

                    original_type = col.type

                    series: pd.Series = col.cast(pa.int64()).to_pandas()
                    info = np.iinfo(np.dtype("int64"))
                    # constrain data to be within valid ranges
                    series.clip(
                        lower=info.min // multiplier + 1,
                        upper=info.max // multiplier,
                        inplace=True,
                    )
                    new_array = pa.array(series, pa.int64())
                    new_array = new_array.cast(original_type)
                    new_columns.append(new_array)
                else:
                    new_columns.append(col)

            return pa.Table.from_arrays(new_columns, names=arrow_table.column_names)

        @classmethod
        def _arrow_table_to_pandas(
            cls,
            arrow_table: pa.Table,
            categories,
            dtype_backend=None,
            convert_string=False,
            **kwargs,
        ) -> pd.DataFrame:
            fixed_arrow_table = cls.clamp_arrow_datetimes(arrow_table)
            return super()._arrow_table_to_pandas(
                fixed_arrow_table,
                categories,
                dtype_backend=dtype_backend,
                convert_string=convert_string,
                **kwargs,
            )

    from dask.dataframe.io.parquet.core import _ENGINES

    assert _ENGINES["pyarrow"] is ArrowDatasetEngine
    monkeypatch.setitem(_ENGINES, "pyarrow", ArrowEngineWithTimestampClamp)

    # this should not fail, but instead produce timestamps that are in the valid range
    dd.read_parquet(str(tmpdir)).compute()


@PYARROW_MARK
def test_arrow_to_pandas(tmpdir, engine):
    # Test that dtypes are correct when arrow_to_pandas is used
    # (See: https://github.com/dask/dask/issues/9664)

    df = pd.DataFrame({"A": [pd.Timestamp("2000-01-01")]})
    path = str(tmpdir.join("test.parquet"))
    df.to_parquet(path, **engine)

    arrow_to_pandas = {"timestamp_as_object": True}
    expect = pq.ParquetFile(path).read().to_pandas(**arrow_to_pandas)
    got = dd.read_parquet(path, arrow_to_pandas=arrow_to_pandas)

    assert_eq(expect, got)
    assert got.A.dtype == got.compute().A.dtype


@pytest.mark.parametrize(
    "write_cols",
    [["part", "col"], ["part", "kind", "col"]],
)
def test_partitioned_column_overlap(tmpdir, engine, write_cols):
    tmpdir.mkdir("part=a")
    tmpdir.mkdir("part=b")
    path0 = str(tmpdir.mkdir("part=a/kind=x"))
    path1 = str(tmpdir.mkdir("part=b/kind=x"))
    path0 = os.path.join(path0, "data.parquet")
    path1 = os.path.join(path1, "data.parquet")

    _df1 = pd.DataFrame({"part": "a", "kind": "x", "col": range(5)})
    _df2 = pd.DataFrame({"part": "b", "kind": "x", "col": range(5)})
    df1 = _df1[write_cols]
    df2 = _df2[write_cols]
    df1.to_parquet(path0, index=False)
    df2.to_parquet(path1, index=False)

    if engine == {"engine": "fastparquet"}:
        path = [path0, path1]
    else:
        path = str(tmpdir)

    expect = pd.concat([_df1, _df2], ignore_index=True)
    if engine == {"engine": "fastparquet"} and fastparquet_version > parse_version(
        "0.8.3"
    ):
        # columns will change order and partitions will be categorical
        result = dd.read_parquet(path, **engine)
        assert result.compute().reset_index(drop=True).to_dict() == expect.to_dict()
    elif write_cols == ["part", "kind", "col"]:
        result = dd.read_parquet(path, **engine)
        assert_eq(result, expect, check_index=False)
    else:
        # For now, partial overlap between partition columns and
        # real columns is not allowed for pyarrow or older fastparquet
        with pytest.raises(ValueError):
            dd.read_parquet(path, **engine)


@PYARROW_MARK
@pytest.mark.parametrize(
    "write_cols",
    [["col"], ["part", "col"]],
)
def test_partitioned_no_pandas_metadata(tmpdir, engine, write_cols):
    # See: https://github.com/dask/dask/issues/8087

    # Manually construct directory-partitioned dataset
    path1 = tmpdir.mkdir("part=a")
    path2 = tmpdir.mkdir("part=b")
    path1 = os.path.join(path1, "data.parquet")
    path2 = os.path.join(path2, "data.parquet")

    # Write partitions without parquet metadata.
    # Note that we always use pyarrow to do this
    # (regardless of the `engine`)
    _df1 = pd.DataFrame({"part": "a", "col": range(5)})
    _df2 = pd.DataFrame({"part": "b", "col": range(5)})
    t1 = pa.Table.from_pandas(
        _df1[write_cols],
        preserve_index=False,
    ).replace_schema_metadata(metadata={})
    pq.write_table(t1, path1)
    t2 = pa.Table.from_pandas(
        _df2[write_cols],
        preserve_index=False,
    ).replace_schema_metadata(metadata={})
    pq.write_table(t2, path2)

    # Check results
    expect = pd.concat([_df1, _df2], ignore_index=True)
    result = dd.read_parquet(str(tmpdir), **engine)
    result["part"] = result["part"].astype("object")
    assert_eq(result[list(expect.columns)], expect, check_index=False)


@PYARROW_MARK
def test_pyarrow_directory_partitioning(tmpdir):
    # Manually construct directory-partitioned dataset
    path1 = tmpdir.mkdir("a")
    path2 = tmpdir.mkdir("b")
    path1 = os.path.join(path1, "data.parquet")
    path2 = os.path.join(path2, "data.parquet")
    _df1 = pd.DataFrame({"part": "a", "col": range(5)})
    _df2 = pd.DataFrame({"part": "b", "col": range(5)})
    _df1.to_parquet(path1)
    _df2.to_parquet(path2)

    # Check results
    expect = pd.concat([_df1, _df2], ignore_index=True)
    result = dd.read_parquet(
        str(tmpdir),
        dataset={"partitioning": ["part"], "partition_base_dir": str(tmpdir)},
    )
    result["part"] = result["part"].astype("object")
    assert_eq(result[list(expect.columns)], expect, check_index=False)


def test_partitioned_preserve_index(tmpdir, write_engine, read_engine):
    tmp = str(tmpdir)
    size = 1_000
    npartitions = 4
    b = np.arange(npartitions, dtype="int32").repeat(size // npartitions)
    data = pd.DataFrame(
        {
            "myindex": np.arange(size),
            "A": np.random.random(size=size),
            "B": pd.Categorical(b),
        }
    ).set_index("myindex")
    data.index.name = None
    df1 = dd.from_pandas(data, npartitions=npartitions)
    df1.to_parquet(tmp, partition_on="B", **write_engine)

    expect = data[data["B"] == 1]
    if PANDAS_GE_200 and read_engine == {"engine": "fastparquet"}:
        # fastparquet does not preserve dtype of cats
        expect = expect.copy()  # SettingWithCopyWarning
        expect["B"] = expect["B"].astype(
            pd.CategoricalDtype(expect["B"].dtype.categories.astype("int64"))
        )
    got = dd.read_parquet(tmp, filters=[("B", "==", 1)], **read_engine)
    assert_eq(expect, got)


def test_from_pandas_preserve_none_index(tmpdir, engine):
    fn = str(tmpdir.join("test.parquet"))
    df = pd.DataFrame({"a": [1, 2], "b": [4, 5], "c": [6, 7]}).set_index("c")
    df.index.name = None
    df.to_parquet(fn, index=True, **engine)
    expect = pd.read_parquet(fn)
    got = dd.read_parquet(fn, **engine)
    assert_eq(expect, got)


def test_multi_partition_none_index_false(tmpdir, engine):
    # Write dataset without dask.to_parquet
    ddf1 = ddf.reset_index(drop=True)
    for i, part in enumerate(ddf1.partitions):
        path = tmpdir.join(f"test.{i}.parquet")
        part.compute().to_parquet(str(path), **engine)

    # Read back with index=False
    ddf2 = dd.read_parquet(str(tmpdir), index=False, **engine)
    assert_eq(ddf1, ddf2)


def test_from_pandas_preserve_none_rangeindex(tmpdir, write_engine, read_engine):
    # See GitHub Issue#6348
    fn = str(tmpdir.join("test.parquet"))
    df0 = pd.DataFrame({"t": [1, 2, 3]}, index=pd.RangeIndex(start=1, stop=4))
    df0.to_parquet(fn, **write_engine)
    df1 = dd.read_parquet(fn, **read_engine)
    assert_eq(df0, df1.compute())


def test_illegal_column_name(tmpdir, engine):
    # Make sure user is prevented from preserving a "None" index
    # name if there is already a column using the special `null_name`
    null_name = "__null_dask_index__"
    fn = str(tmpdir.join("test.parquet"))
    df = pd.DataFrame({"x": [1, 2], null_name: [4, 5]}).set_index("x")
    df.index.name = None
    ddf = dd.from_pandas(df, npartitions=2)

    # If we don't want to preserve the None index name, the
    # write should work, but the user should be warned
    with pytest.warns(UserWarning, match=null_name):
        ddf.to_parquet(fn, write_index=False, **engine)

    # If we do want to preserve the None index name, should
    # get a ValueError for having an illegal column name
    with pytest.raises(ValueError) as e:
        ddf.to_parquet(fn, **engine)
    assert null_name in str(e.value)


def test_divisions_with_null_partition(tmpdir, engine):
    df = pd.DataFrame({"a": [1, 2, None, None], "b": [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(str(tmpdir), write_index=False, **engine)

    ddf_read = dd.read_parquet(str(tmpdir), index="a", **engine)
    assert ddf_read.divisions == (None, None, None)


@PYARROW_MARK
def test_pyarrow_dataset_simple(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df = df.set_index("a", drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, **engine)
    read_df = dd.read_parquet(fn, calculate_divisions=True)
    read_df.compute()
    assert_eq(ddf, read_df)


@PYARROW_MARK
@pytest.mark.parametrize("test_filter", [True, False])
def test_pyarrow_dataset_partitioned(tmpdir, engine, test_filter):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df["b"] = df["b"].astype("category")
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, partition_on="b", write_metadata_file=True, **engine)
    read_df = dd.read_parquet(
        fn,
        filters=[("b", "==", "a")] if test_filter else None,
        calculate_divisions=True,
    )

    if test_filter:
        assert_eq(ddf[ddf["b"] == "a"].compute(), read_df.compute())
    else:
        assert_eq(ddf, read_df)


@PYARROW_MARK
@pytest.mark.parametrize("scheduler", [None, "processes"])
def test_null_partition_pyarrow(tmpdir, scheduler):
    df = pd.DataFrame(
        {
            "id": pd.Series([0, 1, None], dtype="Int64"),
            "x": pd.Series([1, 2, 3], dtype="Int64"),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf.to_parquet(str(tmpdir), partition_on="id")
    fns = glob.glob(os.path.join(tmpdir, "id=*/*.parquet"))
    assert len(fns) == 3

    # Check proper partitioning usage
    ddf_read = dd.read_parquet(
        str(tmpdir),
        dtype_backend="numpy_nullable",
        dataset={
            "partitioning": {
                "flavor": "hive",
                "schema": pa.schema([("id", pa.int64())]),
            },
        },
    )

    if pyarrow_version.major >= 12:
        # pyarrow>=12 would also convert index dtype to nullable
        # see https://github.com/apache/arrow/pull/34445
        ddf.index = ddf.index.astype("Int64")

    assert_eq(
        ddf[["x", "id"]],
        ddf_read[["x", "id"]],
        check_divisions=False,
        scheduler=scheduler,
    )


@PYARROW_MARK
def test_pyarrow_dataset_read_from_paths(tmpdir):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df["b"] = df["b"].astype("category")
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, partition_on="b")

    with pytest.warns(FutureWarning):
        read_df_1 = dd.read_parquet(
            fn, filters=[("b", "==", "a")], read_from_paths=False
        )

    read_df_2 = dd.read_parquet(fn, filters=[("b", "==", "a")])

    assert_eq(read_df_1, read_df_2)
    assert_eq(ddf[ddf["b"] == "a"].compute(), read_df_2.compute())


@PYARROW_MARK
@pytest.mark.parametrize("split_row_groups", [True, False])
def test_pyarrow_dataset_filter_partitioned(tmpdir, split_row_groups):
    fn = str(tmpdir)
    df = pd.DataFrame(
        {
            "a": [4, 5, 6],
            "b": ["a", "b", "b"],
            "c": ["A", "B", "B"],
        }
    )
    df["b"] = df["b"].astype("category")
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, partition_on=["b", "c"])

    # Filter on a a non-partition column
    read_df = dd.read_parquet(
        fn,
        split_row_groups=split_row_groups,
        filters=[("a", "==", 5)],
    )
    assert_eq(
        read_df.compute()[["a"]],
        df[df["a"] == 5][["a"]],
        check_index=False,
    )


def test_pyarrow_dataset_filter_on_partitioned(tmpdir, engine):
    # See: https://github.com/dask/dask/issues/9246
    df = pd.DataFrame({"val": range(7), "part": list("abcdefg")})
    ddf = dd.from_map(
        lambda i: df.iloc[i : i + 1],
        range(7),
    )
    ddf.to_parquet(tmpdir, partition_on=["part"], **engine)

    # Check that List[Tuple] filters are applied
    read_ddf = dd.read_parquet(tmpdir, filters=[("part", "==", "c")], **engine)
    read_ddf["part"] = read_ddf["part"].astype("object")
    assert_eq(df.iloc[2:3], read_ddf)

    # Check that List[List[Tuple]] filters are applied.
    # (fastparquet doesn't support this format)
    if engine == {}:  # payrrow
        read_ddf = dd.read_parquet(tmpdir, filters=[[("part", "==", "c")]])
        read_ddf["part"] = read_ddf["part"].astype("object")
        assert_eq(df.iloc[2:3], read_ddf)


@PYARROW_MARK
def test_parquet_pyarrow_write_empty_metadata(tmpdir):
    # https://github.com/dask/dask/issues/6600
    tmpdir = str(tmpdir)

    df_a = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [], "y": []}, dtype=("int", "int")
    )
    df_b = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [1, 1, 2, 2], "y": [1, 0, 1, 0]}, dtype=("int64", "int64")
    )
    df_c = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [1, 2, 1, 2], "y": [1, 0, 1, 0]}, dtype=("int64", "int64")
    )

    df = dd.from_delayed([df_a, df_b, df_c])
    df.to_parquet(
        tmpdir,
        partition_on=["x"],
        append=False,
        write_metadata_file=True,
    )

    # Check that metadata files where written
    files = os.listdir(tmpdir)
    assert "_metadata" in files
    assert "_common_metadata" in files

    # Check that the schema includes pandas_metadata
    schema_common = pq.ParquetFile(
        os.path.join(tmpdir, "_common_metadata")
    ).schema.to_arrow_schema()
    pandas_metadata = schema_common.pandas_metadata
    assert pandas_metadata
    assert pandas_metadata.get("index_columns", False)


@PYARROW_MARK
def test_parquet_pyarrow_write_empty_metadata_append(tmpdir):
    # https://github.com/dask/dask/issues/6600
    tmpdir = str(tmpdir)

    df_a = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [1, 1, 2, 2], "y": [1, 0, 1, 0]}, dtype=("int64", "int64")
    )
    df_b = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [1, 2, 1, 2], "y": [2, 0, 2, 0]}, dtype=("int64", "int64")
    )

    df1 = dd.from_delayed([df_a, df_b])
    df1.to_parquet(
        tmpdir,
        partition_on=["x"],
        append=False,
        write_metadata_file=True,
    )

    df_c = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [], "y": []}, dtype=("int64", "int64")
    )
    df_d = dask.delayed(pd.DataFrame.from_dict)(
        {"x": [3, 3, 4, 4], "y": [1, 0, 1, 0]}, dtype=("int64", "int64")
    )

    df2 = dd.from_delayed([df_c, df_d])
    df2.to_parquet(
        tmpdir,
        partition_on=["x"],
        append=True,
        ignore_divisions=True,
        write_metadata_file=True,
    )


@PYARROW_MARK
@pytest.mark.parametrize("partition_on", [None, "a"])
def test_create_metadata_file(tmpdir, write_engine, read_engine, partition_on):
    tmpdir = str(tmpdir)

    # Write ddf without a _metadata file
    df1 = pd.DataFrame({"b": range(100), "a": ["A", "B", "C", "D"] * 25})
    df1.index.name = "myindex"
    ddf1 = dd.from_pandas(df1, npartitions=10)
    ddf1.to_parquet(
        tmpdir,
        write_metadata_file=False,
        partition_on=partition_on,
        **write_engine,
    )

    # Add global _metadata file
    if partition_on:
        fns = glob.glob(os.path.join(tmpdir, partition_on + "=*/*.parquet"))
    else:
        fns = glob.glob(os.path.join(tmpdir, "*.parquet"))
    dd.io.parquet.create_metadata_file(
        fns,
        split_every=3,  # Force tree reduction
    )

    # Check that we can now read the ddf
    # with the _metadata file present
    ddf2 = dd.read_parquet(
        tmpdir,
        calculate_divisions=True,
        split_row_groups=False,
        **read_engine,
        index="myindex",  # python-3.6 CI
    )
    if partition_on:
        ddf1 = df1.sort_values("b")
        ddf2 = ddf2.compute().sort_values("b")
        ddf2.a = ddf2.a.astype("object")
    assert_eq(ddf1, ddf2)

    # Check if we can avoid writing an actual file
    fmd = dd.io.parquet.create_metadata_file(
        fns,
        split_every=3,  # Force tree reduction
        out_dir=False,  # Avoid writing file
    )

    # Check that the in-memory metadata is the same as
    # the metadata in the file.
    fmd_file = pq.ParquetFile(os.path.join(tmpdir, "_metadata")).metadata
    assert fmd.num_rows == fmd_file.num_rows
    assert fmd.num_columns == fmd_file.num_columns
    assert fmd.num_row_groups == fmd_file.num_row_groups


def test_read_write_overwrite_is_true(tmpdir, engine):
    # https://github.com/dask/dask/issues/6824

    # Create a Dask DataFrame if size (100, 10) with 5 partitions and write to local
    ddf = dd.from_pandas(
        pd.DataFrame(
            np.random.randint(low=0, high=100, size=(100, 10)),
            columns=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        ),
        npartitions=5,
    )
    ddf = ddf.reset_index(drop=True)
    dd.to_parquet(ddf, tmpdir, overwrite=True, **engine)

    # Keep the contents of the DataFrame constant but change the # of partitions
    ddf2 = ddf.repartition(npartitions=3)

    # Overwrite the existing Dataset with the new dataframe and evaluate
    # the number of files against the number of dask partitions
    dd.to_parquet(ddf2, tmpdir, overwrite=True, **engine)

    # Assert the # of files written are identical to the number of
    # Dask DataFrame partitions (we exclude _metadata and _common_metadata)
    files = os.listdir(tmpdir)
    files = [f for f in files if f not in ["_common_metadata", "_metadata"]]
    assert len(files) == ddf2.npartitions


def test_read_write_partition_on_overwrite_is_true(tmpdir, engine):
    # https://github.com/dask/dask/issues/6824
    from pathlib import Path

    # Create a Dask DataFrame with 5 partitions and write to local, partitioning on the column A and column B
    df = pd.DataFrame(
        np.vstack(
            (
                np.full((50, 3), 0),
                np.full((50, 3), 1),
                np.full((20, 3), 2),
            )
        )
    )
    df.columns = ["A", "B", "C"]
    ddf = dd.from_pandas(df, npartitions=5)
    dd.to_parquet(ddf, tmpdir, partition_on=["A", "B"], overwrite=True, **engine)

    # Get the total number of files and directories from the original write
    files_ = Path(tmpdir).rglob("*")
    files = [f.as_posix() for f in files_]
    # Keep the contents of the DataFrame constant but change the # of partitions
    ddf2 = ddf.repartition(npartitions=3)

    # Overwrite the existing Dataset with the new dataframe and evaluate
    # the number of files against the number of dask partitions
    # Get the total number of files and directories from the original write
    dd.to_parquet(ddf2, tmpdir, partition_on=["A", "B"], overwrite=True, **engine)
    files2_ = Path(tmpdir).rglob("*")
    files2 = [f.as_posix() for f in files2_]
    # After reducing the # of partitions and overwriting, we expect
    # there to be fewer total files than were originally written
    assert len(files2) < len(files)


def test_to_parquet_overwrite_adaptive_round_trip(tmpdir, engine):
    df = pd.DataFrame({"a": range(128)})
    ddf = dd.from_pandas(df, npartitions=8)
    path = os.path.join(str(tmpdir), "path")
    ddf.to_parquet(path, **engine)
    ddf2 = dd.read_parquet(path, split_row_groups="adaptive", **engine).repartition(
        partition_size="1GB"
    )
    path_new = os.path.join(str(tmpdir), "path_new")
    ddf2.to_parquet(path_new, overwrite=True, **engine)
    ddf2.to_parquet(path_new, overwrite=True, **engine)
    assert_eq(
        ddf2,
        dd.read_parquet(path_new, split_row_groups=False, **engine),
    )


def test_to_parquet_overwrite_raises(tmpdir, engine):
    # https://github.com/dask/dask/issues/6824
    # Check that overwrite=True will raise an error if the
    # specified path is the current working directory
    df = pd.DataFrame({"a": range(12)})
    ddf = dd.from_pandas(df, npartitions=3)
    with pytest.raises(ValueError):
        dd.to_parquet(ddf, "./", overwrite=True, **engine)
    with pytest.raises(ValueError):
        dd.to_parquet(ddf, tmpdir, append=True, overwrite=True, **engine)


def test_to_parquet_overwrite_files_from_read_parquet_in_same_call_raises(
    tmpdir, engine
):
    subdir = tmpdir.mkdir("subdir")

    dd.from_pandas(pd.DataFrame({"x": range(20)}), npartitions=2).to_parquet(
        subdir, **engine
    )

    ddf = dd.read_parquet(subdir)

    # Test writing to the same files, as well as a parent directory
    for target in [subdir, tmpdir]:
        with pytest.raises(ValueError, match="same parquet file"):
            ddf.to_parquet(target, overwrite=True)

        ddf2 = ddf.assign(y=ddf.x + 1)

        with pytest.raises(ValueError, match="same parquet file"):
            ddf2.to_parquet(target, overwrite=True)


def test_to_parquet_errors_non_string_column_names(tmpdir, engine):
    df = pd.DataFrame({"x": range(10), 1: range(10)})
    ddf = dd.from_pandas(df, npartitions=2)
    with pytest.raises(ValueError, match="non-string column names"):
        ddf.to_parquet(str(tmpdir.join("temp")), **engine)


def test_dir_filter(tmpdir, engine):
    # github #6898
    df = pd.DataFrame.from_dict(
        {
            "A": {
                0: 351.0,
                1: 355.0,
                2: 358.0,
                3: 266.0,
                4: 266.0,
                5: 268.0,
                6: np.nan,
            },
            "B": {
                0: 2063.0,
                1: 2051.0,
                2: 1749.0,
                3: 4281.0,
                4: 3526.0,
                5: 3462.0,
                6: np.nan,
            },
            "year": {0: 2019, 1: 2019, 2: 2020, 3: 2020, 4: 2020, 5: 2020, 6: 2020},
        }
    )
    ddf = dask.dataframe.from_pandas(df, npartitions=1)
    ddf.to_parquet(tmpdir, partition_on="year", **engine)
    ddf2 = dd.read_parquet(tmpdir, filters=[("year", "==", 2020)], **engine)
    ddf2["year"] = ddf2.year.astype("int64")
    assert_eq(ddf2, df[df.year == 2020])


@PYARROW_MARK
def test_roundtrip_decimal_dtype(tmpdir):
    # https://github.com/dask/dask/issues/6948
    tmpdir = str(tmpdir)

    data = [
        {
            "ts": pd.to_datetime("2021-01-01", utc="Europe/Berlin"),
            "col1": Decimal("123.00"),
        }
        for i in range(23)
    ]
    ddf1 = dd.from_pandas(pd.DataFrame(data), npartitions=1)

    ddf1.to_parquet(path=tmpdir, schema={"col1": pa.decimal128(5, 2)})
    ddf2 = dd.read_parquet(tmpdir)

    if pyarrow_strings_enabled():
        assert pa.types.is_decimal(ddf2["col1"].dtype.pyarrow_dtype)
        ddf1 = ddf1.astype({"col1": pd.ArrowDtype(pa.decimal128(5, 2))})
    else:
        assert ddf1["col1"].dtype == ddf2["col1"].dtype
    assert_eq(ddf1, ddf2, check_divisions=False)


@PYARROW_MARK
def test_roundtrip_date_dtype(tmpdir):
    # https://github.com/dask/dask/issues/6948
    tmpdir = str(tmpdir)

    data = [
        {
            "ts": pd.to_datetime("2021-01-01", utc="Europe/Berlin"),
            "col1": date(2020, 10, 10),
        }
        for _ in range(23)
    ]
    ddf1 = dd.from_pandas(pd.DataFrame(data), npartitions=1)

    ddf1.to_parquet(path=tmpdir, schema={"col1": pa.date32()})
    ddf2 = dd.read_parquet(tmpdir)

    if pyarrow_strings_enabled():
        assert pa.types.is_date32(ddf2["col1"].dtype.pyarrow_dtype)
        ddf1 = ddf1.astype({"col1": pd.ArrowDtype(pa.date32())})
    else:
        assert ddf1["col1"].dtype == ddf2["col1"].dtype
    assert_eq(ddf1, ddf2, check_divisions=False)


def test_roundtrip_rename_columns(tmpdir, engine):
    # https://github.com/dask/dask/issues/7017

    path = os.path.join(str(tmpdir), "test.parquet")
    df1 = pd.DataFrame(columns=["a", "b", "c"], data=np.random.uniform(size=(10, 3)))
    df1.to_parquet(path)

    # read it with dask and rename columns
    ddf2 = dd.read_parquet(path, **engine)
    ddf2.columns = ["d", "e", "f"]
    df1.columns = ["d", "e", "f"]

    assert_eq(df1, ddf2.compute())


def test_custom_metadata(tmpdir, engine):
    # Write a parquet dataset with custom metadata

    # Define custom metadata
    custom_metadata = {b"my_key": b"my_data"}

    # Write parquet dataset
    path = str(tmpdir)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    dd.from_pandas(df, npartitions=2).to_parquet(
        path,
        custom_metadata=custom_metadata,
        write_metadata_file=True,
        **engine,
    )

    # Check that data is correct
    assert_eq(df, dd.read_parquet(path, **engine))

    # Require pyarrow.parquet to check key/value metadata
    if pq:
        # Read footer metadata and _metadata.
        # Check that it contains keys/values from `custom_metadata`
        files = glob.glob(os.path.join(path, "*.parquet"))
        files += [os.path.join(path, "_metadata")]
        for fn in files:
            _md = pq.ParquetFile(fn).metadata.metadata
            for k in custom_metadata.keys():
                assert _md[k] == custom_metadata[k]

    # Make sure we raise an error if the custom metadata
    # includes a b"pandas" key
    custom_metadata = {b"pandas": b"my_new_pandas_md"}
    with pytest.raises(ValueError) as e:
        dd.from_pandas(df, npartitions=2).to_parquet(
            path, custom_metadata=custom_metadata, **engine
        )
    assert "User-defined key/value" in str(e.value)


@pytest.mark.parametrize("calculate_divisions", [True, False, None])
def test_ignore_metadata_file(tmpdir, engine, calculate_divisions):
    tmpdir = str(tmpdir)
    dataset_with_bad_metadata = os.path.join(tmpdir, "data1")
    dataset_without_metadata = os.path.join(tmpdir, "data2")

    # Write two identical datasets without any _metadata file
    df1 = pd.DataFrame({"a": range(100), "b": ["dog", "cat"] * 50})
    ddf1 = dd.from_pandas(df1, npartitions=2)
    ddf1.to_parquet(path=dataset_with_bad_metadata, write_metadata_file=False, **engine)
    ddf1.to_parquet(path=dataset_without_metadata, write_metadata_file=False, **engine)

    # Copy "bad" metadata into `dataset_with_bad_metadata`
    assert "_metadata" not in os.listdir(dataset_with_bad_metadata)
    with open(os.path.join(dataset_with_bad_metadata, "_metadata"), "w") as f:
        f.write("INVALID METADATA")
    assert "_metadata" in os.listdir(dataset_with_bad_metadata)
    assert "_metadata" not in os.listdir(dataset_without_metadata)

    # Read back the datasets with `ignore_metadata_file=True`, and
    # test that the results are the same
    ddf2a = dd.read_parquet(
        dataset_with_bad_metadata,
        ignore_metadata_file=True,
        calculate_divisions=calculate_divisions,
        **engine,
    )
    ddf2b = dd.read_parquet(
        dataset_without_metadata,
        ignore_metadata_file=True,
        calculate_divisions=calculate_divisions,
        **engine,
    )
    assert_eq(ddf2a, ddf2b)


@pytest.mark.parametrize("write_metadata_file", [True, False])
@pytest.mark.parametrize("metadata_task_size", [2, 0])
def test_metadata_task_size(tmpdir, engine, write_metadata_file, metadata_task_size):
    # Write simple dataset
    tmpdir = str(tmpdir)
    df1 = pd.DataFrame({"a": range(100), "b": ["dog", "cat"] * 50})
    ddf1 = dd.from_pandas(df1, npartitions=10)
    ddf1.to_parquet(path=str(tmpdir), write_metadata_file=write_metadata_file, **engine)

    # Read back
    ddf2a = dd.read_parquet(str(tmpdir), calculate_divisions=True, **engine)
    ddf2b = dd.read_parquet(
        str(tmpdir),
        calculate_divisions=True,
        metadata_task_size=metadata_task_size,
        **engine,
    )
    assert_eq(ddf2a, ddf2b)

    with dask.config.set(
        {"dataframe.parquet.metadata-task-size-local": metadata_task_size}
    ):
        ddf2c = dd.read_parquet(str(tmpdir), calculate_divisions=True, **engine)
    assert_eq(ddf2b, ddf2c)


@PYARROW_MARK
@pytest.mark.parametrize("partition_on", ("b", None))
def test_extra_file(tmpdir, engine, partition_on):
    # Check that read_parquet can handle spark output
    # See: https://github.com/dask/dask/issues/8087
    tmpdir = str(tmpdir)
    df = pd.DataFrame({"a": range(100), "b": ["dog", "cat"] * 50})
    df = df.assign(b=df.b.astype("category"))
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(
        tmpdir, write_metadata_file=True, partition_on=partition_on, **engine
    )
    open(os.path.join(tmpdir, "_SUCCESS"), "w").close()
    open(os.path.join(tmpdir, "part.0.parquet.crc"), "w").close()
    os.remove(os.path.join(tmpdir, "_metadata"))
    out = dd.read_parquet(tmpdir, calculate_divisions=True, **engine)
    # Weird two-step since that we don't care if category ordering changes
    assert_eq(out, df, check_categorical=False)
    assert_eq(out.b, df.b, check_category_order=False)

    # For "fastparquet" and "pyarrow", we can pass the
    # expected file extension, or avoid checking file extensions
    # by passing False. Check here that this works:

    def _parquet_file_extension(val, legacy=False):
        # This function allows us to switch between "new"
        # and "legacy" parquet_file_extension behavior
        return (
            {"dataset": {"require_extension": val}}
            if legacy
            else {"parquet_file_extension": val}
        )

    # Should Work
    out = dd.read_parquet(
        tmpdir,
        **_parquet_file_extension(".parquet"),
        calculate_divisions=True,
        **engine,
    )
    # Weird two-step since that we don't care if category ordering changes
    assert_eq(out, df, check_categorical=False)
    assert_eq(out.b, df.b, check_category_order=False)

    # Should Work (with FutureWarning)
    with pytest.warns(FutureWarning, match="require_extension is deprecated"):
        out = dd.read_parquet(
            tmpdir,
            **_parquet_file_extension(".parquet", legacy=True),
            calculate_divisions=True,
            **engine,
        )

    # Should Fail (for not capturing the _SUCCESS and crc files)
    with pytest.raises((OSError, pa.lib.ArrowInvalid)):
        dd.read_parquet(tmpdir, **engine, **_parquet_file_extension(None)).compute()

    # Should Fail (for filtering out all files)
    # (Related to: https://github.com/dask/dask/issues/8349)
    with pytest.raises(ValueError):
        dd.read_parquet(tmpdir, **engine, **_parquet_file_extension(".foo")).compute()


def test_unsupported_extension_file(tmpdir, engine):
    # File extension shouldn't matter when we are only
    # reading a single file.
    # (See: https://github.com/dask/dask/issues/8349)
    fn = os.path.join(str(tmpdir), "multi.foo")
    df0 = pd.DataFrame({"a": range(10)})
    df0.to_parquet(fn, **engine)
    assert_eq(df0, dd.read_parquet(fn, index=False, calculate_divisions=True, **engine))


def test_unsupported_extension_dir(tmpdir, engine):
    # File extensions shouldn't matter when we have
    # a _metadata file
    # (Related to: https://github.com/dask/dask/issues/8349)
    path = str(tmpdir)
    ddf0 = dd.from_pandas(pd.DataFrame({"a": range(10)}), 1)
    ddf0.to_parquet(
        path,
        name_function=lambda i: f"part.{i}.foo",
        write_metadata_file=True,
        **engine,
    )
    assert_eq(ddf0, dd.read_parquet(path, calculate_divisions=True, **engine))


def test_custom_filename(tmpdir, engine):
    fn = str(tmpdir)
    pdf = pd.DataFrame(
        {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
    )
    df = dd.from_pandas(pdf, npartitions=2)
    df.to_parquet(
        fn,
        write_metadata_file=True,
        name_function=lambda x: f"hi-{x}.parquet",
        **engine,
    )

    files = os.listdir(fn)
    assert "_common_metadata" in files
    assert "_metadata" in files
    assert "hi-0.parquet" in files
    assert "hi-1.parquet" in files
    assert_eq(df, dd.read_parquet(fn, calculate_divisions=True, **engine))


@PYARROW_MARK
def test_custom_filename_works_with_pyarrow_when_append_is_true(tmpdir):
    fn = str(tmpdir)
    pdf = pd.DataFrame(
        {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
    )
    df = dd.from_pandas(pdf, npartitions=2)
    df.to_parquet(
        fn,
        write_metadata_file=True,
        name_function=lambda x: f"hi-{x * 2}.parquet",
    )

    pdf = pd.DataFrame(
        {"num1": [33], "num2": [44]},
    )
    df = dd.from_pandas(pdf, npartitions=1)

    df.to_parquet(
        fn,
        name_function=lambda x: f"hi-{x * 2}.parquet",
        append=True,
        ignore_divisions=True,
    )
    files = os.listdir(fn)
    assert "_common_metadata" in files
    assert "_metadata" in files
    assert "hi-0.parquet" in files
    assert "hi-2.parquet" in files
    assert "hi-4.parquet" in files
    expected_pdf = pd.DataFrame(
        {"num1": [1, 2, 3, 4, 33], "num2": [7, 8, 9, 10, 44]},
    )
    actual = dd.read_parquet(fn, index=False)
    assert_eq(actual, expected_pdf, check_index=False)


def test_throws_error_if_custom_filename_is_invalid(tmpdir, engine):
    fn = str(tmpdir)
    pdf = pd.DataFrame(
        {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
    )
    df = dd.from_pandas(pdf, npartitions=2)
    with pytest.raises(
        ValueError, match="``name_function`` must be a callable with one argument."
    ):
        df.to_parquet(fn, name_function="whatever.parquet", **engine)

    with pytest.raises(
        ValueError, match="``name_function`` must produce unique filenames."
    ):
        df.to_parquet(fn, name_function=lambda x: "whatever.parquet", **engine)


def test_custom_filename_with_partition(tmpdir, engine):
    fn = str(tmpdir)
    pdf = pd.DataFrame(
        {
            "first_name": ["frank", "li", "marcela", "luis"],
            "country": ["canada", "china", "venezuela", "venezuela"],
        },
    )
    df = dd.from_pandas(pdf, npartitions=4)
    df.to_parquet(
        fn,
        partition_on=["country"],
        name_function=lambda x: f"{x}-cool.parquet",
        write_index=False,
        **engine,
    )

    for _, dirs, files in os.walk(fn):
        for dir in dirs:
            assert dir in (
                "country=canada",
                "country=china",
                "country=venezuela",
            )
        for file in files:
            assert file in (
                *[f"{i}-cool.parquet" for i in range(df.npartitions)],
                "_common_metadata",
                "_metadata",
            )
    actual = dd.read_parquet(fn, index=False, **engine)
    assert_eq(
        pdf, actual, check_index=False, check_dtype=False, check_categorical=False
    )


@PYARROW_MARK
def test_roundtrip_partitioned_pyarrow_dataset(tmpdir, engine):
    # See: https://github.com/dask/dask/issues/8650

    if engine == {"engine": "fastparquet"} and PANDAS_GE_200:
        # https://github.com/dask/dask/issues/9966
        pytest.xfail("fastparquet reads as int64 while pyarrow does as int32")

    import pyarrow.parquet as pq
    from pyarrow.dataset import HivePartitioning, write_dataset

    # Sample data
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # Write partitioned dataset with dask
    dask_path = tmpdir.mkdir("foo-dask")
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(dask_path, partition_on=["col1"], write_index=False, **engine)

    # Write partitioned dataset with pyarrow
    pa_path = tmpdir.mkdir("foo-pyarrow")
    table = pa.Table.from_pandas(df)
    write_dataset(
        data=table,
        base_dir=pa_path,
        basename_template="part.{i}.parquet",
        format="parquet",
        partitioning=HivePartitioning(pa.schema([("col1", pa.int32())])),
    )

    # Define simple function to ensure results should
    # be comparable (same column and row order)
    def _prep(x):
        return x.sort_values("col2")[["col1", "col2"]]

    # Check that reading dask-written data is the same for pyarrow and dask
    df_read_dask = dd.read_parquet(dask_path, **engine)
    df_read_pa = pq.read_table(dask_path).to_pandas()
    assert_eq(_prep(df_read_dask), _prep(df_read_pa), check_index=False)

    # Check that reading pyarrow-written data is the same for pyarrow and dask
    df_read_dask = dd.read_parquet(pa_path, **engine)
    df_read_pa = pq.read_table(pa_path).to_pandas()
    assert_eq(_prep(df_read_dask), _prep(df_read_pa), check_index=False)


@pytest.mark.parametrize("filter_value", ({1}, [1], (1,)), ids=("set", "list", "tuple"))
def test_in_predicate_can_use_iterables(tmp_path, engine, filter_value):
    """Regression test for https://github.com/dask/dask/issues/8720"""
    path = tmp_path / "in_predicate_iterable_pandas.parquet"
    df = pd.DataFrame(
        {"A": [1, 2, 3, 4], "B": [1, 1, 2, 2]},
    )
    df.to_parquet(path, **engine)
    filters = [("B", "in", filter_value)]
    result = dd.read_parquet(path, filters=filters, **engine)
    expected = pd.read_parquet(path, filters=filters, **engine)
    assert_eq(result, expected)

    # pandas to_parquet outputs a single file, dask outputs a folder with global
    # metadata that changes the filtering code path
    ddf = dd.from_pandas(df, npartitions=2)
    path = tmp_path / "in_predicate_iterable_dask.parquet"
    ddf.to_parquet(path, **engine)
    result = dd.read_parquet(path, filters=filters, **engine)
    expected = pd.read_parquet(path, filters=filters, **engine)
    assert_eq(result, expected, check_index=False)


def test_not_in_predicate(tmp_path, engine):
    ddf = dd.from_dict(
        {"A": range(8), "B": [1, 1, 2, 2, 3, 3, 4, 4]},
        npartitions=4,
    )
    ddf.to_parquet(tmp_path, **engine)
    filters = [[("B", "not in", (1, 2))]]
    result = dd.read_parquet(tmp_path, filters=filters, **engine)
    expected = pd.read_parquet(tmp_path, filters=filters, **engine)
    assert_eq(result, expected, check_index=False)

    with pytest.raises(ValueError, match="not a valid operator in predicates"):
        unsupported_op = [[("B", "not eq", 1)]]
        dd.read_parquet(tmp_path, filters=unsupported_op, **engine)


# Non-iterable filter value with `in` predicate
# Test single nested and double nested lists of filters, as well as having multiple
# filters to test against.
@pytest.mark.parametrize(
    "filter_value",
    (
        [("B", "in", 10)],
        [[("B", "in", 10)]],
        [("B", "<", 10), ("B", "in", 10)],
        [[("B", "<", 10), ("B", "in", 10)]],
    ),
    ids=(
        "one-item-single-nest",
        "one-item-double-nest",
        "two-item-double-nest",
        "two-item-two-nest",
    ),
)
def test_in_predicate_requires_an_iterable(tmp_path, engine, filter_value):
    """Regression test for https://github.com/dask/dask/issues/8720"""
    path = tmp_path / "gh_8720_pandas.parquet"
    df = pd.DataFrame(
        {"A": [1, 2, 3, 4], "B": [1, 1, 2, 2]},
    )
    df.to_parquet(path, **engine)
    with pytest.raises(TypeError, match="Value of 'in' filter"):
        dd.read_parquet(path, filters=filter_value, **engine)

    # pandas to_parquet outputs a single file, dask outputs a folder with global
    # metadata that changes the filtering code path
    ddf = dd.from_pandas(df, npartitions=2)
    path = tmp_path / "gh_8720_dask.parquet"
    ddf.to_parquet(path, **engine)
    with pytest.raises(TypeError, match="Value of 'in' filter"):
        dd.read_parquet(path, filters=filter_value, **engine)


def test_deprecate_gather_statistics(tmp_path, engine):
    # The `gather_statistics` deprecation warning
    # (and this test) should be removed after a
    # "sufficient" deprecation period.
    # See: https://github.com/dask/dask/pull/8992
    df = pd.DataFrame({"a": range(10)})
    path = tmp_path / "test_deprecate_gather_statistics.parquet"
    df.to_parquet(path, **engine)
    with pytest.warns(FutureWarning, match="deprecated"):
        out = dd.read_parquet(path, gather_statistics=True, **engine)
    assert_eq(out, df)


@pytest.mark.gpu
def test_gpu_write_parquet_simple(tmpdir):
    fn = str(tmpdir)
    cudf = pytest.importorskip("cudf")
    dask_cudf = pytest.importorskip("dask_cudf")
    from dask.dataframe.dispatch import pyarrow_schema_dispatch

    @pyarrow_schema_dispatch.register((cudf.DataFrame,))
    def get_pyarrow_schema_cudf(obj):
        return obj.to_arrow().schema

    df = cudf.DataFrame(
        {
            "a": ["abc", "def"],
            "b": ["a", "z"],
        }
    )
    ddf = dask_cudf.from_cudf(df, 3)
    ddf.to_parquet(fn)
    got = dask_cudf.read_parquet(fn)
    assert_eq(df, got)


@PYARROW_MARK
def test_retries_on_remote_filesystem(tmpdir):
    # Fake a remote filesystem with a cached one
    fn = str(tmpdir)
    remote_fn = f"simplecache://{tmpdir}"
    storage_options = {"target_protocol": "file"}

    df = pd.DataFrame({"a": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn)

    # Check that we set retries for reading and writing to parquet when not otherwise set
    scalar = ddf.to_parquet(remote_fn, compute=False, storage_options=storage_options)
    layer = hlg_layer(scalar.dask, "to-parquet")
    assert layer.annotations
    assert layer.annotations["retries"] == 5

    ddf2 = dd.read_parquet(remote_fn, storage_options=storage_options)
    layer = hlg_layer(ddf2.dask, "read-parquet")
    assert layer.annotations
    assert layer.annotations["retries"] == 5

    # But not for a local filesystem
    scalar = ddf.to_parquet(fn, compute=False, storage_options=storage_options)
    layer = hlg_layer(scalar.dask, "to-parquet")
    assert not layer.annotations

    ddf2 = dd.read_parquet(fn, storage_options=storage_options)
    layer = hlg_layer(ddf2.dask, "read-parquet")
    assert not layer.annotations

    # And we don't overwrite existing retries
    with dask.annotate(retries=2):
        scalar = ddf.to_parquet(
            remote_fn, compute=False, storage_options=storage_options
        )
        layer = hlg_layer(scalar.dask, "to-parquet")
        assert layer.annotations
        assert layer.annotations["retries"] == 2

        ddf2 = dd.read_parquet(remote_fn, storage_options=storage_options)
        layer = hlg_layer(ddf2.dask, "read-parquet")
        assert layer.annotations
        assert layer.annotations["retries"] == 2


@pytest.mark.parametrize("fs", ["fsspec", None])
def test_filesystem_option(tmp_path, engine, fs):
    from fsspec.implementations.local import LocalFileSystem

    df = pd.DataFrame({"a": range(10)})
    dd.from_pandas(df, npartitions=2).to_parquet(tmp_path, **engine)
    filesystem = fs or LocalFileSystem()
    ddf = dd.read_parquet(tmp_path, filesystem=filesystem, **engine)
    if fs is None:
        layer_fs = next(iter(ddf.dask.layers.values())).io_func.fs
        assert layer_fs is filesystem
    assert_eq(ddf, df)


@PYARROW_MARK
@pytest.mark.parametrize("fs", ["arrow", None])
def test_pyarrow_filesystem_option(tmp_path, fs):
    from fsspec.implementations.arrow import ArrowFSWrapper
    from pyarrow.fs import LocalFileSystem

    df = pd.DataFrame({"a": range(10)})
    fs = fs or LocalFileSystem()
    dd.from_pandas(df, npartitions=2).to_parquet(tmp_path, filesystem=fs)
    ddf = dd.read_parquet(tmp_path, filesystem=fs)
    layer_fs = next(iter(ddf.dask.layers.values())).io_func.fs
    assert isinstance(layer_fs, ArrowFSWrapper)
    assert isinstance(layer_fs.fs, LocalFileSystem)
    assert_eq(ddf, df)


@PYARROW_MARK
@pytest.mark.network
@pytest.mark.slow
def test_pyarrow_filesystem_option_real_data():
    # See: https://github.com/dask/dask/pull/10590
    dd.read_parquet(
        "s3://coiled-data/uber/part.0.parquet",
        filesystem="pyarrow",
        storage_options={"anonymous": True},
        blocksize=None,
    )


@PYARROW_MARK
def test_fsspec_to_parquet_filesystem_option(tmp_path):
    from fsspec import get_filesystem_class

    key1 = "/read1"
    key2 = str(tmp_path / "write1")

    df = pd.DataFrame({"a": range(10)})
    fs = get_filesystem_class("memory")(use_instance_cache=False)
    df.to_parquet(key1, filesystem=fs)

    # read in prepared data
    ddf = dd.read_parquet(key1, filesystem=fs)
    assert_eq(ddf, df)
    ddf.to_parquet(key2, filesystem=fs)

    # make sure we didn't write to local fs
    assert len(list(tmp_path.iterdir())) == 0, "wrote to local fs"

    # make sure we wrote a key to memory fs
    assert len(fs.ls(key2, detail=False)) == 1

    # ensure append functionality works
    ddf.to_parquet(key2, append=True, filesystem=fs)
    assert len(fs.ls(key2, detail=False)) == 2, "should have two parts"

    rddf = dd.read_parquet(key2, filesystem=fs)
    assert_eq(rddf, dd.concat([ddf, ddf]))


def test_select_filtered_column(tmp_path, engine):
    df = pd.DataFrame({"a": range(10), "b": ["cat"] * 10})
    path = tmp_path / "test_select_filtered_column.parquet"

    if engine == {}:  # pyarrow
        df.to_parquet(path, index=False, write_statistics=True)
    else:  # fastparquet
        df.to_parquet(path, index=False, stats=True, **engine)

    with pytest.warns(UserWarning, match="Sorted columns detected"):
        ddf = dd.read_parquet(path, filters=[("b", "==", "cat")], **engine)
        assert_eq(df, ddf)

    with pytest.warns(UserWarning, match="Sorted columns detected"):
        ddf = dd.read_parquet(path, filters=[("b", "is not", None)], **engine)
        assert_eq(df, ddf)


def test_select_filtered_column_no_stats(tmp_path, engine):
    df = pd.DataFrame({"a": range(10), "b": ["cat"] * 10})
    path = tmp_path / "test_select_filtered_column_no_stats.parquet"

    if engine == {}:  # pyarrow
        df.to_parquet(path, write_statistics=False)
    else:  # fastparquet
        df.to_parquet(path, stats=False, **engine)

    ddf = dd.read_parquet(path, filters=[("b", "==", "cat")], **engine)
    assert_eq(df, ddf)

    ddf = dd.read_parquet(path, filters=[("b", "is not", None)], **engine)
    assert_eq(df, ddf)


@PYARROW_MARK
@pytest.mark.parametrize("convert_string", [True, False])
@pytest.mark.skipif(
    not PANDAS_GE_200, reason="dataframe.convert-string requires pandas>=2.0"
)
def test_read_parquet_convert_string(tmp_path, convert_string, engine):
    df = pd.DataFrame(
        {"A": ["def", "abc", "ghi"], "B": [5, 2, 3], "C": ["x", "y", "z"]}
    ).set_index("C")

    outfile = tmp_path / "out.parquet"
    df.to_parquet(outfile, **engine)

    with dask.config.set({"dataframe.convert-string": convert_string}):
        ddf = dd.read_parquet(outfile, **engine)

    if convert_string and engine == {}:  # pyarrow
        expected = df.astype({"A": "string[pyarrow]"})
        expected.index = expected.index.astype("string[pyarrow]")
    else:
        expected = df
    assert_eq(ddf, expected)
    assert len(ddf.dask.layers) == 1

    # Test collection name takes into account `dataframe.convert-string`
    with dask.config.set({"dataframe.convert-string": convert_string}):
        ddf1 = dd.read_parquet(outfile)
    with dask.config.set({"dataframe.convert-string": not convert_string}):
        ddf2 = dd.read_parquet(outfile)

    assert ddf1._name != ddf2._name


@PYARROW_MARK
@pytest.mark.skipif(
    not PANDAS_GE_200, reason="dataframe.convert-string requires pandas>=2.0"
)
def test_read_parquet_convert_string_nullable_mapper(tmp_path, engine):
    """Make sure that when convert_string, dtype_backend and types_mapper are set,
    all three are used."""
    df = pd.DataFrame(
        {
            "A": pd.Series(["def", "abc", "ghi"], dtype="string"),
            "B": pd.Series([5, 2, 3], dtype="Int64"),
            "C": pd.Series([1.1, 6.3, 8.4], dtype="Float32"),
            "I": pd.Series(["x", "y", "z"], dtype="string"),
        }
    ).set_index("I")

    outfile = tmp_path / "out.parquet"
    df.to_parquet(outfile, **engine)

    types_mapper = {
        pa.float32(): pd.Float64Dtype(),
    }

    with dask.config.set({"dataframe.convert-string": True}):
        ddf = dd.read_parquet(
            tmp_path,
            dtype_backend="numpy_nullable",
            arrow_to_pandas={"types_mapper": types_mapper.get},
        )

    expected = df.astype(
        {
            "A": "string[pyarrow]",  # bc dataframe.convert-string=True
            "B": pd.Int64Dtype(),  # bc use_nullable_dtypes=Pandas
            "C": pd.Float64Dtype(),  # bc user mapper
        }
    )
    expected.index = expected.index.astype("string[pyarrow]")

    assert_eq(ddf, expected)


@PYARROW_MARK
@pytest.mark.parametrize("dtype_backend", ["numpy_nullable", "pyarrow"])
@pytest.mark.skipif(not PANDAS_GE_150, reason="Requires pyarrow-backed nullable dtypes")
def test_dtype_backend(tmp_path, dtype_backend, engine):
    """
    Test reading a parquet file without pandas metadata,
    but forcing use of nullable dtypes where appropriate
    """
    dtype_extra = "" if dtype_backend == "numpy_nullable" else "[pyarrow]"
    df = pd.DataFrame(
        {
            "a": pd.Series([1, 2, pd.NA, 3, 4], dtype=f"Int64{dtype_extra}"),
            "b": pd.Series(
                [True, pd.NA, False, True, False], dtype=f"boolean{dtype_extra}"
            ),
            "c": pd.Series([0.1, 0.2, 0.3, pd.NA, 0.4], dtype=f"Float64{dtype_extra}"),
            "d": pd.Series(["a", "b", "c", "d", pd.NA], dtype=f"string{dtype_extra}"),
        }
    )
    ddf = dd.from_pandas(df, npartitions=2)

    @dask.delayed
    def write_partition(df, i):
        """Write a parquet file without the pandas metadata"""
        table = pa.Table.from_pandas(df).replace_schema_metadata({})
        pq.write_table(table, tmp_path / f"part.{i}.parquet")

    # Create a pandas-metadata-free partitioned parquet. By default it will
    # not read into nullable extension dtypes
    partitions = ddf.to_delayed()
    dask.compute([write_partition(p, i) for i, p in enumerate(partitions)])

    # Not supported by fastparquet
    if engine == {"engine": "fastparquet"}:
        with pytest.raises(ValueError, match="`dtype_backend` is not supported"):
            dd.read_parquet(tmp_path, dtype_backend=dtype_backend, **engine)

    # Works in pyarrow
    else:
        ddf2 = dd.read_parquet(tmp_path, dtype_backend=dtype_backend, **engine)
        assert_eq(df, ddf2, check_index=False)


@PYARROW_MARK
@pytest.mark.skipif(
    not PANDAS_GE_200, reason="pd.Index does not support int32 before 2.0"
)
def test_read_parquet_preserve_categorical_column_dtype(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    outdir = tmp_path / "out.parquet"
    df.to_parquet(outdir, partition_cols=["a"])
    ddf = dd.read_parquet(outdir)

    expected = pd.DataFrame(
        {"b": ["x", "y"], "a": pd.Categorical(pd.Index([1, 2], dtype="int32"))},
        index=[0, 0],
    )
    assert_eq(ddf, expected)


@PYARROW_MARK
@pytest.mark.skipif(not PANDAS_GE_200, reason="Requires pd.ArrowDtype")
def test_dtype_backend_categoricals(tmp_path):
    df = pd.DataFrame({"a": pd.Series(["x", "y"], dtype="category"), "b": [1, 2]})
    outdir = tmp_path / "out.parquet"
    df.to_parquet(outdir)
    ddf = dd.read_parquet(outdir, dtype_backend="pyarrow")
    pdf = pd.read_parquet(outdir, dtype_backend="pyarrow")
    # Set sort_results=False because of pandas bug up to 2.0.1
    assert_eq(ddf, pdf, sort_results=PANDAS_GE_202)


@PYARROW_MARK
@pytest.mark.parametrize("filters", [None, [[("b", "==", "dog")]]])
def test_non_categorical_partitioning_pyarrow(tmpdir, filters):
    from pyarrow.dataset import partitioning as pd_partitioning

    df1 = pd.DataFrame({"a": range(100), "b": ["cat", "dog"] * 50})
    ddf1 = dd.from_pandas(df1, npartitions=2)
    ddf1.to_parquet(path=tmpdir, partition_on=["b"], write_index=False)

    schema = pa.schema([("b", pa.string())])
    partitioning = dict(flavor="hive", schema=schema)
    ddf = dd.read_parquet(
        tmpdir, dataset={"partitioning": partitioning}, filters=filters
    )
    pdf = pd.read_parquet(
        tmpdir, partitioning=pd_partitioning(**partitioning), filters=filters
    )
    assert_eq(ddf, pdf, check_index=False)
    assert ddf["b"].dtype != "category"


@PYARROW_MARK
def test_read_parquet_lists_not_converting(tmpdir):
    df = pd.DataFrame({"a": [1, 2], "b": [{"a": 2}, {"a": 4}]})
    df.to_parquet(tmpdir + "test.parquet")
    result = dd.read_parquet(tmpdir + "test.parquet").compute()
    assert_eq(df, result)
