import glob
import math
import os
import sys
import warnings
from decimal import Decimal
from distutils.version import LooseVersion

import numpy as np
import pandas as pd
import pytest

import dask
import dask.dataframe as dd
import dask.multiprocessing
from dask.dataframe._compat import PANDAS_GT_110, PANDAS_GT_121, PANDAS_GT_130
from dask.dataframe.io.parquet.core import ParquetSubgraph
from dask.dataframe.io.parquet.utils import _parse_pandas_metadata
from dask.dataframe.optimize import optimize_read_parquet_getitem
from dask.dataframe.utils import assert_eq
from dask.utils import natural_sort_key, parse_bytes

try:
    import fastparquet
except ImportError:
    fastparquet = False


try:
    import pyarrow as pa
except ImportError:
    check_pa_divs = pa = False

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = False


SKIP_FASTPARQUET = not fastparquet
SKIP_FASTPARQUET_REASON = "fastparquet not found"
FASTPARQUET_MARK = pytest.mark.skipif(SKIP_FASTPARQUET, reason=SKIP_FASTPARQUET_REASON)

if pq and pa.__version__ < LooseVersion("0.13.1"):
    SKIP_PYARROW = True
    SKIP_PYARROW_REASON = "pyarrow >= 0.13.1 required for parquet"
else:
    if (
        sys.platform == "win32"
        and pa
        and (
            (pa.__version__ == LooseVersion("0.16.0"))
            or (pa.__version__ == LooseVersion("2.0.0"))
        )
    ):
        SKIP_PYARROW = True
        SKIP_PYARROW_REASON = (
            "skipping pyarrow 0.16.0 and 2.0.0 on windows: "
            "https://github.com/dask/dask/issues/6093"
            "|https://github.com/dask/dask/issues/6754"
        )
    else:
        SKIP_PYARROW = not pq
        SKIP_PYARROW_REASON = "pyarrow not found"
PYARROW_MARK = pytest.mark.skipif(SKIP_PYARROW, reason=SKIP_PYARROW_REASON)

if pa and pa.__version__ < LooseVersion("1.0.0"):
    SKIP_PYARROW_DS = True
    SKIP_PYARROW_DS_REASON = "pyarrow >= 1.0.0 required for pyarrow dataset API"
else:
    SKIP_PYARROW_DS = SKIP_PYARROW
    SKIP_PYARROW_DS_REASON = "pyarrow not found"
PYARROW_DS_MARK = pytest.mark.skipif(SKIP_PYARROW_DS, reason=SKIP_PYARROW_DS_REASON)


def check_fastparquet():
    if SKIP_FASTPARQUET:
        pytest.skip(SKIP_FASTPARQUET_REASON)


def check_pyarrow():
    if SKIP_PYARROW:
        pytest.skip(SKIP_PYARROW_REASON)


def check_engine():
    if SKIP_FASTPARQUET and SKIP_PYARROW:
        pytest.skip("No parquet engine (fastparquet or pyarrow) found")


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


@pytest.fixture(
    params=[
        pytest.param("fastparquet", marks=FASTPARQUET_MARK),
        pytest.param("pyarrow-legacy", marks=PYARROW_MARK),
        pytest.param("pyarrow-dataset", marks=PYARROW_DS_MARK),
    ]
)
def engine(request):
    return request.param


def write_read_engines(**kwargs):
    """Product of both engines for write/read:

    To add custom marks, pass keyword of the form: `mark_writer_reader=reason`,
    or `mark_engine=reason` to apply to all parameters with that engine."""
    backends = {"pyarrow-dataset", "pyarrow-legacy", "fastparquet"}
    marks = {(w, r): [] for w in backends for r in backends}

    # Skip if uninstalled
    for name, skip, reason in [
        ("fastparquet", SKIP_FASTPARQUET, SKIP_FASTPARQUET_REASON),
        ("pyarrow-legacy", SKIP_PYARROW_DS, SKIP_PYARROW_DS_REASON),
        ("pyarrow-dataset", SKIP_PYARROW, SKIP_PYARROW_REASON),
    ]:
        if skip:
            val = pytest.mark.skip(reason=reason)
            for k in marks:
                if name in k:
                    marks[k].append(val)

    # Custom marks
    for kw, val in kwargs.items():
        kind, rest = kw.split("_", 1)
        key = tuple(rest.split("_"))
        if kind not in ("xfail", "skip") or len(key) > 2 or set(key) - backends:
            raise ValueError("unknown keyword %r" % kw)
        val = getattr(pytest.mark, kind)(reason=val)
        if len(key) == 2:
            marks[key].append(val)
        else:
            for k in marks:
                if key in k:
                    marks[k].append(val)

    return pytest.mark.parametrize(
        ("write_engine", "read_engine"),
        [pytest.param(*k, marks=tuple(v)) for (k, v) in sorted(marks.items())],
    )


pyarrow_fastparquet_msg = "fastparquet fails reading pyarrow written directories"
write_read_engines_xfail = write_read_engines(
    **{
        "xfail_pyarrow-dataset_fastparquet": pyarrow_fastparquet_msg,
        "xfail_pyarrow-legacy_fastparquet": pyarrow_fastparquet_msg,
    }
)

if (
    fastparquet
    and fastparquet.__version__ < LooseVersion("0.5")
    and PANDAS_GT_110
    and not PANDAS_GT_121
):
    # a regression in pandas 1.1.x / 1.2.0 caused a failure in writing partitioned
    # categorical columns when using fastparquet 0.4.x, but this was (accidentally)
    # fixed in fastparquet 0.5.0
    fp_pandas_msg = "pandas with fastparquet engine does not preserve index"
    fp_pandas_xfail = write_read_engines(
        **{
            "xfail_pyarrow-dataset_fastparquet": pyarrow_fastparquet_msg,
            "xfail_pyarrow-legacy_fastparquet": pyarrow_fastparquet_msg,
            "xfail_fastparquet_fastparquet": fp_pandas_msg,
            "xfail_fastparquet_pyarrow-dataset": fp_pandas_msg,
            "xfail_fastparquet_pyarrow-legacy": fp_pandas_msg,
        }
    )
else:
    fp_pandas_msg = "pandas with fastparquet engine does not preserve index"
    fp_pandas_xfail = write_read_engines()


@write_read_engines()
def test_local(tmpdir, write_engine, read_engine):
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

    df.to_parquet(tmp, write_index=False, engine=write_engine)

    files = os.listdir(tmp)
    assert "_common_metadata" in files
    assert "_metadata" in files
    assert "part.0.parquet" in files

    df2 = dd.read_parquet(tmp, index=False, engine=read_engine)

    assert len(df2.divisions) > 1

    out = df2.compute(scheduler="sync").reset_index()

    for column in df.columns:
        assert (data[column] == out[column]).all()


@pytest.mark.parametrize("index", [False, True])
@write_read_engines_xfail
def test_empty(tmpdir, write_engine, read_engine, index):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})[:0]
    if index:
        df.set_index("a", inplace=True, drop=True)
    ddf = dd.from_pandas(df, npartitions=2)

    ddf.to_parquet(fn, write_index=index, engine=write_engine)
    read_df = dd.read_parquet(fn, engine=read_engine)
    assert_eq(ddf, read_df)


@write_read_engines()
def test_simple(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    if write_engine != "fastparquet":
        df = pd.DataFrame({"a": [b"a", b"b", b"b"], "b": [4, 5, 6]})
    else:
        df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})
    df.set_index("a", inplace=True, drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, engine=write_engine)
    read_df = dd.read_parquet(fn, index=["a"], engine=read_engine)
    assert_eq(ddf, read_df)


@write_read_engines()
def test_delayed_no_metadata(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["a", "b", "b"], "b": [4, 5, 6]})
    df.set_index("a", inplace=True, drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(
        fn, engine=write_engine, compute=False, write_metadata_file=False
    ).compute()
    files = os.listdir(fn)
    assert "_metadata" not in files
    # Fastparquet doesn't currently handle a directory without "_metadata"
    read_df = dd.read_parquet(
        os.path.join(fn, "*.parquet"),
        index=["a"],
        engine=read_engine,
        gather_statistics=True,
    )
    assert_eq(ddf, read_df)


@write_read_engines()
def test_read_glob(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, engine=write_engine)
    if os.path.exists(os.path.join(tmp_path, "_metadata")):
        os.unlink(os.path.join(tmp_path, "_metadata"))
    files = os.listdir(tmp_path)
    assert "_metadata" not in files

    ddf2 = dd.read_parquet(
        os.path.join(tmp_path, "*.parquet"),
        engine=read_engine,
        index="myindex",  # Must specify index without _metadata
        gather_statistics=True,
    )
    assert_eq(ddf, ddf2)


@write_read_engines()
def test_read_list(tmpdir, write_engine, read_engine):
    if write_engine == read_engine == "fastparquet" and os.name == "nt":
        # fastparquet or dask is not normalizing filepaths correctly on
        # windows.
        pytest.skip("filepath bug.")

    tmpdir = str(tmpdir)
    ddf.to_parquet(tmpdir, engine=write_engine)
    files = sorted(
        [
            os.path.join(tmpdir, f)
            for f in os.listdir(tmpdir)
            if not f.endswith("_metadata")
        ],
        key=natural_sort_key,
    )

    ddf2 = dd.read_parquet(
        files, engine=read_engine, index="myindex", gather_statistics=True
    )
    assert_eq(ddf, ddf2)


@write_read_engines()
def test_columns_auto_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=write_engine)

    # XFAIL, auto index selection no longer supported (for simplicity)
    # ### Empty columns ###
    # With divisions if supported
    assert_eq(dd.read_parquet(fn, columns=[], engine=read_engine), ddf[[]])

    # No divisions
    assert_eq(
        dd.read_parquet(fn, columns=[], engine=read_engine, gather_statistics=False),
        ddf[[]].clear_divisions(),
        check_divisions=True,
    )

    # ### Single column, auto select index ###
    # With divisions if supported
    assert_eq(dd.read_parquet(fn, columns=["x"], engine=read_engine), ddf[["x"]])

    # No divisions
    assert_eq(
        dd.read_parquet(fn, columns=["x"], engine=read_engine, gather_statistics=False),
        ddf[["x"]].clear_divisions(),
        check_divisions=True,
    )


@write_read_engines()
def test_columns_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=write_engine)

    # With Index
    # ----------
    # ### Empty columns, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(fn, columns=[], engine=read_engine, index="myindex"), ddf[[]]
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn, columns=[], engine=read_engine, index="myindex", gather_statistics=False
        ),
        ddf[[]].clear_divisions(),
        check_divisions=True,
    )

    # ### Single column, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(fn, index="myindex", columns=["x"], engine=read_engine),
        ddf[["x"]],
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x"],
            engine=read_engine,
            gather_statistics=False,
        ),
        ddf[["x"]].clear_divisions(),
        check_divisions=True,
    )

    # ### Two columns, specify index ###
    # With divisions if supported
    assert_eq(
        dd.read_parquet(fn, index="myindex", columns=["x", "y"], engine=read_engine),
        ddf,
    )

    # No divisions
    assert_eq(
        dd.read_parquet(
            fn,
            index="myindex",
            columns=["x", "y"],
            engine=read_engine,
            gather_statistics=False,
        ),
        ddf.clear_divisions(),
        check_divisions=True,
    )


def test_nonsense_column(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=engine)
    with pytest.raises((ValueError, KeyError)):
        dd.read_parquet(fn, columns=["nonesense"], engine=engine)
    with pytest.raises((Exception, KeyError)):
        dd.read_parquet(fn, columns=["nonesense"] + list(ddf.columns), engine=engine)


@write_read_engines()
def test_columns_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=write_engine)
    ddf2 = ddf.reset_index()

    # No Index
    # --------
    # All columns, none as index
    assert_eq(
        dd.read_parquet(fn, index=False, engine=read_engine, gather_statistics=True),
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
            engine=read_engine,
            gather_statistics=True,
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
            engine=read_engine,
            gather_statistics=True,
        ),
        ddf2[["myindex", "x"]],
        check_index=False,
        check_divisions=True,
    )


@write_read_engines()
def test_gather_statistics_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=write_engine, write_index=False)

    df = dd.read_parquet(fn, engine=read_engine, index=False)
    assert df.index.name is None
    assert not df.known_divisions


def test_columns_index_with_multi_index(tmpdir, engine):
    fn = os.path.join(str(tmpdir), "test.parquet")
    index = pd.MultiIndex.from_arrays(
        [np.arange(10), np.arange(10) + 1], names=["x0", "x1"]
    )
    df = pd.DataFrame(np.random.randn(10, 2), columns=["a", "b"], index=index)
    df2 = df.reset_index(drop=False)

    if engine == "fastparquet":
        fastparquet.write(fn, df.reset_index(), write_index=False)

        # fastparquet doesn't support multi-index
        with pytest.raises(ValueError):
            ddf = dd.read_parquet(fn, engine=engine, index=index.names)

    else:
        pq.write_table(pa.Table.from_pandas(df.reset_index(), preserve_index=False), fn)

        # Pyarrow supports multi-index reads
        ddf = dd.read_parquet(fn, engine=engine, index=index.names)
        assert_eq(ddf, df)

        d = dd.read_parquet(fn, columns="a", engine=engine, index=index.names)
        assert_eq(d, df["a"])

        d = dd.read_parquet(fn, index=["a", "b"], columns=["x0", "x1"], engine=engine)
        assert_eq(d, df2.set_index(["a", "b"])[["x0", "x1"]])

    # Just index
    d = dd.read_parquet(fn, index=False, engine=engine)
    assert_eq(d, df2)

    d = dd.read_parquet(fn, columns=["b"], index=["a"], engine=engine)
    assert_eq(d, df2.set_index("a")[["b"]])

    d = dd.read_parquet(fn, columns=["a", "b"], index=["x0"], engine=engine)
    assert_eq(d, df2.set_index("x0")[["a", "b"]])

    # Just columns
    d = dd.read_parquet(fn, columns=["x0", "a"], index=["x1"], engine=engine)
    assert_eq(d, df2.set_index("x1")[["x0", "a"]])

    # Both index and columns
    d = dd.read_parquet(fn, index=False, columns=["x0", "b"], engine=engine)
    assert_eq(d, df2[["x0", "b"]])

    for index in ["x1", "b"]:
        d = dd.read_parquet(fn, index=index, columns=["x0", "a"], engine=engine)
        assert_eq(d, df2.set_index(index)[["x0", "a"]])

    # Columns and index intersect
    for index in ["a", "x0"]:
        with pytest.raises(ValueError):
            d = dd.read_parquet(fn, index=index, columns=["x0", "a"], engine=engine)

    # Series output
    for ind, col, sol_df in [
        ("x1", "x0", df2.set_index("x1")),
        (False, "b", df2),
        (False, "x0", df2[["x0"]]),
        ("a", "x0", df2.set_index("a")[["x0"]]),
        ("a", "b", df2.set_index("a")),
    ]:
        d = dd.read_parquet(fn, index=ind, columns=col, engine=engine)
        assert_eq(d, sol_df[col])


@write_read_engines()
def test_no_index(tmpdir, write_engine, read_engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, engine=write_engine)
    ddf2 = dd.read_parquet(fn, engine=read_engine)
    assert_eq(df, ddf2, check_index=False)


def test_read_series(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=engine)
    ddf2 = dd.read_parquet(fn, columns=["x"], index="myindex", engine=engine)
    assert_eq(ddf[["x"]], ddf2)

    ddf2 = dd.read_parquet(fn, columns="x", index="myindex", engine=engine)
    assert_eq(ddf.x, ddf2)


def test_names(tmpdir, engine):
    fn = str(tmpdir)
    ddf.to_parquet(fn, engine=engine)

    def read(fn, **kwargs):
        return dd.read_parquet(fn, engine=engine, **kwargs)

    assert set(read(fn).dask) == set(read(fn).dask)

    assert set(read(fn).dask) != set(read(fn, columns=["x"]).dask)

    assert set(read(fn, columns=("x",)).dask) == set(read(fn, columns=["x"]).dask)


@write_read_engines()
def test_roundtrip_from_pandas(tmpdir, write_engine, read_engine):
    fn = str(tmpdir.join("test.parquet"))
    dfp = df.copy()
    dfp.index.name = "index"
    dfp.to_parquet(
        fn, engine="pyarrow" if write_engine.startswith("pyarrow") else "fastparquet"
    )
    ddf = dd.read_parquet(fn, index="index", engine=read_engine)
    assert_eq(dfp, ddf)


@write_read_engines()
def test_categorical(tmpdir, write_engine, read_engine):
    tmp = str(tmpdir)
    df = pd.DataFrame({"x": ["a", "b", "c"] * 100}, dtype="category")
    ddf = dd.from_pandas(df, npartitions=3)
    dd.to_parquet(ddf, tmp, engine=write_engine)

    ddf2 = dd.read_parquet(tmp, categories="x", engine=read_engine)
    assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

    ddf2 = dd.read_parquet(tmp, categories=["x"], engine=read_engine)
    assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

    # autocat
    if read_engine == "fastparquet":
        ddf2 = dd.read_parquet(tmp, engine=read_engine)
        assert ddf2.compute().x.cat.categories.tolist() == ["a", "b", "c"]

        ddf2.loc[:1000].compute()
        assert assert_eq(df, ddf2)

    # dereference cats
    ddf2 = dd.read_parquet(tmp, categories=[], engine=read_engine)

    ddf2.loc[:1000].compute()
    assert (df.x == ddf2.x.compute()).all()


def test_append(tmpdir, engine):
    """Test that appended parquet equal to the original one."""
    check_fastparquet()
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
    ddf1.to_parquet(tmp, engine=engine)
    ddf2.to_parquet(tmp, append=True, engine=engine)

    ddf3 = dd.read_parquet(tmp, engine=engine)
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
    ddf1.to_parquet(tmp_path, append=True, engine=engine)
    ddf2.to_parquet(tmp_path, append=True, engine=engine)

    ddf3 = dd.read_parquet(tmp_path, engine=engine)
    assert_eq(df, ddf3)


def test_append_with_partition(tmpdir, engine):
    # check_fastparquet()
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
    dd_df0 = dd.from_pandas(df0, npartitions=1)
    dd_df1 = dd.from_pandas(df1, npartitions=1)
    dd.to_parquet(dd_df0, tmp, partition_on=["lon"], engine=engine)
    dd.to_parquet(
        dd_df1,
        tmp,
        partition_on=["lon"],
        append=True,
        ignore_divisions=True,
        engine=engine,
    )

    out = dd.read_parquet(
        tmp, engine=engine, index="index", gather_statistics=True
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
    d.to_parquet(tmp, partition_on=["b"], engine=engine)
    df = dd.read_parquet(tmp, engine=engine)
    assert set(df.b.cat.categories) == {"x", "y", "z"}


@pytest.mark.parametrize("meta", [False, True])
@pytest.mark.parametrize("stats", [False, True])
def test_partition_on_cats_pyarrow(tmpdir, stats, meta):
    check_pyarrow()

    tmp = str(tmpdir)
    d = pd.DataFrame(
        {
            "a": np.random.rand(50),
            "b": np.random.choice(["x", "y", "z"], size=50),
            "c": np.random.choice(["x", "y", "z"], size=50),
        }
    )
    d = dd.from_pandas(d, 2)
    d.to_parquet(tmp, partition_on=["b"], engine="pyarrow", write_metadata_file=meta)
    df = dd.read_parquet(tmp, engine="pyarrow", gather_statistics=stats)
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
    d.to_parquet(tmp, partition_on=["b", "c"], engine=engine)
    df = dd.read_parquet(tmp, engine=engine)
    assert set(df.b.cat.categories) == {"x", "y", "z"}
    assert set(df.c.cat.categories) == {"x", "y", "z"}

    df = dd.read_parquet(tmp, columns=["a", "c"], engine=engine)
    assert set(df.c.cat.categories) == {"x", "y", "z"}
    assert "b" not in df.columns
    assert_eq(df, df.compute())
    df = dd.read_parquet(tmp, index="c", engine=engine)
    assert set(df.index.categories) == {"x", "y", "z"}
    assert "c" not in df.columns
    # series
    df = dd.read_parquet(tmp, columns="b", engine=engine)
    assert set(df.cat.categories) == {"x", "y", "z"}


def test_append_wo_index(tmpdir, engine):
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
    ddf1.to_parquet(tmp, engine=engine)

    with pytest.raises(ValueError) as excinfo:
        ddf2.to_parquet(tmp, write_index=False, append=True, engine=engine)
    assert "Appended columns" in str(excinfo.value)

    tmp = str(tmpdir.join("tmp2.parquet"))
    ddf1.to_parquet(tmp, write_index=False, engine=engine)
    ddf2.to_parquet(tmp, write_index=False, append=True, engine=engine)

    ddf3 = dd.read_parquet(tmp, index="f", engine=engine)
    assert_eq(df.set_index("f"), ddf3)


def test_append_overlapping_divisions(tmpdir, engine):
    """Test raising of error when divisions overlapping."""
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
    half = len(df) // 2
    ddf1 = dd.from_pandas(df.iloc[:half], chunksize=100)
    ddf2 = dd.from_pandas(df.iloc[half - 10 :], chunksize=100)
    ddf1.to_parquet(tmp, engine=engine)

    with pytest.raises(ValueError) as excinfo:
        ddf2.to_parquet(tmp, engine=engine, append=True)
    assert "Appended divisions" in str(excinfo.value)

    ddf2.to_parquet(tmp, engine=engine, append=True, ignore_divisions=True)


def test_append_different_columns(tmpdir, engine):
    """Test raising of error when non equal columns."""
    tmp = str(tmpdir)
    df1 = pd.DataFrame({"i32": np.arange(100, dtype=np.int32)})
    df2 = pd.DataFrame({"i64": np.arange(100, dtype=np.int64)})
    df3 = pd.DataFrame({"i32": np.arange(100, dtype=np.int64)})

    ddf1 = dd.from_pandas(df1, chunksize=2)
    ddf2 = dd.from_pandas(df2, chunksize=2)
    ddf3 = dd.from_pandas(df3, chunksize=2)

    ddf1.to_parquet(tmp, engine=engine)

    with pytest.raises(ValueError) as excinfo:
        ddf2.to_parquet(tmp, engine=engine, append=True)
    assert "Appended columns" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        ddf3.to_parquet(tmp, engine=engine, append=True)
    assert "Appended dtypes" in str(excinfo.value)


@write_read_engines_xfail
def test_ordering(tmpdir, write_engine, read_engine):
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]},
        index=pd.Index([-1, -2, -3], name="myindex"),
        columns=["c", "a", "b"],
    )
    ddf = dd.from_pandas(df, npartitions=2)
    dd.to_parquet(ddf, tmp, engine=write_engine)

    if read_engine == "fastparquet":
        pf = fastparquet.ParquetFile(tmp)
        assert pf.columns == ["myindex", "c", "a", "b"]

    ddf2 = dd.read_parquet(tmp, index="myindex", engine=read_engine)
    assert_eq(ddf, ddf2, check_divisions=False)


def test_read_parquet_custom_columns(tmpdir, engine):
    tmp = str(tmpdir)
    data = pd.DataFrame(
        {"i32": np.arange(1000, dtype=np.int32), "f": np.arange(1000, dtype=np.float64)}
    )
    df = dd.from_pandas(data, chunksize=50)
    df.to_parquet(tmp, engine=engine)

    df2 = dd.read_parquet(tmp, columns=["i32", "f"], engine=engine)
    assert_eq(df[["i32", "f"]], df2, check_index=False)

    fns = glob.glob(os.path.join(tmp, "*.parquet"))
    df2 = dd.read_parquet(fns, columns=["i32"], engine=engine).compute()
    df2.sort_values("i32", inplace=True)
    assert_eq(df[["i32"]], df2, check_index=False, check_divisions=False)

    df3 = dd.read_parquet(tmp, columns=["f", "i32"], engine=engine)
    assert_eq(df[["f", "i32"]], df3, check_index=False)


@pytest.mark.parametrize(
    "df,write_kwargs,read_kwargs",
    [
        (pd.DataFrame({"x": [3, 2, 1]}), {}, {}),
        (pd.DataFrame({"x": ["c", "a", "b"]}), {}, {}),
        (pd.DataFrame({"x": ["cc", "a", "bbb"]}), {}, {}),
        (pd.DataFrame({"x": [b"a", b"b", b"c"]}), {"object_encoding": "bytes"}, {}),
        (
            pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])}),
            {},
            {"categories": ["x"]},
        ),
        (pd.DataFrame({"x": pd.Categorical([1, 2, 1])}), {}, {"categories": ["x"]}),
        (pd.DataFrame({"x": list(map(pd.Timestamp, [3000, 2000, 1000]))}), {}, {}),
        (pd.DataFrame({"x": [3000, 2000, 1000]}).astype("M8[ns]"), {}, {}),
        pytest.param(
            pd.DataFrame({"x": [3, 2, 1]}).astype("M8[ns]"),
            {},
            {},
            marks=pytest.mark.xfail(
                reason="Parquet doesn't support nanosecond precision"
            ),
        ),
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
def test_roundtrip(tmpdir, df, write_kwargs, read_kwargs, engine):
    if (
        PANDAS_GT_130
        and engine == "fastparquet"
        and read_kwargs.get("categories", None)
    ):
        pytest.xfail("https://github.com/dask/fastparquet/issues/577")

    tmp = str(tmpdir)
    if df.index.name is None:
        df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)

    oe = write_kwargs.pop("object_encoding", None)
    if oe and engine == "fastparquet":
        dd.to_parquet(ddf, tmp, engine=engine, object_encoding=oe, **write_kwargs)
    else:
        dd.to_parquet(ddf, tmp, engine=engine, **write_kwargs)
    ddf2 = dd.read_parquet(tmp, index=df.index.name, engine=engine, **read_kwargs)
    assert_eq(ddf, ddf2)


def test_categories(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": list("caaab")})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf["y"] = ddf.y.astype("category")
    ddf.to_parquet(fn, engine=engine)
    ddf2 = dd.read_parquet(fn, categories=["y"], engine=engine)

    # Shouldn't need to specify categories explicitly
    ddf3 = dd.read_parquet(fn, engine=engine)
    assert_eq(ddf3, ddf2)

    with pytest.raises(NotImplementedError):
        ddf2.y.cat.categories
    assert set(ddf2.y.compute().cat.categories) == {"a", "b", "c"}
    cats_set = ddf2.map_partitions(lambda x: x.y.cat.categories.sort_values()).compute()
    assert cats_set.tolist() == ["a", "c", "a", "b"]

    if engine == "fastparquet":
        assert_eq(ddf.y, ddf2.y, check_names=False)
        with pytest.raises(TypeError):
            # attempt to load as category that which is not so encoded
            ddf2 = dd.read_parquet(fn, categories=["x"], engine=engine).compute()

    with pytest.raises((ValueError, FutureWarning)):
        # attempt to load as category unknown column
        ddf2 = dd.read_parquet(fn, categories=["foo"], engine=engine)


def test_categories_unnamed_index(tmpdir, engine):
    # Check that we can handle an unnamed categorical index
    # https://github.com/dask/dask/issues/6885

    if engine.startswith("pyarrow") and pa.__version__ < LooseVersion("0.15.0"):
        pytest.skip("PyArrow>=0.15 Required.")

    tmpdir = str(tmpdir)

    df = pd.DataFrame(
        data={"A": [1, 2, 3], "B": ["a", "a", "b"]}, index=["x", "y", "y"]
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = ddf.categorize(columns=["B"])

    ddf.to_parquet(tmpdir, engine=engine)
    ddf2 = dd.read_parquet(tmpdir, engine=engine)

    assert_eq(ddf.index, ddf2.index, check_divisions=False)


def test_empty_partition(tmpdir, engine):
    fn = str(tmpdir)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    ddf = dd.from_pandas(df, npartitions=5)

    ddf2 = ddf[ddf.a <= 5]
    ddf2.to_parquet(fn, engine=engine)

    ddf3 = dd.read_parquet(fn, engine=engine)
    assert ddf3.npartitions < 5
    sol = ddf2.compute()
    assert_eq(sol, ddf3, check_names=False, check_index=False)


def test_timestamp_index(tmpdir, engine):
    fn = str(tmpdir)
    df = dd._compat.makeTimeDataFrame()
    df.index.name = "foo"
    ddf = dd.from_pandas(df, npartitions=5)
    ddf.to_parquet(fn, engine=engine)
    ddf2 = dd.read_parquet(fn, engine=engine)
    assert_eq(ddf, ddf2)


def test_to_parquet_default_writes_nulls(tmpdir):
    check_fastparquet()
    check_pyarrow()
    fn = str(tmpdir.join("test.parquet"))

    df = pd.DataFrame({"c1": [1.0, np.nan, 2, np.nan, 3]})
    ddf = dd.from_pandas(df, npartitions=1)

    ddf.to_parquet(fn)
    table = pq.read_table(fn)
    assert table[1].null_count == 2


def test_to_parquet_pyarrow_w_inconsistent_schema_by_partition_fails_by_default(tmpdir):
    check_pyarrow()

    df = pd.DataFrame(
        {"partition_column": [0, 0, 1, 1], "strings": ["a", "b", None, None]}
    )

    ddf = dd.from_pandas(df, npartitions=2)
    # In order to allow pyarrow to write an inconsistent schema,
    # we need to avoid writing the _metadata file (will fail >0.17.1)
    # and need to avoid schema inference (i.e. use `schema=None`)
    ddf.to_parquet(
        str(tmpdir),
        engine="pyarrow",
        partition_on=["partition_column"],
        write_metadata_file=False,
        schema=None,
    )

    # Test that schema is not validated by default
    # (shouldn't raise error with legacy dataset)
    dd.read_parquet(
        str(tmpdir),
        engine="pyarrow-legacy",
        gather_statistics=False,
    ).compute()

    # Test that read fails when validate_schema=True
    # Note: This fails differently for pyarrow.dataset api
    with pytest.raises(ValueError) as e_info:
        dd.read_parquet(
            str(tmpdir),
            engine="pyarrow-legacy",
            gather_statistics=False,
            dataset={"validate_schema": True},
        ).compute()
        assert e_info.message.contains("ValueError: Schema in partition")
        assert e_info.message.contains("was different")


def test_to_parquet_pyarrow_w_inconsistent_schema_by_partition_succeeds_w_manual_schema(
    tmpdir,
):
    check_pyarrow()

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
    ddf.to_parquet(
        str(tmpdir), engine="pyarrow", partition_on="partition_column", schema=schema
    )
    ddf_after_write = (
        dd.read_parquet(str(tmpdir), engine="pyarrow", gather_statistics=False)
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


@pytest.mark.parametrize("index", [False, True])
@pytest.mark.parametrize("schema", ["infer", "complex"])
def test_pyarrow_schema_inference(tmpdir, index, engine, schema):

    check_pyarrow()
    if pa.__version__ < LooseVersion("0.15.0"):
        pytest.skip("PyArrow>=0.15 Required.")
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

    df.to_parquet(tmpdir, engine="pyarrow", schema=schema)
    df_out = dd.read_parquet(tmpdir, engine=engine)
    df_out.compute()

    if index and engine == "fastparquet":
        # Fastparquet fails to detect int64 from _metadata
        df_out["amount"] = df_out["amount"].astype("int64")

        # Fastparquet not handling divisions for
        # pyarrow-written dataset with string index
        assert_eq(df, df_out, check_divisions=False)
    else:
        assert_eq(df, df_out)


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
    d.to_parquet(tmpdir, partition_on=["a1", "a2"], engine=engine)
    # Note #1: Cross-engine functionality is missing
    # Note #2: The index is not preserved in pyarrow when partition_on is used
    out = dd.read_parquet(
        tmpdir, engine=engine, index=False, gather_statistics=False
    ).compute()
    for val in df.a1.unique():
        assert set(df.b[df.a1 == val]) == set(out.b[out.a1 == val])

    # Now specify the columns and allow auto-index detection
    out = dd.read_parquet(tmpdir, engine=engine, columns=["b", "a2"]).compute()
    for val in df.a2.unique():
        assert set(df.b[df.a2 == val]) == set(out.b[out.a2 == val])


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
        d.to_parquet(tmpdir, partition_on=["a1", "a2"], engine=engine)

    out = dd.read_parquet(tmpdir, engine=engine).compute()

    assert len(df) == len(out)
    for root, dirs, files in os.walk(tmpdir):
        for file in files:
            assert file in (
                "part.0.parquet",
                "part.1.parquet",
                "_common_metadata",
                "_metadata",
            )


@pytest.mark.parametrize("partition_on", ["aa", ["aa"]])
def test_partition_on_string(tmpdir, partition_on):
    tmpdir = str(tmpdir)
    check_pyarrow()
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
        d.to_parquet(
            tmpdir, partition_on=partition_on, write_index=False, engine="pyarrow"
        )
        out = dd.read_parquet(
            tmpdir, index=False, gather_statistics=False, engine="pyarrow"
        )
    out = out.compute()
    for val in df.aa.unique():
        assert set(df.bb[df.aa == val]) == set(out.bb[out.aa == val])


@write_read_engines()
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
    ddftest.to_parquet(tmpdir, partition_on="DatePart", engine=write_engine)
    ddftest_read = dd.read_parquet(
        tmpdir,
        index="dummy",
        engine=read_engine,
        filters=[(("DatePart", "<=", "2018-01-02"))],
    )
    assert len(ddftest_read) == 2


@write_read_engines()
def test_filters(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    df = pd.DataFrame({"x": range(10), "y": list("aabbccddee")})
    ddf = dd.from_pandas(df, npartitions=5)
    assert ddf.npartitions == 5

    ddf.to_parquet(tmp_path, engine=write_engine)

    a = dd.read_parquet(tmp_path, engine=read_engine, filters=[("x", ">", 4)])
    assert a.npartitions == 3
    assert (a.x > 3).all().compute()

    b = dd.read_parquet(tmp_path, engine=read_engine, filters=[("y", "==", "c")])
    assert b.npartitions == 1
    assert (b.y == "c").all().compute()

    c = dd.read_parquet(
        tmp_path, engine=read_engine, filters=[("y", "==", "c"), ("x", ">", 6)]
    )
    assert c.npartitions <= 1
    assert not len(c)
    assert_eq(c, c)

    d = dd.read_parquet(
        tmp_path,
        engine=read_engine,
        filters=[
            # Select two overlapping ranges
            [("x", ">", 1), ("x", "<", 6)],
            [("x", ">", 3), ("x", "<", 8)],
        ],
    )
    assert d.npartitions == 3
    assert ((d.x > 1) & (d.x < 8)).all().compute()

    e = dd.read_parquet(tmp_path, engine=read_engine, filters=[("x", "in", (0, 9))])
    assert e.npartitions == 2
    assert ((e.x < 2) | (e.x > 7)).all().compute()


@write_read_engines()
def test_filters_v0(tmpdir, write_engine, read_engine):
    if write_engine == "fastparquet" or read_engine == "fastparquet":
        pytest.importorskip("fastparquet", minversion="0.3.1")

    # Recent versions of pyarrow support full row-wise filtering
    # (fastparquet and older pyarrow versions do not)
    pyarrow_row_filtering = (
        read_engine == "pyarrow-dataset" and pa.__version__ >= LooseVersion("1.0.0")
    )

    fn = str(tmpdir)
    df = pd.DataFrame({"at": ["ab", "aa", "ba", "da", "bb"]})
    ddf = dd.from_pandas(df, npartitions=1)

    # Ok with 1 partition and filters
    ddf.repartition(npartitions=1, force=True).to_parquet(
        fn, write_index=False, engine=write_engine
    )
    ddf2 = dd.read_parquet(
        fn, index=False, engine=read_engine, filters=[("at", "==", "aa")]
    ).compute()
    if pyarrow_row_filtering:
        assert_eq(ddf2, ddf[ddf["at"] == "aa"], check_index=False)
    else:
        assert_eq(ddf2, ddf)

    # with >1 partition and no filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn, engine=write_engine)
    ddf2 = dd.read_parquet(fn, engine=read_engine).compute()
    assert_eq(ddf2, ddf)

    # with >1 partition and filters using base fastparquet
    if read_engine == "fastparquet":
        ddf.repartition(npartitions=2, force=True).to_parquet(fn, engine=write_engine)
        df2 = fastparquet.ParquetFile(fn).to_pandas(filters=[("at", "==", "aa")])
        assert len(df2) > 0

    # with >1 partition and filters
    ddf.repartition(npartitions=2, force=True).to_parquet(fn, engine=write_engine)
    ddf2 = dd.read_parquet(
        fn, engine=read_engine, filters=[("at", "==", "aa")]
    ).compute()
    assert len(ddf2) > 0


def test_filtering_pyarrow_dataset(tmpdir, engine):
    pytest.importorskip("pyarrow", minversion="1.0.0")

    fn = str(tmpdir)
    df = pd.DataFrame({"aa": range(100), "bb": ["cat", "dog"] * 50})
    ddf = dd.from_pandas(df, npartitions=10)
    ddf.to_parquet(fn, write_index=False, engine=engine)

    # Filtered read
    aa_lim = 40
    bb_val = "dog"
    filters = [[("aa", "<", aa_lim), ("bb", "==", bb_val)]]
    ddf2 = dd.read_parquet(fn, index=False, engine="pyarrow-dataset", filters=filters)

    # Check that partitions are filetered for "aa" filter
    nonempty = 0
    for part in ddf[ddf["aa"] < aa_lim].partitions:
        nonempty += int(len(part.compute()) > 0)
    assert ddf2.npartitions == nonempty

    # Check that rows are filtered for "aa" and "bb" filters
    df = df[df["aa"] < aa_lim]
    df = df[df["bb"] == bb_val]
    assert_eq(df, ddf2.compute(), check_index=False)


def test_fiters_file_list(tmpdir, engine):
    df = pd.DataFrame({"x": range(10), "y": list("aabbccddee")})
    ddf = dd.from_pandas(df, npartitions=5)

    ddf.to_parquet(str(tmpdir), engine=engine)
    fils = str(tmpdir.join("*.parquet"))
    ddf_out = dd.read_parquet(
        fils, gather_statistics=True, engine=engine, filters=[("x", ">", 3)]
    )

    assert ddf_out.npartitions == 3
    assert_eq(df[df["x"] > 3], ddf_out.compute(), check_index=False)

    # Check that first parition gets filtered for single-path input
    ddf2 = dd.read_parquet(
        str(tmpdir.join("part.0.parquet")),
        gather_statistics=True,
        engine=engine,
        filters=[("x", ">", 3)],
    )
    assert len(ddf2) == 0


def test_pyarrow_filter_divisions(tmpdir):
    pytest.importorskip("pyarrow")

    # Write simple dataset with an index that will only
    # have a sorted index if certain row-groups are filtered out.
    # In this case, we filter "a" <= 3 to get a sorted
    # index. Otherwise, "a" is NOT monotonically increasing.
    df = pd.DataFrame({"a": [0, 1, 10, 12, 2, 3, 8, 9], "b": range(8)}).set_index("a")
    df.iloc[:4].to_parquet(
        str(tmpdir.join("file.0.parquet")), engine="pyarrow", row_group_size=2
    )
    df.iloc[4:].to_parquet(
        str(tmpdir.join("file.1.parquet")), engine="pyarrow", row_group_size=2
    )

    if pa.__version__ >= LooseVersion("1.0.0"):
        # Only works for ArrowDatasetEngine.
        # Legacy code will not apply filters on individual row-groups
        # when `split_row_groups=False`.
        ddf = dd.read_parquet(
            str(tmpdir),
            engine="pyarrow-dataset",
            split_row_groups=False,
            gather_statistics=True,
            filters=[("a", "<=", 3)],
        )
        assert ddf.divisions == (0, 2, 3)

    ddf = dd.read_parquet(
        str(tmpdir),
        engine="pyarrow-dataset",
        split_row_groups=True,
        gather_statistics=True,
        filters=[("a", "<=", 3)],
    )
    assert ddf.divisions == (0, 2, 3)


def test_divisions_read_with_filters(tmpdir):
    pytest.importorskip("fastparquet", minversion="0.3.1")
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
    out = dd.read_parquet(tmpdir, engine="fastparquet", filters=[("a", "==", "b")])
    # test it
    expected_divisions = (25, 49)
    assert out.divisions == expected_divisions


def test_divisions_are_known_read_with_filters(tmpdir):
    pytest.importorskip("fastparquet", minversion="0.3.1")
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
    out = dd.read_parquet(tmpdir, engine="fastparquet", filters=[("id", "==", "id1")])
    # test it
    assert out.known_divisions
    expected_divisions = (0, 2, 3)
    assert out.divisions == expected_divisions


@pytest.mark.xfail(reason="No longer accept ParquetFile objects")
def test_read_from_fastparquet_parquetfile(tmpdir):
    check_fastparquet()
    fn = str(tmpdir)

    df = pd.DataFrame(
        {
            "a": np.random.choice(["A", "B", "C"], size=100),
            "b": np.random.random(size=100),
            "c": np.random.randint(1, 5, size=100),
        }
    )
    d = dd.from_pandas(df, npartitions=2)
    d.to_parquet(fn, partition_on=["a"], engine="fastparquet")

    pq_f = fastparquet.ParquetFile(fn)

    # OK with no filters
    out = dd.read_parquet(pq_f).compute()
    for val in df.a.unique():
        assert set(df.b[df.a == val]) == set(out.b[out.a == val])

    # OK with  filters
    out = dd.read_parquet(pq_f, filters=[("a", "==", "B")]).compute()
    assert set(df.b[df.a == "B"]) == set(out.b)

    # Engine should not be set to 'pyarrow'
    with pytest.raises(AssertionError):
        out = dd.read_parquet(pq_f, engine="pyarrow")


@pytest.mark.parametrize("scheduler", ["threads", "processes"])
def test_to_parquet_lazy(tmpdir, scheduler, engine):
    tmpdir = str(tmpdir)
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [1.0, 2.0, 3.0, 4.0]})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    value = ddf.to_parquet(tmpdir, compute=False, engine=engine)

    assert hasattr(value, "dask")
    value.compute(scheduler=scheduler)
    assert os.path.exists(tmpdir)

    ddf2 = dd.read_parquet(tmpdir, engine=engine)

    assert_eq(ddf, ddf2)


def test_timestamp96(tmpdir):
    check_fastparquet()
    fn = str(tmpdir)
    df = pd.DataFrame({"a": ["now"]}, dtype="M8[ns]")
    ddf = dd.from_pandas(df, 1)
    ddf.to_parquet(fn, write_index=False, times="int96")
    pf = fastparquet.ParquetFile(fn)
    assert pf._schema[1].type == fastparquet.parquet_thrift.Type.INT96
    out = dd.read_parquet(fn, index=False).compute()
    assert_eq(out, df)


def test_drill_scheme(tmpdir):
    check_fastparquet()
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

    df = dd.read_parquet(files)
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
    ddf.to_parquet(fn, engine=engine)
    rddf = dd.read_parquet(fn, columns=["ints"], engine=engine)
    assert list(rddf.columns) == ["ints"]
    rddf = dd.read_parquet(fn, engine=engine)
    assert list(rddf.columns) == list(df)


def test_columns_name(tmpdir, engine):
    if engine == "fastparquet" and fastparquet.__version__ <= LooseVersion("0.3.1"):
        pytest.skip("Fastparquet does not write column_indexes up to 0.3.1")
    tmp_path = str(tmpdir)
    df = pd.DataFrame({"A": [1, 2]}, index=pd.Index(["a", "b"], name="idx"))
    df.columns.name = "cols"
    ddf = dd.from_pandas(df, 2)

    ddf.to_parquet(tmp_path, engine=engine)
    result = dd.read_parquet(tmp_path, engine=engine, index=["idx"])
    assert_eq(result, df)


def check_compression(engine, filename, compression):
    if engine == "fastparquet":
        pf = fastparquet.ParquetFile(filename)
        md = pf.fmd.row_groups[0].columns[0].meta_data
        if compression is None:
            assert md.total_compressed_size == md.total_uncompressed_size
        else:
            assert md.total_compressed_size != md.total_uncompressed_size
    else:
        metadata = pa.parquet.ParquetDataset(filename).metadata
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


@pytest.mark.parametrize("compression,", ["default", None, "gzip", "snappy"])
def test_writing_parquet_with_compression(tmpdir, compression, engine):
    fn = str(tmpdir)
    if compression in ["snappy", "default"]:
        pytest.importorskip("snappy")

    df = pd.DataFrame({"x": ["a", "b", "c"] * 10, "y": [1, 2, 3] * 10})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    ddf.to_parquet(fn, compression=compression, engine=engine)
    out = dd.read_parquet(fn, engine=engine)
    assert_eq(out, ddf)
    check_compression(engine, fn, compression)


@pytest.mark.parametrize("compression,", ["default", None, "gzip", "snappy"])
def test_writing_parquet_with_partition_on_and_compression(tmpdir, compression, engine):
    fn = str(tmpdir)
    if compression in ["snappy", "default"]:
        pytest.importorskip("snappy")

    df = pd.DataFrame({"x": ["a", "b", "c"] * 10, "y": [1, 2, 3] * 10})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    ddf.to_parquet(fn, compression=compression, engine=engine, partition_on=["x"])
    check_compression(engine, fn, compression)


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


def test_read_no_metadata(tmpdir, engine):
    # use pyarrow.parquet to create a parquet file without
    # pandas metadata
    check_pyarrow()
    tmp = str(tmpdir) + "table.parq"

    table = pa.Table.from_arrays(
        [pa.array([1, 2, 3]), pa.array([3, 4, 5])], names=["A", "B"]
    )
    pq.write_table(table, tmp)
    result = dd.read_parquet(tmp, engine=engine)
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
    pytest.importorskip("snappy")

    df = pd.DataFrame(
        {
            "a": np.random.choice(["A", "B", "C"], size=100),
            "b": np.random.random(size=100),
            "c": np.random.randint(1, 5, size=100),
        }
    )
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=3)

    engine_kwargs = {
        "pyarrow-dataset": {
            "compression": "snappy",
            "coerce_timestamps": None,
            "use_dictionary": True,
        },
        "fastparquet": {"compression": "snappy", "times": "int64", "fixed_text": None},
    }
    engine_kwargs["pyarrow-legacy"] = engine_kwargs["pyarrow-dataset"]

    ddf.to_parquet(path1, engine=engine, **engine_kwargs[engine])
    out = dd.read_parquet(path1, engine=engine)
    assert_eq(out, ddf, check_index=(engine != "fastparquet"))

    # Avoid race condition in pyarrow 0.8.0 on writing partitioned datasets
    with dask.config.set(scheduler="sync"):
        ddf.to_parquet(
            path2, engine=engine, partition_on=["a"], **engine_kwargs[engine]
        )
    out = dd.read_parquet(path2, engine=engine).compute()
    for val in df.a.unique():
        assert set(df.b[df.a == val]) == set(out.b[out.a == val])


def test_writing_parquet_with_unknown_kwargs(tmpdir, engine):
    fn = str(tmpdir)

    with pytest.raises(TypeError):
        ddf.to_parquet(fn, engine=engine, unknown_key="unknown_value")


def test_to_parquet_with_get(tmpdir):
    check_engine()

    from dask.multiprocessing import get as mp_get

    tmpdir = str(tmpdir)

    flag = [False]

    def my_get(*args, **kwargs):
        flag[0] = True
        return mp_get(*args, **kwargs)

    df = pd.DataFrame({"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)

    ddf.to_parquet(tmpdir, compute_kwargs={"scheduler": my_get})
    assert flag[0]

    result = dd.read_parquet(os.path.join(tmpdir, "*"))
    assert_eq(result, df, check_index=False)


def test_select_partitioned_column(tmpdir, engine):
    pytest.importorskip("snappy")
    if engine.startswith("pyarrow"):
        import pyarrow as pa

        if pa.__version__ < LooseVersion("0.9.0"):
            pytest.skip("pyarrow<0.9.0 did not support this")

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
        engine=engine,
        partition_on=["fake_categorical1", "fake_categorical2"],
    )

    df_partitioned = dd.read_parquet(fn, engine=engine)
    df_partitioned[df_partitioned.fake_categorical1 == "A"].compute()


def test_with_tz(tmpdir, engine):
    if engine.startswith("pyarrow") and pa.__version__ < LooseVersion("0.11.0"):
        pytest.skip("pyarrow<0.11.0 did not support this")
    if engine == "fastparquet" and fastparquet.__version__ < LooseVersion("0.3.0"):
        pytest.skip("fastparquet<0.3.0 did not support this")

    with warnings.catch_warnings():
        if engine == "fastparquet":
            # fastparquet-442
            warnings.simplefilter("ignore", FutureWarning)  # pandas 0.25
            fn = str(tmpdir)
            df = pd.DataFrame([[0]], columns=["a"], dtype="datetime64[ns, UTC]")
            df = dd.from_pandas(df, 1)
            df.to_parquet(fn, engine=engine)
            df2 = dd.read_parquet(fn, engine=engine)
            assert_eq(df, df2, check_divisions=False, check_index=False)


def test_arrow_partitioning(tmpdir):
    # Issue #3518
    check_pyarrow()
    path = str(tmpdir)
    data = {
        "p": np.repeat(np.arange(3), 2).astype(np.int8),
        "b": np.repeat(-1, 6).astype(np.int16),
        "c": np.repeat(-2, 6).astype(np.float32),
        "d": np.repeat(-3, 6).astype(np.float64),
    }
    pdf = pd.DataFrame(data)
    ddf = dd.from_pandas(pdf, npartitions=2)
    ddf.to_parquet(path, engine="pyarrow", write_index=False, partition_on="p")

    ddf = dd.read_parquet(path, index=False, engine="pyarrow")

    ddf.astype({"b": np.float32}).compute()


def test_informative_error_messages():
    with pytest.raises(ValueError) as info:
        dd.read_parquet("foo", engine="foo")

    assert "foo" in str(info.value)
    assert "arrow" in str(info.value)
    assert "fastparquet" in str(info.value)


def test_append_cat_fp(tmpdir, engine):
    path = str(tmpdir)
    # https://github.com/dask/dask/issues/4120
    df = pd.DataFrame({"x": ["a", "a", "b", "a", "b"]})
    df["x"] = df["x"].astype("category")
    ddf = dd.from_pandas(df, npartitions=1)

    dd.to_parquet(ddf, path, engine=engine)
    dd.to_parquet(ddf, path, append=True, ignore_divisions=True, engine=engine)

    d = dd.read_parquet(path, engine=engine).compute()
    assert d["x"].tolist() == ["a", "a", "b", "a", "b"] * 2


@pytest.mark.parametrize(
    "df",
    [
        pd.DataFrame({"x": [4, 5, 6, 1, 2, 3]}),
        pd.DataFrame({"x": ["c", "a", "b"]}),
        pd.DataFrame({"x": ["cc", "a", "bbb"]}),
        pd.DataFrame({"x": [b"a", b"b", b"c"]}),
        pytest.param(pd.DataFrame({"x": pd.Categorical(["a", "b", "a"])})),
        pytest.param(pd.DataFrame({"x": pd.Categorical([1, 2, 1])})),
        pd.DataFrame({"x": list(map(pd.Timestamp, [3000000, 2000000, 1000000]))}),  # ms
        pd.DataFrame({"x": list(map(pd.Timestamp, [3000, 2000, 1000]))}),  # us
        pd.DataFrame({"x": [3000, 2000, 1000]}).astype("M8[ns]"),
        # pd.DataFrame({'x': [3, 2, 1]}).astype('M8[ns]'), # Casting errors
        pd.DataFrame({"x": [3, 2, 1]}).astype("M8[us]"),
        pd.DataFrame({"x": [3, 2, 1]}).astype("M8[ms]"),
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
    check_pyarrow()
    # Index will be given a name when preserved as index
    tmp_path = str(tmpdir)
    if not df.index.name:
        df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    dd.to_parquet(ddf, tmp_path, engine="pyarrow", write_index=True)
    ddf2 = dd.read_parquet(tmp_path, engine="pyarrow", gather_statistics=True)
    assert_eq(ddf, ddf2)


def test_datasets_timeseries(tmpdir, engine):
    tmp_path = str(tmpdir)
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-01-10", freq="1d"
    ).persist()
    df.to_parquet(tmp_path, engine=engine)

    df2 = dd.read_parquet(tmp_path, engine=engine)
    assert_eq(df, df2)


def test_pathlib_path(tmpdir, engine):
    import pathlib

    df = pd.DataFrame({"x": [4, 5, 6, 1, 2, 3]})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    path = pathlib.Path(str(tmpdir))
    ddf.to_parquet(path, engine=engine)
    ddf2 = dd.read_parquet(path, engine=engine)
    assert_eq(ddf, ddf2)


def test_pyarrow_metadata_nthreads(tmpdir):
    check_pyarrow()
    tmp_path = str(tmpdir)
    df = pd.DataFrame({"x": [4, 5, 6, 1, 2, 3]})
    df.index.name = "index"
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(tmp_path, engine="pyarrow")
    ops = {"dataset": {"metadata_nthreads": 2}}
    ddf2 = dd.read_parquet(tmp_path, engine="pyarrow", **ops)
    assert_eq(ddf, ddf2)


def test_categories_large(tmpdir, engine):
    # Issue #5112
    check_fastparquet()
    fn = str(tmpdir.join("parquet_int16.parq"))
    numbers = np.random.randint(0, 800000, size=1000000)
    df = pd.DataFrame(numbers.T, columns=["name"])
    df.name = df.name.astype("category")

    df.to_parquet(fn, engine="fastparquet", compression="uncompressed")
    ddf = dd.read_parquet(fn, engine=engine, categories={"name": 80000})

    assert_eq(sorted(df.name.cat.categories), sorted(ddf.compute().name.cat.categories))


@write_read_engines()
def test_read_glob_no_meta(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, engine=write_engine)

    ddf2 = dd.read_parquet(
        os.path.join(tmp_path, "*.parquet"), engine=read_engine, gather_statistics=False
    )
    assert_eq(ddf, ddf2, check_divisions=False)


@write_read_engines()
def test_read_glob_yes_meta(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, engine=write_engine)
    paths = glob.glob(os.path.join(tmp_path, "*.parquet"))
    paths.append(os.path.join(tmp_path, "_metadata"))
    ddf2 = dd.read_parquet(paths, engine=read_engine, gather_statistics=False)
    assert_eq(ddf, ddf2, check_divisions=False)


@pytest.mark.parametrize("statistics", [True, False, None])
@pytest.mark.parametrize("remove_common", [True, False])
@write_read_engines()
def test_read_dir_nometa(tmpdir, write_engine, read_engine, statistics, remove_common):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, engine=write_engine)
    if os.path.exists(os.path.join(tmp_path, "_metadata")):
        os.unlink(os.path.join(tmp_path, "_metadata"))
    files = os.listdir(tmp_path)
    assert "_metadata" not in files

    if remove_common and os.path.exists(os.path.join(tmp_path, "_common_metadata")):
        os.unlink(os.path.join(tmp_path, "_common_metadata"))

    ddf2 = dd.read_parquet(tmp_path, engine=read_engine, gather_statistics=statistics)
    assert_eq(ddf, ddf2, check_divisions=False)
    assert ddf.divisions == tuple(range(0, 420, 30))
    if statistics is False or statistics is None and read_engine.startswith("pyarrow"):
        assert ddf2.divisions == (None,) * 14
    else:
        assert ddf2.divisions == tuple(range(0, 420, 30))


@write_read_engines()
def test_statistics_nometa(tmpdir, write_engine, read_engine):
    tmp_path = str(tmpdir)
    ddf.to_parquet(tmp_path, engine=write_engine, write_metadata_file=False)

    ddf2 = dd.read_parquet(tmp_path, engine=read_engine, gather_statistics=True)
    assert_eq(ddf, ddf2)
    assert ddf.divisions == tuple(range(0, 420, 30))
    assert ddf2.divisions == tuple(range(0, 420, 30))


@pytest.mark.parametrize("schema", ["infer", None])
def test_timeseries_nulls_in_schema(tmpdir, engine, schema):

    if (
        schema == "infer"
        and engine.startswith("pyarrow")
        and pa.__version__ < LooseVersion("0.15.0")
    ):
        pytest.skip("PyArrow>=0.15 Required.")

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
    dataset = {"validate_schema": False}
    engine_use = engine
    if schema != "infer" and engine.startswith("pyarrow"):
        engine_use = "pyarrow-legacy"
    ddf2.to_parquet(
        tmp_path, engine=engine_use, write_metadata_file=False, schema=schema
    )
    ddf_read = dd.read_parquet(tmp_path, engine=engine_use, dataset=dataset)

    assert_eq(ddf_read, ddf2, check_divisions=False, check_index=False)

    # Can force schema validation on each partition in pyarrow
    if engine.startswith("pyarrow") and schema is None:
        dataset = {"validate_schema": True}
        engine_use = engine
        if schema != "infer":
            engine_use = "pyarrow-legacy"
        # The schema mismatch should raise an error if the
        # dataset was written with `schema=None` (no inference)
        with pytest.raises(ValueError):
            ddf_read = dd.read_parquet(tmp_path, dataset=dataset, engine=engine_use)


@pytest.mark.parametrize("numerical", [True, False])
@pytest.mark.parametrize(
    "timestamp", ["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"]
)
def test_timeseries_nulls_in_schema_pyarrow(tmpdir, timestamp, numerical):
    check_pyarrow()
    tmp_path = str(tmpdir)
    ddf2 = dd.from_pandas(
        pd.DataFrame(
            {
                "timestamp": [
                    pd.Timestamp("2000-01-01"),
                    pd.Timestamp("2000-01-02"),
                    pd.Timestamp("2000-01-03"),
                    pd.Timestamp("2000-01-04"),
                ],
                "id": np.arange(4, dtype="float64"),
                "name": ["cat", "dog", "bird", "cow"],
            }
        ),
        npartitions=2,
    ).persist()
    if numerical:
        ddf2.id = ddf2.id.where(ddf2.timestamp == timestamp, None)
        ddf2.id = ddf2.id.astype("float64")
    else:
        ddf2.name = ddf2.name.where(ddf2.timestamp == timestamp, None)

    # There should be no schema error if you specify a schema on write
    schema = pa.schema(
        [("timestamp", pa.timestamp("ns")), ("id", pa.float64()), ("name", pa.string())]
    )
    ddf2.to_parquet(tmp_path, schema=schema, write_index=False, engine="pyarrow")
    assert_eq(
        dd.read_parquet(
            tmp_path, dataset={"validate_schema": True}, index=False, engine="pyarrow"
        ),
        ddf2,
        check_divisions=False,
        check_index=False,
    )


def test_read_inconsistent_schema_pyarrow(tmpdir):
    check_pyarrow()

    # Note: This is a proxy test for a cudf-related issue fix
    # (see cudf#5062 github issue).  The cause of that issue is
    # schema inconsistencies that do not actually correspond to
    # different types, but whether or not the file/column contains
    # null values.

    df1 = pd.DataFrame({"id": [0, 1], "val": [10, 20]})
    df2 = pd.DataFrame({"id": [2, 3], "val": [30, 40]})

    desired_type = "int64"
    other_type = "int32"
    df1.val = df1.val.astype(desired_type)
    df2.val = df2.val.astype(other_type)

    df_expect = pd.concat([df1, df2], ignore_index=True)
    df_expect["val"] = df_expect.val.astype(desired_type)

    df1.to_parquet(os.path.join(tmpdir, "0.parquet"))
    df2.to_parquet(os.path.join(tmpdir, "1.parquet"))

    # Read Directory
    check = dd.read_parquet(str(tmpdir), dataset={"validate_schema": False})
    assert_eq(check.compute(), df_expect, check_index=False)

    # Read List
    check = dd.read_parquet(
        os.path.join(tmpdir, "*.parquet"), dataset={"validate_schema": False}
    )
    assert_eq(check.compute(), df_expect, check_index=False)


def test_graph_size_pyarrow(tmpdir, engine):
    import pickle

    fn = str(tmpdir)

    ddf1 = dask.datasets.timeseries(
        start="2000-01-01", end="2000-01-02", freq="60S", partition_freq="1H"
    )

    ddf1.to_parquet(fn, engine=engine)
    ddf2 = dd.read_parquet(fn, engine=engine)

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

    ddf.to_parquet(tmp_path_rd, engine=engine, write_index=preserve_index)
    ddf = dd.read_parquet(tmp_path_rd, engine=engine)["B"]

    # Write ddf back to disk to check that the round trip
    # preserves the getitem optimization
    out = ddf.to_frame().to_parquet(tmp_path_wt, engine=engine, compute=False)
    dsk = optimize_read_parquet_getitem(out.dask, keys=[out.key])

    read = [key for key in dsk.layers if key.startswith("read-parquet")][0]
    subgraph = dsk.layers[read]
    assert isinstance(subgraph, ParquetSubgraph)
    assert subgraph.columns == ["B"]

    assert_eq(ddf.compute(optimize_graph=False), ddf.compute())


def test_getitem_optimization_empty(tmpdir, engine):
    df = pd.DataFrame({"A": [1] * 100, "B": [2] * 100, "C": [3] * 100, "D": [4] * 100})
    ddf = dd.from_pandas(df, 2)
    fn = os.path.join(str(tmpdir))
    ddf.to_parquet(fn, engine=engine)

    df2 = dd.read_parquet(fn, columns=[], engine=engine)
    dsk = optimize_read_parquet_getitem(df2.dask, keys=[df2._name])

    subgraph = next(iter((dsk.layers.values())))
    assert isinstance(subgraph, ParquetSubgraph)
    assert subgraph.columns == []


def test_getitem_optimization_multi(tmpdir, engine):
    df = pd.DataFrame({"A": [1] * 100, "B": [2] * 100, "C": [3] * 100, "D": [4] * 100})
    ddf = dd.from_pandas(df, 2)
    fn = os.path.join(str(tmpdir))
    ddf.to_parquet(fn, engine=engine)

    a = dd.read_parquet(fn, engine=engine)["B"]
    b = dd.read_parquet(fn, engine=engine)[["C"]]
    c = dd.read_parquet(fn, engine=engine)[["C", "A"]]

    a1, a2, a3 = dask.compute(a, b, c)
    b1, b2, b3 = dask.compute(a, b, c, optimize_graph=False)

    assert_eq(a1, b1)
    assert_eq(a2, b2)
    assert_eq(a3, b3)


def test_subgraph_getitem():
    meta = pd.DataFrame(columns=["a"])
    subgraph = ParquetSubgraph("name", "pyarrow", "fs", meta, [], [], [0, 1, 2], {})

    with pytest.raises(KeyError):
        subgraph["foo"]

    with pytest.raises(KeyError):
        subgraph[("name", -1)]

    with pytest.raises(KeyError):
        subgraph[("name", 3)]


def test_blockwise_parquet_annotations(tmpdir):
    check_engine()

    df = pd.DataFrame({"a": np.arange(40, dtype=np.int32)})
    expect = dd.from_pandas(df, npartitions=2)
    expect.to_parquet(str(tmpdir))

    with dask.annotate(foo="bar"):
        ddf = dd.read_parquet(str(tmpdir))

    # `ddf` should now have ONE Blockwise layer
    layers = ddf.__dask_graph__().layers
    assert len(layers) == 1
    layer = next(iter((layers.values())))
    assert isinstance(layer, ParquetSubgraph)
    assert layer.annotations == {"foo": "bar"}


def test_split_row_groups(tmpdir, engine):
    """Test split_row_groups read_parquet kwarg"""
    check_pyarrow()
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"i32": np.arange(800, dtype=np.int32), "f": np.arange(800, dtype=np.float64)}
    )
    df.index.name = "index"

    half = len(df) // 2
    dd.from_pandas(df.iloc[:half], npartitions=2).to_parquet(
        tmp, engine="pyarrow", row_group_size=100
    )

    ddf3 = dd.read_parquet(tmp, engine=engine, split_row_groups=True, chunksize=1)
    assert ddf3.npartitions == 4

    ddf3 = dd.read_parquet(
        tmp, engine=engine, gather_statistics=True, split_row_groups=False
    )
    assert ddf3.npartitions == 2

    dd.from_pandas(df.iloc[half:], npartitions=2).to_parquet(
        tmp, append=True, engine="pyarrow", row_group_size=50
    )

    ddf3 = dd.read_parquet(
        tmp,
        engine=engine,
        gather_statistics=True,
        split_row_groups=True,
        chunksize=1,
    )
    assert ddf3.npartitions == 12

    ddf3 = dd.read_parquet(
        tmp, engine=engine, gather_statistics=True, split_row_groups=False
    )
    assert ddf3.npartitions == 4


@pytest.mark.parametrize("split_row_groups", [1, 12])
@pytest.mark.parametrize("gather_statistics", [True, False])
def test_split_row_groups_int(tmpdir, split_row_groups, gather_statistics, engine):

    check_pyarrow()
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
        tmp, engine="pyarrow", row_group_size=row_group_size
    )
    dd.from_pandas(df.iloc[half:], npartitions=npartitions).to_parquet(
        tmp, append=True, engine="pyarrow", row_group_size=row_group_size
    )

    ddf2 = dd.read_parquet(
        tmp,
        engine=engine,
        split_row_groups=split_row_groups,
        gather_statistics=gather_statistics,
    )
    expected_rg_cout = int(half_size / row_group_size)
    assert ddf2.npartitions == 2 * math.ceil(expected_rg_cout / split_row_groups)


def test_split_row_groups_filter(tmpdir, engine):
    check_pyarrow()
    tmp = str(tmpdir)
    df = pd.DataFrame(
        {"i32": np.arange(800, dtype=np.int32), "f": np.arange(800, dtype=np.float64)}
    )
    df.index.name = "index"
    search_val = 600
    filters = [("f", "==", search_val)]

    dd.from_pandas(df, npartitions=4).to_parquet(
        tmp, append=True, engine="pyarrow", row_group_size=50
    )

    ddf2 = dd.read_parquet(tmp, engine=engine)
    ddf3 = dd.read_parquet(
        tmp,
        engine=engine,
        gather_statistics=True,
        split_row_groups=True,
        filters=filters,
    )

    assert search_val in ddf3["i32"]
    assert_eq(
        ddf2[ddf2["i32"] == search_val].compute(),
        ddf3[ddf3["i32"] == search_val].compute(),
    )


def test_optimize_getitem_and_nonblockwise(tmpdir):
    check_engine()
    path = os.path.join(tmpdir, "path.parquet")
    df = pd.DataFrame(
        {"a": [3, 4, 2], "b": [1, 2, 4], "c": [5, 4, 2], "d": [1, 2, 3]},
        index=["a", "b", "c"],
    )
    df.to_parquet(path)

    df2 = dd.read_parquet(path)
    df2[["a", "b"]].rolling(3).max().compute()


def test_optimize_and_not(tmpdir):
    check_engine()
    path = os.path.join(tmpdir, "path.parquet")
    df = pd.DataFrame(
        {"a": [3, 4, 2], "b": [1, 2, 4], "c": [5, 4, 2], "d": [1, 2, 3]},
        index=["a", "b", "c"],
    )
    df.to_parquet(path)

    df2 = dd.read_parquet(path)
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


@pytest.mark.parametrize("metadata", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1024, 4096, "1MiB"])
def test_chunksize(tmpdir, chunksize, engine, metadata):
    check_pyarrow()  # Need pyarrow for write phase in this test

    nparts = 2
    df_size = 100
    row_group_size = 5
    row_group_byte_size = 451  # Empirically measured

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
        engine="pyarrow",
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
        engine=engine,
        chunksize=chunksize,
        split_row_groups=True,
        gather_statistics=True,
        index="index",
    )

    assert_eq(ddf1, ddf2, check_divisions=False)

    num_row_groups = df_size // row_group_size
    if not chunksize:
        assert ddf2.npartitions == num_row_groups
    else:
        # Check that we are really aggregating
        df_byte_size = row_group_byte_size * num_row_groups
        expected = df_byte_size // parse_bytes(chunksize)
        remainder = (df_byte_size % parse_bytes(chunksize)) > 0
        expected += int(remainder) * nparts
        assert ddf2.npartitions == max(nparts, expected)


@write_read_engines()
def test_roundtrip_pandas_chunksize(tmpdir, write_engine, read_engine):
    path = str(tmpdir.join("test.parquet"))
    pdf = df.copy()
    pdf.index.name = "index"
    pdf.to_parquet(
        path, engine="pyarrow" if write_engine.startswith("pyarrow") else "fastparquet"
    )

    ddf_read = dd.read_parquet(
        path,
        engine=read_engine,
        chunksize="10 kiB",
        gather_statistics=True,
        split_row_groups=True,
        index="index",
    )

    assert_eq(pdf, ddf_read)


def test_read_pandas_fastparquet_partitioned(tmpdir, engine):
    check_fastparquet()

    pdf = pd.DataFrame(
        [{"str": str(i), "int": i, "group": "ABC"[i % 3]} for i in range(6)]
    )
    path = str(tmpdir)
    pdf.to_parquet(path, partition_cols=["group"], engine="fastparquet")
    ddf_read = dd.read_parquet(path, engine=engine)

    assert len(ddf_read["group"].compute()) == 6
    assert len(ddf_read.compute().group) == 6


def test_read_parquet_getitem_skip_when_getting_read_parquet(tmpdir, engine):
    # https://github.com/dask/dask/issues/5893
    pdf = pd.DataFrame({"A": [1, 2, 3, 4, 5, 6], "B": ["a", "b", "c", "d", "e", "f"]})
    path = os.path.join(str(tmpdir), "data.parquet")
    pd_engine = "pyarrow" if engine.startswith("pyarrow") else "fastparquet"
    pdf.to_parquet(path, engine=pd_engine)

    ddf = dd.read_parquet(path, engine=engine)
    a, b = dask.optimize(ddf["A"], ddf)

    # Make sure we are still allowing the getitem optimization
    ddf = ddf["A"]
    dsk = optimize_read_parquet_getitem(ddf.dask, keys=[(ddf._name, 0)])
    read = [key for key in dsk.layers if key.startswith("read-parquet")][0]
    subgraph = dsk.layers[read]
    assert isinstance(subgraph, ParquetSubgraph)
    assert subgraph.columns == ["A"]


@pytest.mark.parametrize("gather_statistics", [None, True])
@write_read_engines()
def test_filter_nonpartition_columns(
    tmpdir, write_engine, read_engine, gather_statistics
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
    ddf_write.to_parquet(
        tmpdir, write_index=False, partition_on=["id"], engine=write_engine
    )
    ddf_read = dd.read_parquet(
        tmpdir,
        index=False,
        engine=read_engine,
        gather_statistics=gather_statistics,
        filters=[(("time", "<", 5))],
    )
    df_read = ddf_read.compute()
    assert len(df_read) == len(df_read[df_read["time"] < 5])
    assert df_read["time"].max() < 5


def test_pandas_metadata_nullable_pyarrow(tmpdir):

    check_pyarrow()
    if pa.__version__ < LooseVersion("0.16.0") or pd.__version__ < LooseVersion(
        "1.0.0"
    ):
        pytest.skip("PyArrow>=0.16 and Pandas>=1.0.0 Required.")
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
    ddf1.to_parquet(tmpdir, engine="pyarrow")
    ddf2 = dd.read_parquet(tmpdir, engine="pyarrow")

    assert_eq(ddf1, ddf2, check_index=False)


def test_pandas_timestamp_overflow_pyarrow(tmpdir):

    check_pyarrow()
    if pa.__version__ < LooseVersion("0.17.0"):
        pytest.skip("PyArrow>=0.17 Required.")

    info = np.iinfo(np.dtype("int64"))
    arr_numeric = np.linspace(
        start=info.min + 2, stop=info.max, num=1024, dtype="int64"
    )
    arr_dates = arr_numeric.astype("datetime64[ms]")

    table = pa.Table.from_arrays([pa.array(arr_dates)], names=["ts"])
    pa.parquet.write_table(
        table, f"{tmpdir}/file.parquet", use_deprecated_int96_timestamps=False
    )

    # This will raise by default due to overflow
    with pytest.raises(pa.lib.ArrowInvalid) as e:
        dd.read_parquet(str(tmpdir), engine="pyarrow-legacy").compute()
    assert "out of bounds" in str(e.value)

    from dask.dataframe.io.parquet.arrow import ArrowEngine

    class ArrowEngineWithTimestampClamp(ArrowEngine):
        @classmethod
        def clamp_arrow_datetimes(cls, arrow_table: pa.Table) -> pa.Table:
            """Constrain datetimes to be valid for pandas

            Since pandas works in ns precision and arrow / parquet defaults to ms
            precision we need to clamp our datetimes to something reasonable"""

            new_columns = []
            for i, col in enumerate(arrow_table.columns):
                if pa.types.is_timestamp(col.type) and (
                    col.type.unit in ("s", "ms", "us")
                ):
                    multiplier = {"s": 1_0000_000_000, "ms": 1_000_000, "us": 1_000}[
                        col.type.unit
                    ]

                    original_type = col.type

                    series: pd.Series = col.cast(pa.int64()).to_pandas(
                        types_mapper={pa.int64(): pd.Int64Dtype}
                    )
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
            cls, arrow_table: pa.Table, categories, **kwargs
        ) -> pd.DataFrame:
            fixed_arrow_table = cls.clamp_arrow_datetimes(arrow_table)
            return super()._arrow_table_to_pandas(
                fixed_arrow_table, categories, **kwargs
            )

    # this should not fail, but instead produce timestamps that are in the valid range
    dd.read_parquet(str(tmpdir), engine=ArrowEngineWithTimestampClamp).compute()


@fp_pandas_xfail
def test_partitioned_preserve_index(tmpdir, write_engine, read_engine):

    if write_engine.startswith("pyarrow") and pa.__version__ < LooseVersion("0.15.0"):
        pytest.skip("PyArrow>=0.15 Required.")

    tmp = str(tmpdir)
    size = 1_000
    npartitions = 4
    b = np.arange(npartitions).repeat(size // npartitions)
    data = pd.DataFrame(
        {
            "myindex": np.arange(size),
            "A": np.random.random(size=size),
            "B": pd.Categorical(b),
        }
    ).set_index("myindex")
    data.index.name = None
    df1 = dd.from_pandas(data, npartitions=npartitions)
    df1.to_parquet(tmp, partition_on="B", engine=write_engine)

    expect = data[data["B"] == 1]
    got = dd.read_parquet(tmp, engine=read_engine, filters=[("B", "==", 1)])
    assert_eq(expect, got)


def test_from_pandas_preserve_none_index(tmpdir, engine):

    check_pyarrow()
    if pa.__version__ < LooseVersion("0.15.0"):
        pytest.skip("PyArrow>=0.15 Required.")

    fn = str(tmpdir.join("test.parquet"))
    df = pd.DataFrame({"a": [1, 2], "b": [4, 5], "c": [6, 7]}).set_index("c")
    df.index.name = None
    df.to_parquet(
        fn,
        engine="pyarrow" if engine.startswith("pyarrow") else "fastparquet",
        index=True,
    )

    expect = pd.read_parquet(fn)
    got = dd.read_parquet(fn, engine=engine)
    assert_eq(expect, got)


def test_multi_partition_none_index_false(tmpdir, engine):
    check_pyarrow()
    if pa.__version__ < LooseVersion("0.15.0"):
        pytest.skip("PyArrow>=0.15 Required.")

    if engine.startswith("pyarrow"):
        write_engine = "pyarrow"
    else:
        assert engine == "fastparquet"
        write_engine = "fastparquet"

    # Write dataset without dask.to_parquet
    ddf1 = ddf.reset_index(drop=True)
    for i, part in enumerate(ddf1.partitions):
        path = tmpdir.join(f"test.{i}.parquet")
        part.compute().to_parquet(str(path), engine=write_engine)

    # Read back with index=False
    ddf2 = dd.read_parquet(str(tmpdir), index=False, engine=engine)
    assert_eq(ddf1, ddf2)


@write_read_engines()
def test_from_pandas_preserve_none_rangeindex(tmpdir, write_engine, read_engine):
    # See GitHub Issue#6348
    fn = str(tmpdir.join("test.parquet"))
    df0 = pd.DataFrame({"t": [1, 2, 3]}, index=pd.RangeIndex(start=1, stop=4))
    df0.to_parquet(
        fn, engine="pyarrow" if write_engine.startswith("pyarrow") else "fastparquet"
    )

    df1 = dd.read_parquet(fn, engine=read_engine)
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
        ddf.to_parquet(fn, engine=engine, write_index=False)

    # If we do want to preserve the None index name, should
    # get a ValueError for having an illegal column name
    with pytest.raises(ValueError) as e:
        ddf.to_parquet(fn, engine=engine)
    assert null_name in str(e.value)


def test_divisions_with_null_partition(tmpdir, engine):
    df = pd.DataFrame({"a": [1, 2, None, None], "b": [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(str(tmpdir), engine=engine, write_index=False)

    ddf_read = dd.read_parquet(str(tmpdir), engine=engine, index="a")
    assert ddf_read.divisions == (None, None, None)


def test_pyarrow_dataset_simple(tmpdir, engine):
    check_pyarrow()
    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df.set_index("a", inplace=True, drop=True)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, engine=engine)
    read_df = dd.read_parquet(fn, engine="pyarrow-legacy")
    read_df.compute(scheduler="synchronous")
    assert_eq(ddf, read_df)


@pytest.mark.parametrize("test_filter", [True, False])
def test_pyarrow_dataset_partitioned(tmpdir, engine, test_filter):
    check_pyarrow()

    if pa.__version__ <= LooseVersion("0.17.1"):
        # Using pyarrow.dataset API does not produce
        # Categorical type for partitioned columns.
        pytest.skip("PyArrow>0.17.1 Required.")

    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df["b"] = df["b"].astype("category")
    ddf = dd.from_pandas(df, npartitions=2)
    ddf.to_parquet(fn, engine=engine, partition_on="b")
    read_df = dd.read_parquet(
        fn,
        engine="pyarrow",
        filters=[("b", "==", "a")] if test_filter else None,
    )

    if test_filter:
        assert_eq(ddf[ddf["b"] == "a"].compute(), read_df.compute())
    else:
        assert_eq(ddf, read_df)


@pytest.mark.parametrize("read_from_paths", [True, False])
@pytest.mark.parametrize("test_filter_partitioned", [True, False])
def test_pyarrow_dataset_read_from_paths(
    tmpdir, read_from_paths, test_filter_partitioned
):
    check_pyarrow()

    if pa.__version__ <= LooseVersion("0.17.1"):
        # Using pyarrow.dataset API does not produce
        # Categorical type for partitioned columns.
        pytest.skip("PyArrow>0.17.1 Required.")

    fn = str(tmpdir)
    df = pd.DataFrame({"a": [4, 5, 6], "b": ["a", "b", "b"]})
    df["b"] = df["b"].astype("category")
    ddf = dd.from_pandas(df, npartitions=2)

    if test_filter_partitioned:
        ddf.to_parquet(fn, engine="pyarrow", partition_on="b")
    else:
        ddf.to_parquet(fn, engine="pyarrow")
    read_df = dd.read_parquet(
        fn,
        engine="pyarrow",
        filters=[("b", "==", "a")] if test_filter_partitioned else None,
        read_from_paths=read_from_paths,
    )

    if test_filter_partitioned:
        assert_eq(ddf[ddf["b"] == "a"].compute(), read_df.compute())
    else:
        assert_eq(ddf, read_df)


@pytest.mark.parametrize("split_row_groups", [True, False])
def test_pyarrow_dataset_filter_partitioned(tmpdir, split_row_groups):
    check_pyarrow()

    if pa.__version__ < LooseVersion("1.0.0"):
        # pyarrow.dataset API required.
        pytest.skip("PyArrow>=1.0.0 Required.")

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
    ddf.to_parquet(fn, engine="pyarrow", partition_on=["b", "c"])

    # Filter on a a non-partition column
    read_df = dd.read_parquet(
        fn,
        engine="pyarrow-dataset",
        split_row_groups=split_row_groups,
        filters=[("a", "==", 5)],
    )
    assert_eq(
        read_df.compute()[["a"]],
        df[df["a"] == 5][["a"]],
        check_index=False,
    )


def test_parquet_pyarrow_write_empty_metadata(tmpdir):
    # https://github.com/dask/dask/issues/6600
    check_pyarrow()
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

    try:
        df.to_parquet(
            tmpdir,
            engine="pyarrow",
            partition_on=["x"],
            append=False,
        )

    except AttributeError:
        pytest.fail("Unexpected AttributeError")

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


def test_parquet_pyarrow_write_empty_metadata_append(tmpdir):
    # https://github.com/dask/dask/issues/6600
    check_pyarrow()
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
        engine="pyarrow",
        partition_on=["x"],
        append=False,
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
        engine="pyarrow",
        partition_on=["x"],
        append=True,
        ignore_divisions=True,
    )


@pytest.mark.parametrize("partition_on", [None, "a"])
@write_read_engines()
def test_create_metadata_file(tmpdir, write_engine, read_engine, partition_on):

    check_pyarrow()
    tmpdir = str(tmpdir)

    # Write ddf without a _metadata file
    df1 = pd.DataFrame({"b": range(100), "a": ["A", "B", "C", "D"] * 25})
    df1.index.name = "myindex"
    ddf1 = dd.from_pandas(df1, npartitions=10)
    ddf1.to_parquet(
        tmpdir,
        write_metadata_file=False,
        partition_on=partition_on,
        engine=write_engine,
    )

    # Add global _metadata file
    if partition_on:
        fns = glob.glob(os.path.join(tmpdir, partition_on + "=*/*.parquet"))
    else:
        fns = glob.glob(os.path.join(tmpdir, "*.parquet"))
    dd.io.parquet.create_metadata_file(
        fns,
        engine="pyarrow",
        split_every=3,  # Force tree reduction
    )

    # Check that we can now read the ddf
    # with the _metadata file present
    ddf2 = dd.read_parquet(
        tmpdir,
        gather_statistics=True,
        split_row_groups=False,
        engine=read_engine,
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
        engine="pyarrow",
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
    dd.to_parquet(ddf, tmpdir, engine=engine, overwrite=True)

    # Keep the contents of the DataFrame constatn but change the # of partitions
    ddf2 = ddf.repartition(npartitions=3)

    # Overwrite the existing Dataset with the new dataframe and evaluate
    # the number of files against the number of dask partitions
    dd.to_parquet(ddf2, tmpdir, engine=engine, overwrite=True)

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
    dd.to_parquet(ddf, tmpdir, engine=engine, partition_on=["A", "B"], overwrite=True)

    # Get the total number of files and directories from the original write
    files_ = Path(tmpdir).rglob("*")
    files = [f.as_posix() for f in files_]
    # Keep the contents of the DataFrame constant but change the # of partitions
    ddf2 = ddf.repartition(npartitions=3)

    # Overwrite the existing Dataset with the new dataframe and evaluate
    # the number of files against the number of dask partitions
    # Get the total number of files and directories from the original write
    dd.to_parquet(ddf2, tmpdir, engine=engine, partition_on=["A", "B"], overwrite=True)
    files2_ = Path(tmpdir).rglob("*")
    files2 = [f.as_posix() for f in files2_]
    # After reducing the # of partitions and overwriting, we expect
    # there to be fewer total files than were originally written
    assert len(files2) < len(files)


def test_to_parquet_overwrite_raises(tmpdir, engine):
    # https://github.com/dask/dask/issues/6824
    # Check that overwrite=True will raise an error if the
    # specified path is the current working directory
    df = pd.DataFrame({"a": range(12)})
    ddf = dd.from_pandas(df, npartitions=3)
    with pytest.raises(ValueError):
        dd.to_parquet(ddf, "./", engine=engine, overwrite=True)
    with pytest.raises(ValueError):
        dd.to_parquet(ddf, tmpdir, engine=engine, append=True, overwrite=True)


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
    ddf.to_parquet(tmpdir, partition_on="year", engine=engine)
    dd.read_parquet(tmpdir, filters=[("year", "==", 2020)], engine=engine)
    assert all


def test_roundtrip_decimal_dtype(tmpdir):
    # https://github.com/dask/dask/issues/6948
    check_pyarrow()
    tmpdir = str(tmpdir)

    data = [
        {
            "ts": pd.to_datetime("2021-01-01", utc="Europe/Berlin"),
            "col1": Decimal("123.00"),
        }
        for i in range(23)
    ]
    ddf1 = dd.from_pandas(pd.DataFrame(data), npartitions=1)

    ddf1.to_parquet(path=tmpdir, engine="pyarrow")
    ddf2 = dd.read_parquet(tmpdir, engine="pyarrow")

    assert ddf1["col1"].dtype == ddf2["col1"].dtype
    assert_eq(ddf1, ddf2, check_divisions=False)


def test_roundtrip_rename_columns(tmpdir, engine):
    # https://github.com/dask/dask/issues/7017

    path = os.path.join(str(tmpdir), "test.parquet")
    df1 = pd.DataFrame(columns=["a", "b", "c"], data=np.random.uniform(size=(10, 3)))
    df1.to_parquet(path)

    # read it with dask and rename columns
    ddf2 = dd.read_parquet(path, engine=engine)
    ddf2.columns = ["d", "e", "f"]
    df1.columns = ["d", "e", "f"]

    assert_eq(df1, ddf2.compute())
