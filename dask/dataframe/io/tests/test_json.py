import json
import os

import pandas as pd
import pytest

import dask.dataframe as dd
from dask.dataframe.utils import assert_eq
from dask.utils import tmpdir, tmpfile

df = pd.DataFrame({"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]})
ddf = dd.from_pandas(df, npartitions=2)


@pytest.mark.parametrize("orient", ["split", "records", "index", "columns", "values"])
def test_read_json_basic(orient):
    with tmpfile("json") as f:
        df.to_json(f, orient=orient, lines=False)
        actual = dd.read_json(f, orient=orient, lines=False)
        actual_pd = pd.read_json(f, orient=orient, lines=False)

        out = actual.compute()
        assert_eq(out, actual_pd)
        if orient == "values":
            out.columns = list(df.columns)
        assert_eq(out, df)


@pytest.mark.parametrize("fkeyword", ["pandas", "json"])
def test_read_json_fkeyword(fkeyword):
    def _my_json_reader(*args, **kwargs):
        if fkeyword == "json":
            return pd.DataFrame.from_dict(json.load(*args))
        return pd.read_json(*args)

    with tmpfile("json") as f:
        df.to_json(f, orient="records", lines=False)
        actual = dd.read_json(f, orient="records", lines=False, engine=_my_json_reader)
        actual_pd = pd.read_json(f, orient="records", lines=False)
        assert_eq(actual, actual_pd)


@pytest.mark.parametrize("orient", ["split", "records", "index", "columns", "values"])
def test_read_json_meta(orient, tmpdir):
    df = pd.DataFrame({"x": range(5), "y": ["a", "b", "c", "d", "e"]})
    df2 = df.assign(x=df.x + 0.5)
    lines = orient == "records"
    df.to_json(str(tmpdir.join("fil1.json")), orient=orient, lines=lines)
    df2.to_json(str(tmpdir.join("fil2.json")), orient=orient, lines=lines)
    sol = pd.concat([df, df2])
    meta = df2.iloc[:0]

    if orient == "values":
        # orient=values loses column names
        sol.columns = meta.columns = [0, 1]

    res = dd.read_json(
        str(tmpdir.join("fil*.json")), orient=orient, meta=meta, lines=lines
    )
    assert_eq(res, sol)

    if orient == "records":
        # Also check chunked version
        res = dd.read_json(
            str(tmpdir.join("fil*.json")),
            orient=orient,
            meta=meta,
            lines=True,
            blocksize=50,
        )
        assert_eq(res, sol, check_index=False)


@pytest.mark.parametrize("orient", ["split", "records", "index", "columns", "values"])
def test_write_json_basic(orient):
    with tmpdir() as path:
        fn = os.path.join(path, "1.json")
        df.to_json(fn, orient=orient, lines=False)
        actual = dd.read_json(fn, orient=orient, lines=False)
        out = actual.compute()
        if orient == "values":
            out.columns = list(df.columns)
        assert_eq(out, df)


def test_to_json_with_get():
    from dask.multiprocessing import get as mp_get

    flag = [False]

    def my_get(*args, **kwargs):
        flag[0] = True
        return mp_get(*args, **kwargs)

    df = pd.DataFrame({"x": ["a", "b", "c", "d"], "y": [1, 2, 3, 4]})
    ddf = dd.from_pandas(df, npartitions=2)

    with tmpdir() as dn:
        ddf.to_json(dn, compute_kwargs={"scheduler": my_get})
        assert flag[0]
        result = dd.read_json(os.path.join(dn, "*"))
        assert_eq(result, df, check_index=False)


def test_read_json_error():
    with tmpfile("json") as f:
        with pytest.raises(ValueError):
            df.to_json(f, orient="split", lines=True)
        df.to_json(f, orient="split", lines=False)
        with pytest.raises(ValueError):
            dd.read_json(f, orient="split", blocksize=1)


@pytest.mark.parametrize("block", [5, 15, 33, 200, 90000])
def test_read_chunked(block):
    with tmpdir() as path:
        fn = os.path.join(path, "1.json")
        df.to_json(fn, orient="records", lines=True)
        d = dd.read_json(fn, blocksize=block, sample=10)
        assert (d.npartitions > 1) or (block > 50)
        assert_eq(d, df, check_index=False)


@pytest.mark.parametrize("compression", [None, "gzip", "xz"])
def test_json_compressed(compression):
    with tmpdir() as path:
        dd.to_json(ddf, path, compression=compression)
        actual = dd.read_json(os.path.join(path, "*"), compression=compression)
        assert_eq(df, actual.compute(), check_index=False)


def test_read_json_inferred_compression():
    with tmpdir() as path:
        fn = os.path.join(path, "*.json.gz")
        dd.to_json(ddf, fn, compression="gzip")
        actual = dd.read_json(fn)
        assert_eq(df, actual.compute(), check_index=False)
