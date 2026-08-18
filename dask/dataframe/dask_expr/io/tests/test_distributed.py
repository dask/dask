from __future__ import annotations

import os

import pytest

distributed = pytest.importorskip("distributed")

from distributed.utils_test import *  # noqa: F403
from distributed.utils_test import gen_cluster

import dask.dataframe as dd
from dask.dataframe.dask_expr import read_parquet
from dask.dataframe.dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


@pytest.fixture(params=["arrow"])
def filesystem(request):
    return request.param


def _make_file(dir, df=None, filename="myfile.parquet", **kwargs):
    fn = os.path.join(str(dir), filename)
    if df is None:
        df = pd.DataFrame({c: range(10) for c in "abcde"})
    df.to_parquet(fn, **kwargs)
    return fn


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_io_fusion_merge(c, s, a, b, tmpdir):
    pdf = pd.DataFrame({c: range(100) for c in "abcdefghij"})
    p = dd.from_pandas(pdf, 2).to_parquet(tmpdir, compute=False)
    _ = await c.gather(c.compute(p))

    df = dd.read_parquet(tmpdir).merge(
        dd.read_parquet(tmpdir).add_suffix("_x"), left_on="a", right_on="a_x"
    )[["a_x", "b_x", "b"]]
    out = await c.gather(c.compute(df.optimize()))
    pd.testing.assert_frame_equal(
        out.sort_values(by="a_x", ignore_index=True),
        pdf.merge(pdf.add_suffix("_x"), left_on="a", right_on="a_x")[
            ["a_x", "b_x", "b"]
        ],
    )


@pytest.mark.filterwarnings("error")
def test_parquet_distributed(c, tmpdir, filesystem):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    df = read_parquet(_make_file(tmpdir, df=pdf), filesystem=filesystem)
    assert_eq(c.gather(c.compute(df)), pdf)


def test_persist_root_expr_npartitions_matches_futures(c, tmpdir):
    # https://github.com/dask/dask/issues/12488
    #
    # `FrameBase.persist` used to optimize a bare (parent-less) root expr on
    # its own, so the "tune" stage (e.g. FusedIO's IO-partition bucketing,
    # triggered here by selecting few enough columns) never fired: `_tune_up`
    # only runs when a parent calls it on a child during `Expr.rewrite`.
    # `Client.persist` re-optimizes the same expr wrapped for submission,
    # where it *does* have a parent, so it *does* get fused. The two passes
    # would disagree on partition count, leaving the returned collection's
    # npartitions out of sync with the futures actually created -- any later
    # compute would fail with "lost dependencies".
    pdf = pd.DataFrame({c: range(10) for c in "abcde"})
    for i in range(2):
        _make_file(tmpdir, df=pdf, filename=f"part.{i}.parquet")

    # 2 of 5 columns selected -> low fusion_compression_factor -> IO fusion
    df = read_parquet(str(tmpdir), columns=["a", "b"])
    persisted = df.persist()

    from distributed.client import futures_of

    assert persisted.npartitions == len(futures_of(persisted))
    assert_eq(
        persisted.compute().sort_values("a", ignore_index=True),
        pd.concat([pdf[["a", "b"]], pdf[["a", "b"]]], ignore_index=True).sort_values(
            "a", ignore_index=True
        ),
    )


def test_pickle_size(tmpdir, filesystem):
    pdf = pd.DataFrame({"x": [1, 4, 3, 2, 0, 5]})
    [_make_file(tmpdir, df=pdf, filename=f"{x}.parquet") for x in range(10)]
    df = read_parquet(tmpdir, filesystem=filesystem)
    from distributed.protocol import dumps

    assert (low_level := len(b"".join(dumps(df.optimize().dask)))) <= 10_000
    assert len(b"".join(dumps(df.optimize()))) <= low_level
