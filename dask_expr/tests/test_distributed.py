from __future__ import annotations

import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library

distributed = pytest.importorskip("distributed")

from distributed import Client, LocalCluster
from distributed.utils_test import client as c  # noqa F401
from distributed.utils_test import gen_cluster

import dask_expr as dx

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.mark.parametrize("npartitions", [None, 1, 20])
@gen_cluster(client=True)
async def test_p2p_shuffle(c, s, a, b, npartitions):
    df = dx.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = df.shuffle("x", backend="p2p", npartitions=npartitions)
    if npartitions is None:
        assert out.npartitions == df.npartitions
    else:
        assert out.npartitions == npartitions
    x, y, z = c.compute([df.x.size, out.x.size, out.partitions[-1].x.size])
    x = await x
    y = await y
    z = await z
    assert x == y
    if npartitions != 1:
        assert x > z


@pytest.mark.parametrize("npartitions_left", [5, 6])
@gen_cluster(client=True)
async def test_merge_p2p_shuffle(c, s, a, b, npartitions_left):
    df_left = lib.DataFrame({"a": [1, 2, 3] * 100, "b": 2})
    df_right = lib.DataFrame({"a": [4, 2, 3] * 100, "c": 2})
    left = from_pandas(df_left, npartitions=npartitions_left)
    right = from_pandas(df_right, npartitions=5)

    out = left.merge(right, shuffle_backend="p2p")
    assert out.npartitions == npartitions_left
    x = c.compute(out)
    x = await x
    lib.testing.assert_frame_equal(x.reset_index(drop=True), df_left.merge(df_right))


@pytest.mark.parametrize("npartitions_left", [5, 6])
@gen_cluster(client=True)
async def test_index_merge_p2p_shuffle(c, s, a, b, npartitions_left):
    df_left = lib.DataFrame({"a": [1, 2, 3] * 100, "b": 2}).set_index("a")
    df_right = lib.DataFrame({"a": [4, 2, 3] * 100, "c": 2})
    left = from_pandas(df_left, npartitions=npartitions_left, sort=False)
    right = from_pandas(df_right, npartitions=5)

    out = left.merge(right, left_index=True, right_on="a", shuffle_backend="p2p")
    assert out.npartitions == npartitions_left
    x = c.compute(out)
    x = await x
    lib.testing.assert_frame_equal(
        x.sort_index(),
        df_left.merge(df_right, left_index=True, right_on="a").sort_index(),
    )


@gen_cluster(client=True)
async def test_merge_p2p_shuffle(c, s, a, b):
    df_left = lib.DataFrame({"a": [1, 2, 3] * 100, "b": 2, "e": 2})
    df_right = lib.DataFrame({"a": [4, 2, 3] * 100, "c": 2})
    left = from_pandas(df_left, npartitions=6)
    right = from_pandas(df_right, npartitions=5)

    out = left.merge(right, shuffle_backend="p2p")[["b", "c"]]
    assert out.npartitions == 6
    x = c.compute(out)
    x = await x
    lib.testing.assert_frame_equal(
        x.reset_index(drop=True), df_left.merge(df_right)[["b", "c"]]
    )


@gen_cluster(client=True)
async def test_merge_p2p_shuffle_projection_error(c, s, a, b):
    pdf1 = lib.DataFrame({"a": [1, 2, 3], "b": 1})
    pdf2 = lib.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": 1})
    df1 = from_pandas(pdf1, npartitions=2)
    df2 = from_pandas(pdf2, npartitions=3)
    df = df1.merge(df2, left_on="a", right_on="x")
    min_val = df.groupby("x")["y"].sum().reset_index()
    result = df.merge(min_val)
    expected = lib.DataFrame(
        {"a": [2, 3, 1], "b": 1, "x": [2, 3, 1], "y": 1}, index=[0, 0, 1]
    )
    x = c.compute(result)
    x = await x
    lib.testing.assert_frame_equal(
        x.sort_values("a", ignore_index=True),
        expected.sort_values("a", ignore_index=True),
    )


def test_sort_values():
    with LocalCluster(processes=False, n_workers=2) as cluster:
        with Client(cluster) as client:  # noqa: F841
            pdf = lib.DataFrame({"a": [5] + list(range(100)), "b": 2})
            df = from_pandas(pdf, npartitions=10)

            out = df.sort_values(by="a").compute()
    lib.testing.assert_frame_equal(
        out.reset_index(drop=True),
        pdf.sort_values(by="a", ignore_index=True),
    )


@pytest.mark.parametrize("add_repartition", [True, False])
def test_merge_combine_similar_squash_merges(add_repartition):
    with LocalCluster(processes=False, n_workers=2) as cluster:
        with Client(cluster) as client:  # noqa: F841
            pdf = lib.DataFrame(
                {
                    "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 2,
                    "b": 1,
                    "c": 1,
                }
            )
            pdf2 = lib.DataFrame(
                {"m": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 2, "n": 1, "o": 2, "p": 3}
            )
            pdf3 = lib.DataFrame(
                {"x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] * 2, "y": 1, "z": 1, "zz": 2}
            )

            df = from_pandas(pdf, npartitions=5)
            df2 = from_pandas(pdf2, npartitions=10)
            df3 = from_pandas(pdf3, npartitions=10)

            df = df[df.a > 1]
            df2 = df2[df2.m > 1]
            df3 = df3[df3.x > 1]
            if add_repartition:
                df = df.repartition(df.npartitions // 2)
                df2 = df2.repartition(df2.npartitions // 2)
            q = df.merge(df2, left_on="a", right_on="m")
            if add_repartition:
                df3 = df3.repartition(df3.npartitions // 2)
                q = q.repartition(q.npartitions // 2)
            q = q.merge(df3, left_on="n", right_on="x")
            q["revenue"] = q.y * (1 - q.z)
            result = q[["x", "n", "o", "revenue"]]
            result_q = result.optimize(fuse=False)

            assert (
                result_q.expr.frame.frame.frame._name
                == result_q.expr.frame.value.left.frame._name
            )
            out = result.compute()

    pdf = pdf[pdf.a > 1]
    pdf2 = pdf2[pdf2.m > 1]
    pdf3 = pdf3[pdf3.x > 1]
    q = pdf.merge(pdf2, left_on="a", right_on="m")
    q = q.merge(pdf3, left_on="n", right_on="x")
    q["revenue"] = q.y * (1 - q.z)
    expected = q[["x", "n", "o", "revenue"]]

    lib.testing.assert_frame_equal(
        out.reset_index(drop=True),
        expected,
    )
