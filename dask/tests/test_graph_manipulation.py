import random
import time

import pytest

import dask
from dask import delayed
from dask.utils_test import import_or_none
from dask.graph_manipulation import checkpoint, clone, bind, block_until_done

from dask.tests.test_base import Tuple


da = import_or_none("dask.array")
db = import_or_none("dask.bag")
dd = import_or_none("dask.dataframe")
pd = import_or_none("pandas")


class NodeCounter:
    def __init__(self):
        self.n = 0

    def f(self, x):
        time.sleep(random.random() / 100)
        self.n += 1
        return x


# Generic hashables
h1 = object()
h2 = object()


def collections():
    cnt = NodeCounter()
    df = pd.DataFrame({"x": list(range(10))})

    # Test two samples of all collections where applicable, one with multiple chunks
    # and one with a single chunk
    colls = [
        # dask.delayed
        delayed(cnt.f)("Hello 1"),  # 1 chunk
        # dask.array
        da.ones((10, 10), chunks=5).map_blocks(cnt.f),  # 4 chunks
        da.ones((1,), chunks=-1).map_blocks(cnt.f),  # 1 chunk
        # dask.bag
        db.from_sequence([1, 2], npartitions=2).map(cnt.f),  # 2 chunks
        db.from_sequence([1], npartitions=1).map(cnt.f),  # 1 chunk
        db.Item.from_delayed(delayed(cnt.f)("Hello 2")),  # 1 chunk
        # dask.dataframe
        dd.from_pandas(df, npartitions=2).map_partitions(cnt.f),  # 2 chunks
        dd.from_pandas(df, npartitions=1).map_partitions(cnt.f),  # 1 chunk
        dd.from_pandas(df["x"], npartitions=2).map_partitions(cnt.f),  # 2 chunks
        dd.from_pandas(df["x"], npartitions=1).map_partitions(cnt.f),  # 1 chunk
    ]
    cnt.n = 0
    return colls, cnt


def test_checkpoint():
    cnt = NodeCounter()
    dsk1 = {("a", h1): (cnt.f, 1), ("a", h2): (cnt.f, 2)}
    dsk2 = {"b": (cnt.f, 2)}
    cp = checkpoint(Tuple(dsk1, list(dsk1)), {"x": [Tuple(dsk2, list(dsk2))]})
    assert cp.compute(scheduler="sync") is None
    assert cnt.n == 3


@pytest.mark.skipif("not da or not db or not dd")
def test_checkpoint_collections():
    colls, cnt = collections()
    cp = checkpoint(*colls)
    cp.compute(scheduler="sync")
    assert cnt.n == 16


def test_block_until_done_one():
    cnt = NodeCounter()
    dsk = {("a", h1): (cnt.f, 1), ("a", h2): (cnt.f, 2)}
    t = block_until_done(Tuple(dsk, list(dsk)))
    assert t.compute(scheduler="sync") == (1, 2)
    assert cnt.n == 2


def test_block_until_done_many():
    cnt = NodeCounter()
    dsk1 = {("a", h1): (cnt.f, 1), ("a", h2): (cnt.f, 2)}
    dsk2 = {"b": (cnt.f, 3)}
    out = block_until_done(Tuple(dsk1, list(dsk1)), {"x": [Tuple(dsk2, list(dsk2))]})
    assert dask.compute(*out, scheduler="sync") == ((1, 2), {"x": [(3,)]})
    assert cnt.n == 3


@pytest.mark.skipif("not da or not db or not dd")
def test_block_until_done_collections():
    colls, cnt = collections()

    # Create a delayed that depends on a single one among all collections
    @delayed
    def f(x):
        pass

    colls2 = block_until_done(*colls)
    f(colls2[0]).compute()
    assert cnt.n == 16

    # dask.delayed
    assert colls2[0].compute() == colls[0].compute()
    # dask.array
    da.utils.assert_eq(colls2[1], colls[1])
    da.utils.assert_eq(colls2[2], colls[2])
    # dask.bag
    db.utils.assert_eq(colls2[3], colls[3])
    db.utils.assert_eq(colls2[4], colls[4])
    db.utils.assert_eq(colls2[5], colls[5])
    # dask.dataframe
    dd.utils.assert_eq(colls2[6], colls[6])
    dd.utils.assert_eq(colls2[7], colls[7])
    dd.utils.assert_eq(colls2[8], colls[8])
    dd.utils.assert_eq(colls2[9], colls[9])
