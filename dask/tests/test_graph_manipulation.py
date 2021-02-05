import random
import time
from operator import add

import pytest

import dask
from dask import delayed
from dask.graph_manipulation import checkpoint, clone, bind, block_until_done
from dask.highlevelgraph import HighLevelGraph
from dask.tests.test_base import Tuple
from dask.utils_test import import_or_none


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


def assert_no_common_keys(a, b):
    dsk1 = a.__dask_graph__()
    dsk2 = b.__dask_graph__()
    assert not dsk1.keys() & dsk2.keys()
    if isinstance(dsk1, HighLevelGraph) and isinstance(dsk2, HighLevelGraph):
        assert not dsk1.layers.keys() & dsk2.layers.keys()
        assert not dsk1.dependencies.keys() & dsk2.dependencies.keys()


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


@pytest.mark.parametrize("layers", [False, True])
def test_clone(layers):
    dsk1 = {("a", h1): 1, ("a", h2): 2}
    dsk2 = {"b": (add, ("a", h1), ("a", h2))}
    if layers:
        dsk1 = HighLevelGraph({"a": dsk1}, dependencies={"a": set()})
        dsk2 = HighLevelGraph(
            {"a": dsk1, "b": dsk2}, dependencies={"a": set(), "b": {"a"}}
        )
    else:
        dsk2.update(dsk1)
    t1 = Tuple(dsk1, [("a", h1), ("a", h2)])
    t2 = Tuple(dsk2, ["b"])

    c1 = clone(t2, seed=1, assume_layers=layers)
    c2 = clone(t2, seed=1, assume_layers=layers)
    c3 = clone(t2, seed=2, assume_layers=layers)
    c4 = clone(c1, seed=1, assume_layers=layers)  # Clone of a clone has different keys
    c5 = clone(t2, assume_layers=layers)  # Random seed
    c6 = clone(t2, assume_layers=layers)  # Random seed
    c7 = clone(t2, omit=t1, seed=1, assume_layers=layers)

    assert c1.__dask_graph__() == c2.__dask_graph__()
    assert_no_common_keys(c1, t2)
    assert_no_common_keys(c1, c3)
    assert_no_common_keys(c1, c4)
    assert_no_common_keys(c1, c5)
    assert_no_common_keys(c5, c6)
    # All keys of t1 are in c7
    assert not t1.__dask_graph__().keys() - c7.__dask_graph__().keys()
    # No top-level keys of t2 are in c7
    assert not set(t2.__dask_keys__()) & c7.__dask_graph__().keys()
    assert dask.compute(t2, c1, c2, c3, c4, c5, c6, c7) == ((3,),) * 8
