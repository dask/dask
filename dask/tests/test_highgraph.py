import os

import pytest

import dask.array as da
from dask.utils_test import inc
from dask.highlevelgraph import HighLevelGraph


def test_visualize(tmpdir):
    pytest.importorskip("graphviz")
    fn = str(tmpdir)
    a = da.ones(10, chunks=(5,))
    b = a + 1
    c = a + 2
    d = b + c
    d.dask.visualize(fn)
    assert os.path.exists(fn)


def test_basic():
    a = {"x": 1}
    b = {"y": (inc, "x")}
    layers = {"a": a, "b": b}
    dependencies = {"a": set(), "b": {"a"}}
    hg = HighLevelGraph(layers, dependencies)

    assert dict(hg) == {"x": 1, "y": (inc, "x")}


def test_keys_values_items_methods():
    a = da.ones(10, chunks=(5,))
    b = a + 1
    c = a + 2
    d = b + c
    hg = d.dask

    keys, values, items = hg.keys(), hg.values(), hg.items()
    assert all(isinstance(i, list) for i in [keys, values, items])
    assert keys == [i for i in hg]
    assert values == [hg[i] for i in hg]
    assert items == [(k, v) for k, v in zip(keys, values)]
