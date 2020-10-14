from functools import partial
import os

import pytest

import dask.array as da
from dask.utils_test import inc
from dask.highlevelgraph import HighLevelGraph, BasicLayer, Layer
from dask.blockwise import Blockwise
from dask.array.utils import assert_eq


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
    assert all(isinstance(layer, Layer) for layer in hg.layers.values())


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


def test_cull():
    a = {"x": 1, "y": (inc, "x")}
    layers = {
        "a": BasicLayer(
            a, dependencies={"x": set(), "y": {"x"}}, global_dependencies=set()
        )
    }
    dependencies = {"a": set()}
    hg = HighLevelGraph(layers, dependencies)

    culled_by_x = hg.cull({"x"})
    assert dict(culled_by_x) == {"x": 1}

    culled_by_y = hg.cull({"y"})
    assert dict(culled_by_y) == a


@pytest.mark.parametrize("inject_dict", [True, False])
def test_map_basic_layers(inject_dict):
    """Check map_basic_layers() by injecting an inc() call"""

    y = da.ones(3, chunks=(3,), dtype="int") + 40

    def inject_inc(dsk):
        assert isinstance(dsk, BasicLayer)
        dsk = dict(dsk)
        k = next(iter(dsk))
        dsk[k] = (inc, dsk[k])
        if inject_dict:
            return dsk  # map_basic_layers() should automatically convert it to a `BasicLayer`
        else:
            return BasicLayer(dsk)

    dsk = y.__dask_graph__()
    y.dask = dsk.map_basic_layers(inject_inc)
    layers = list(y.dask.layers.values())
    assert isinstance(layers[0], BasicLayer)
    assert isinstance(layers[1], Blockwise)
    assert_eq(y, [42] * 3)


@pytest.mark.parametrize("use_layer_map_task", [True, False])
def test_map_tasks(use_layer_map_task):
    """Check map_tasks() by injecting an +1 to the `40` literal"""
    y = da.ones(3, chunks=(3,), dtype="int") + 40

    def plus_one(tasks):
        ret = []
        for t in tasks:
            if t == 40:
                t += 1
            ret.append(t)
        return tuple(ret)

    dsk = y.__dask_graph__()

    if use_layer_map_task:
        # In order to test the default map_tasks() implementation on a Blockwise Layer,
        # we overwrite Blockwise.map_tasks with Layer.map_tasks
        blockwise_layer = list(dsk.layers.values())[1]
        blockwise_layer.map_tasks = partial(Layer.map_tasks, blockwise_layer)

    y.dask = dsk.map_tasks(plus_one)
    assert_eq(y, [42] * 3)
