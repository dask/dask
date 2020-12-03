import os

import pytest

import dask
from dask.utils_test import inc
from dask.highlevelgraph import HighLevelGraph, BasicLayer, Layer


def test_visualize(tmpdir):
    pytest.importorskip("graphviz")
    da = pytest.importorskip("dask.array")
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
    da = pytest.importorskip("dask.array")
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
    layers = {"a": BasicLayer(a)}
    dependencies = {"a": set()}
    hg = HighLevelGraph(layers, dependencies)

    culled_by_x = hg.cull({"x"})
    assert dict(culled_by_x) == {"x": 1}

    culled_by_y = hg.cull({"y"})
    assert dict(culled_by_y) == a


def annot_map_fn(key):
    return key[1:]


@pytest.mark.parametrize(
    "annotation",
    [
        {"worker": "alice"},
        {"block_id": annot_map_fn},
    ],
)
def test_single_annotation(annotation):
    da = pytest.importorskip("dask.array")
    with dask.annotate(**annotation):
        A = da.ones((10, 10), chunks=(5, 5))

    alayer = A.__dask_graph__().layers[A.name]
    assert alayer.annotations == annotation
    assert dask.config.get("annotations", None) is None


def test_multiple_annotations():
    da = pytest.importorskip("dask.array")
    with dask.annotate(block_id=annot_map_fn):
        with dask.annotate(resources={"GPU": 1}):
            A = da.ones((10, 10), chunks=(5, 5))

        B = A + 1

    C = B + 1

    assert dask.config.get("annotations", None) is None

    alayer = A.__dask_graph__().layers[A.name]
    blayer = B.__dask_graph__().layers[B.name]
    clayer = C.__dask_graph__().layers[C.name]
    assert alayer.annotations == {"resources": {"GPU": 1}, "block_id": annot_map_fn}
    assert blayer.annotations == {"block_id": annot_map_fn}
    assert clayer.annotations is None


@pytest.mark.parametrize("flat", [True, False])
def test_blockwise_cull(flat):
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")
    if flat:
        # Simple "flat" mapping between input and
        # outut indices
        x = da.from_array(np.arange(40).reshape((4, 10)), (2, 4)) + 100
    else:
        # Complex mapping between input and output
        # indices (outer product and transpose)
        x = da.from_array(np.arange(10).reshape((10,)), (4,))
        y = da.from_array(np.arange(10).reshape((10,)), (4,))
        x = da.outer(x, y).transpose()

    # Check that blockwise culling results in correct
    # output keys and that full graph is not materialized
    dsk = x.__dask_graph__()
    select = (1, 1)  # Select a single chunk
    keys = {(x._name, *select)}
    dsk_cull = dsk.cull(keys)
    for name, layer in dsk_cull.layers.items():
        if not isinstance(layer, dask.blockwise.Blockwise):
            # The original layer shouldn't be Blockwise if the new one isn't
            assert not isinstance(dsk.layers[name], dask.blockwise.Blockwise)
            continue
        assert isinstance(dsk.layers[name], dask.blockwise.Blockwise)
        assert not layer.is_materialized()
        out_keys = layer.get_output_keys()
        assert out_keys == {(layer.output, *select)}
        assert not layer.is_materialized()
