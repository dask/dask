import os
from collections.abc import Set

import pytest

import dask
from dask.utils_test import inc
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer, Layer


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


def test_keys_values_items_to_dict_methods():
    da = pytest.importorskip("dask.array")
    a = da.ones(10, chunks=(5,))
    b = a + 1
    c = a + 2
    d = b + c
    hg = d.dask

    keys, values, items = hg.keys(), hg.values(), hg.items()
    assert isinstance(keys, Set)
    assert list(keys) == list(hg)
    assert list(values) == [hg[i] for i in hg]
    assert list(items) == list(zip(keys, values))
    assert hg.to_dict() == dict(hg)


def test_getitem():
    hg = HighLevelGraph(
        {"a": {"a": 1, ("a", 0): 2, "b": 3}, "b": {"c": 4}}, {"a": set(), "b": set()}
    )
    # Key is a string and it exists in a layer with the same name
    assert hg["a"] == 1
    # Key is a tuple and the name exists in a layer with the same name
    assert hg["a", 0] == 2
    # Key is in the wrong layer, while the right layer does not contain it
    assert hg["b"] == 3
    # Key is in the wrong layer, while the right layer does not exist
    assert hg["c"] == 4

    for k in ("d", "", 1, ()):
        with pytest.raises(KeyError):
            hg[k]

    class Unhashable:
        __hash__ = None

    for k in (Unhashable(), (Unhashable(),)):
        with pytest.raises(TypeError):
            hg[k]


def test_copy():
    h1 = HighLevelGraph(
        {"a": {"a": "b"}, "b": {"b": 1}},
        {"a": {"b"}, "b": set()},
    )
    h1.get_all_dependencies()
    assert h1.key_dependencies
    h2 = h1.copy()
    for k in ("layers", "dependencies", "key_dependencies"):
        v1 = getattr(h1, k)
        v2 = getattr(h2, k)
        assert v1 is not v2
        assert v1 == v2


def test_cull():
    a = {"x": 1, "y": (inc, "x")}
    hg = HighLevelGraph({"a": a}, {"a": set()})

    culled_by_x = hg.cull({"x"})
    assert dict(culled_by_x) == {"x": 1}

    # parameter is the raw output of __dask_keys__()
    culled_by_y = hg.cull([[["y"]]])
    assert dict(culled_by_y) == a


def test_cull_layers():
    hg = HighLevelGraph(
        {
            "a": {"a1": "d1", "a2": "e1"},
            "b": {"b": "d", "dontcull_b": 1},
            "c": {"dontcull_c": 1},
            "d": {"d": 1, "dontcull_d": 1},
            "e": {"e": 1, "dontcull_e": 1},
        },
        {"a": {"d", "e"}, "b": {"d"}, "c": set(), "d": set(), "e": set()},
    )

    # Deep-copy layers before calling method to test they aren't modified in place
    expect = HighLevelGraph(
        {k: dict(v) for k, v in hg.layers.items() if k != "c"},
        {k: set(v) for k, v in hg.dependencies.items() if k != "c"},
    )

    culled = hg.cull_layers(["a", "b"])

    assert culled.layers == expect.layers
    assert culled.dependencies == expect.dependencies
    for k in culled.layers:
        assert culled.layers[k] is hg.layers[k]
        assert culled.dependencies[k] is hg.dependencies[k]


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


def test_annotation_pack_unpack():
    layer = MaterializedLayer({"n": 42}, annotations={"workers": ("alice",)})
    packed_anno = layer.__dask_distributed_anno_pack__()
    annotations = {}
    Layer.__dask_distributed_annotations_unpack__(
        annotations, packed_anno, layer.keys()
    )
    assert annotations == {"workers": {"n": ("alice",)}}


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


def test_highlevelgraph_dicts_deprecation():
    with pytest.warns(FutureWarning):
        layers = {"a": MaterializedLayer({"x": 1, "y": (inc, "x")})}
        hg = HighLevelGraph(layers, {"a": set()})
        assert hg.dicts == layers
