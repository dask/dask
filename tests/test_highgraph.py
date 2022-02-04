import os
import xml.etree.ElementTree
from collections.abc import Set

import pytest

import dask
from dask.blockwise import Blockwise, blockwise_token
from dask.highlevelgraph import HighLevelGraph, Layer, MaterializedLayer, to_graphviz
from dask.utils_test import inc


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


def test_repr_html_hlg_layers():
    pytest.importorskip("jinja2")
    hg = HighLevelGraph(
        {"a": {"a": 1, ("a", 0): 2, "b": 3}, "b": {"c": 4}},
        {"a": set(), "b": set()},
    )
    assert xml.etree.ElementTree.fromstring(hg._repr_html_()) is not None
    for layer in hg.layers.values():
        assert xml.etree.ElementTree.fromstring(layer._repr_html_()) is not None


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
    packed_anno = layer.__dask_distributed_annotations_pack__()
    annotations = {}
    Layer.__dask_distributed_annotations_unpack__(
        annotations, packed_anno, layer.keys()
    )
    assert annotations == {"workers": {"n": ("alice",)}}


def test_materializedlayer_cull_preserves_annotations():
    layer = MaterializedLayer(
        {"a": 42, "b": 3.14},
        annotations={"foo": "bar"},
    )

    culled_layer, _ = layer.cull({"a"}, [])
    assert len(culled_layer) == 1
    assert culled_layer.annotations == {"foo": "bar"}


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


def test_len_does_not_materialize():
    a = {"x": 1}
    b = Blockwise(
        output="b",
        output_indices=tuple("ij"),
        dsk={"b": [[blockwise_token(0)]]},
        indices=(),
        numblocks={},
        new_axes={"i": (1, 1, 1), "j": (1, 1)},
    )
    assert len(b) == len(b.get_output_keys())

    layers = {"a": a, "b": b}
    dependencies = {"a": set(), "b": {"a"}}
    hg = HighLevelGraph(layers, dependencies)

    assert hg.layers["a"].is_materialized()
    assert not hg.layers["b"].is_materialized()

    assert len(hg) == len(a) + len(b) == 7

    assert not hg.layers["b"].is_materialized()


def test_node_tooltips_exist():
    da = pytest.importorskip("dask.array")
    pytest.importorskip("graphviz")

    a = da.ones((1000, 1000), chunks=(100, 100))
    b = a + a.T
    c = b.sum(axis=1)

    hg = c.dask
    g = to_graphviz(hg)

    for layer in g.body:
        if "label" in layer:
            assert "tooltip" in layer
            start = layer.find('tooltip="') + len('tooltip="')
            end = layer.find('"', start)
            tooltip = layer[start:end]
            assert len(tooltip) > 0
