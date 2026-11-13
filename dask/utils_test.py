from __future__ import annotations

import contextlib
import importlib
import os
import time
from typing import TYPE_CHECKING

from packaging.version import Version

if TYPE_CHECKING:
    from dask.highlevelgraph import HighLevelGraph, Layer


def inc(x):
    return x + 1


def dec(x):
    return x - 1


def add(x, y):
    return x + y


def slowadd(a, b, delay=0.1):
    time.sleep(delay)
    return a + b


def xdist_worker_port(base_port: int) -> int:
    """Pick a TCP port that does not clash with the other pytest-xdist workers.

    Test modules that bind a fixed port, e.g. to talk to a local server, cannot run in
    parallel unless every worker uses a port of its own.

    Outside of pytest-xdist, or when the worker id does not end with a number,
    ``base_port`` is returned unchanged.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    index = worker.removeprefix("gw")
    return base_port + int(index) if index.isdigit() else base_port


class GetFunctionTestMixin:
    """
    The GetFunctionTestCase class can be imported and used to test foreign
    implementations of the `get` function specification. It aims to enforce all
    known expectations of `get` functions.

    To use the class, inherit from it and override the `get` function. For
    example:

    > from dask.utils_test import GetFunctionTestMixin
    > class TestCustomGet(GetFunctionTestMixin):
         get = staticmethod(myget)

    Note that the foreign `myget` function has to be explicitly decorated as a
    staticmethod.
    """

    def test_get(self):
        d = {":x": 1, ":y": (inc, ":x"), ":z": (add, ":x", ":y")}

        assert self.get(d, ":x") == 1
        assert self.get(d, ":y") == 2
        assert self.get(d, ":z") == 3

    def test_badkey(self):
        d = {":x": 1, ":y": (inc, ":x"), ":z": (add, ":x", ":y")}
        try:
            result = self.get(d, "badkey")
        except KeyError:
            pass
        else:
            msg = "Expected `{}` with badkey to raise KeyError.\n"
            msg += f"Obtained '{result}' instead."
            assert False, msg.format(self.get.__name__)

    def test_nested_badkey(self):
        d = {"x": 1, "y": 2, "z": (sum, ["x", "y"])}

        try:
            result = self.get(d, [["badkey"], "y"])
        except KeyError:
            pass
        else:
            msg = "Expected `{}` with badkey to raise KeyError.\n"
            msg += f"Obtained '{result}' instead."
            assert False, msg.format(self.get.__name__)

    def test_data_not_in_dict_is_ok(self):
        d = {"x": 1, "y": (add, "x", 10)}
        assert self.get(d, "y") == 11

    def test_get_with_list(self):
        d = {"x": 1, "y": 2, "z": (sum, ["x", "y"])}

        assert self.get(d, ["x", "y"]) == (1, 2)
        assert self.get(d, "z") == 3

    def test_get_with_list_top_level(self):
        d = {
            "a": [1, 2, 3],
            "b": "a",
            "c": [1, (inc, 1)],
            "d": [(sum, "a")],
            "e": ["a", "b"],
            "f": [[[(sum, "a"), "c"], (sum, "b")], 2],
        }
        assert self.get(d, "a") == [1, 2, 3]
        assert self.get(d, "b") == [1, 2, 3]
        assert self.get(d, "c") == [1, 2]
        assert self.get(d, "d") == [6]
        assert self.get(d, "e") == [[1, 2, 3], [1, 2, 3]]
        assert self.get(d, "f") == [[[6, [1, 2]], 6], 2]

    def test_get_with_nested_list(self):
        d = {"x": 1, "y": 2, "z": (sum, ["x", "y"])}

        assert self.get(d, [["x"], "y"]) == ((1,), 2)
        assert self.get(d, "z") == 3

    def test_get_works_with_unhashables_in_values(self):
        f = lambda x, y: x + len(y)
        d = {"x": 1, "y": (f, "x", {1})}

        assert self.get(d, "y") == 2

    def test_nested_tasks(self):
        d = {"x": 1, "y": (inc, "x"), "z": (add, (inc, "x"), "y")}

        assert self.get(d, "z") == 4

    def test_get_stack_limit(self):
        d = {f"x{i + 1}": (inc, f"x{i}") for i in range(10000)}
        d["x0"] = 0
        assert self.get(d, "x10000") == 10000

    def test_with_HighLevelGraph(self):
        from dask.highlevelgraph import HighLevelGraph

        layers = {"a": {"x": 1, "y": (inc, "x")}, "b": {"z": (add, (inc, "x"), "y")}}
        dependencies = {"a": (), "b": {"a"}}
        graph = HighLevelGraph(layers, dependencies)
        assert self.get(graph, "z") == 4


def import_or_none(name):
    """Import a module and return it; in case of failure; return None"""
    try:
        return importlib.import_module(name)
    except (ImportError, AttributeError):
        return None


def hlg_layer(hlg: HighLevelGraph, prefix: str) -> Layer:
    "Get the first layer from a HighLevelGraph whose name starts with a prefix"
    for key, lyr in hlg.layers.items():
        if key.startswith(prefix):
            return lyr
    raise KeyError(f"No layer starts with {prefix!r}: {list(hlg.layers)}")


def hlg_layer_topological(hlg: HighLevelGraph, i: int) -> Layer:
    "Get the layer from a HighLevelGraph at position ``i``, topologically"
    return hlg.layers[hlg._toposort_layers()[i]]


def scipy_sparse_matrix_deprecated() -> bool:
    """Whether the installed scipy deprecates the sparse matrix API.

    scipy 2.0 deprecates the sparse matrix classes (``csr_matrix``, ``csc_matrix``,
    ``coo_matrix``, ...) and constructors (``eye``, ``rand``, ...) in favor of the
    sparse array API. See the spmatrix to sparray migration guide:
    https://docs.scipy.org/doc/scipy/reference/sparse.migration_to_sparray.html
    """
    try:
        import scipy
    except ImportError:
        return False
    return Version(scipy.__version__) >= Version("2.0.0.dev0")


@contextlib.contextmanager
def _check_warning(condition: bool, category: type[Warning], message: str):
    """Conditionally check if a warning is raised"""
    if condition:
        import pytest

        with pytest.warns(category, match=message) as ctx:
            yield ctx
    else:
        with contextlib.nullcontext() as ctx:
            yield ctx
