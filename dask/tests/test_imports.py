from __future__ import annotations

import importlib

import pytest


@pytest.fixture(autouse=True)
def invalidate_caches():
    importlib.invalidate_caches()


@pytest.mark.parametrize(
    "module",
    [
        "dask",
        "dask.bag",
        "dask.base",
        "dask.delayed",
        "dask.graph_manipulation",
        "dask.layers",
        "dask.multiprocessing",
        "dask.optimization",
        "dask.threaded",
    ],
)
def test_defaults(module):
    __import__(module)


def test_array():
    pytest.importorskip("numpy")
    import dask.array  # noqa: F401


def test_pandas_pyarrow():
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import dask.dataframe  # noqa: F401


def test_bokeh():
    pytest.importorskip("bokeh")
    import dask.diagnostics  # noqa: F401


def test_distributed():
    pytest.importorskip("distributed")
    import dask.distributed  # noqa: F401


def test_version_fallback_without_commit_id():
    """The backwards-compat version import must not drop ``__version__``.

    Some builds (e.g. conda) ship a ``dask._version`` module that has
    ``__version__`` but no ``__commit_id__``.  The two attributes used to be
    imported together, so the ``ImportError`` for the commit id also reset the
    real version to ``"unknown"`` (see issue #12591).
    """
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent("""
        import sys
        import types

        _v = types.ModuleType("dask._version")
        _v.__version__ = "1.2.3"
        sys.modules["dask._version"] = _v

        import dask

        assert dask.__version__ == "1.2.3", dask.__version__
        assert dask.__git_revision__ == "unknown", dask.__git_revision__
        """)
    subprocess.run([sys.executable, "-c", code], check=True)
