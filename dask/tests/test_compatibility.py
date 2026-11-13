import builtins
import importlib
import sys

import pytest
from packaging.version import InvalidVersion, Version

import dask
from dask._compatibility import importlib_metadata


def test_version_falls_back_to_distribution_metadata(monkeypatch):
    """A missing ``dask._version`` must not leave ``dask.__version__`` unparseable.

    ``dask._version`` is written by setuptools-scm at install time. A cached install
    (e.g. a pixi/conda build) can skip it, which used to leave ``dask.__version__``
    set to the literal string ``"unknown"``. Anything that parses the version, such as
    xarray's ``DuckArrayModule``, then raises ``InvalidVersion``. The fallback must use
    the distribution metadata instead.
    """
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "dask._version":
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "dask._version", raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    try:
        reloaded = importlib.reload(dask)
        # Must be a parseable PEP 440 version, not the "unknown" sentinel.
        Version(reloaded.__version__)
        assert reloaded.__version__ == importlib_metadata.version("dask")
    except InvalidVersion:  # pragma: no cover
        pytest.fail(f"dask.__version__ is not a valid version: {dask.__version__!r}")
    finally:
        monkeypatch.undo()
        importlib.reload(dask)
