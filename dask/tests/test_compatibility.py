import pytest

from dask._compatibility import entry_points


def test_deprecation():
    with pytest.warns(DeprecationWarning):
        from dask.compatibility import _EMSCRIPTEN  # noqa


def test_entry_points():
    with pytest.warns(DeprecationWarning):
        assert "pytest" in [ep.name for ep in entry_points(group="console_scripts")]
