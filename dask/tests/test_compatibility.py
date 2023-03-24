import pytest

from dask.compatibility import entry_points


def test_entry_points():
    with pytest.warns(DeprecationWarning):
        assert "pytest" in [ep.name for ep in entry_points(group="console_scripts")]
