import pytest

try:
    import psutil
except ImportError:
    pytest.skip()


def test_import_dask_dataframe(monkeypatch):
    class MockOutput:
        total = None

    def mock_virtual_memory():
        return MockOutput

    monkeypatch.setattr(psutil, "virtual_memory", mock_virtual_memory)
    import dask.dataframe  # noqa F401
