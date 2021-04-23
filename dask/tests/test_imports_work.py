import pytest

from dask.dataframe.io.csv import _infer_block_size


def test_import_dask_dataframe(monkeypatch):
    pytest.importorskip("psutil")
    import psutil

    class MockOutput:
        total = None

    def mock_virtual_memory():
        return MockOutput

    monkeypatch.setattr(psutil, "virtual_memory", mock_virtual_memory)
    assert _infer_block_size()
