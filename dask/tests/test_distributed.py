import pytest
pytest.importorskip('distributed')

def test_can_import_executor():
    from dask.distributed import Executor
