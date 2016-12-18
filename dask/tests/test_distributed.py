import pytest
pytest.importorskip('distributed')


def test_can_import_client():
    from dask.distributed import Client # noqa: F401
