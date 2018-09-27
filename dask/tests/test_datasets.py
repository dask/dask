import dask
import pytest


def test_mimesis():
    pytest.importorskip('mimesis')

    b = dask.datasets.make_people()
    assert b.take(5)

    assert b.take(3) == b.take(3)
