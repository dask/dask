import pytest
pytest.importorskip('distributed')

from dask import persist, delayed
from distributed.client import _wait
from distributed.utils_test import gen_cluster, inc


def test_can_import_client():
    from dask.distributed import Client # noqa: F401


@gen_cluster(client=True)
def test_persist(c, s, a, b):
    x = delayed(inc)(1)
    x2, = persist(x)

    yield _wait(x2)
    assert x2.key in a.data or x2.key in b.data

    y = delayed(inc)(10)
    y2, one = persist(y, 1)

    yield _wait(y2)
    assert y2.key in a.data or y2.key in b.data
