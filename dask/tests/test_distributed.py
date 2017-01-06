import pytest
distributed = pytest.importorskip('distributed')

from distributed.utils_test import loop, cluster
from contextlib import contextmanager


@contextmanager
def loop_ctx():
    return loop()


def distributed_context(func, *args, **kwargs):
    with cluster() as (s, [a, b]):
        with loop_ctx() as l:
            with distributed.Client(('127.0.0.1', s['port']), loop=l) as c:  # noqa: F841
                func(*args, **kwargs)


def test_can_import_client():
    from dask.distributed import Client # noqa: F401


def test_to_hdf_distributed():
    from ..dataframe.io.tests.test_hdf import test_to_hdf
    distributed_context(test_to_hdf)


@pytest.mark.parametrize('npartitions', [1, 4, 10])
def test_to_hdf_scheduler_distributed(npartitions):
    from ..dataframe.io.tests.test_hdf import test_to_hdf_schedulers
    distributed_context(test_to_hdf_schedulers, None, npartitions)
