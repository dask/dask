import pytest

np = pytest.importorskip('numpy')
pytest.importorskip('IPython.parallel')

import numpy as np
from numpy.testing import assert_array_almost_equal

from dask.distributed import dask_client_from_ipclient
import dask.array as da


def test_dask_client_from_ipclient():
    from IPython.parallel import Client
    c = Client()
    dask_client = dask_client_from_ipclient(c)

    # data
    a = np.arange(100).reshape(10, 10)
    d = da.from_array(a, ((5, 5), (5, 5)))

    # test array.mean
    expected = a.mean(axis=0)
    d1 = d.mean(axis=0)
    result = d1.compute(get=dask_client.get)
    assert_array_almost_equal(result, expected)

    # test ghosting
    d2 = da.ghost.ghost(d, depth=1, boundary='reflect')
    d3 = da.ghost.trim_internal(d2, {0: 1, 1: 1})
    result1 = d3.compute(get=dask_client.get)
    assert_array_almost_equal(result1, a)

    # close the workers
    dask_client.close(close_workers=True, close_scheduler=True)
