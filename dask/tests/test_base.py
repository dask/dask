import tempfile
import os
import shutil
import pytest

import dask
from dask.base import compute
from dask.utils import raises

da = pytest.importorskip('dask.array')
import numpy as np


def test_compute_array():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    darr1 = darr + 1
    darr2 = darr + 2
    out1, out2 = compute(darr1, darr2)
    assert np.allclose(out1, arr + 1)
    assert np.allclose(out2, arr + 2)


dd = pytest.importorskip('dask.dataframe')
import pandas as pd
from pandas.util.testing import assert_series_equal


def test_compute_dataframe():
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf1 = ddf.a + 1
    ddf2 = ddf.a + ddf.b
    out1, out2 = compute(ddf1, ddf2)
    assert_series_equal(out1, df.a + 1)
    assert_series_equal(out2, df.a + df.b)


def test_compute_array_dataframe():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5)) + 1
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2).a + 2
    arr_out, df_out = compute(darr, ddf)
    assert np.allclose(arr_out, arr + 1)
    assert_series_equal(df_out, df.a + 2)


db = pytest.importorskip('dask.bag')


def test_compute_array_bag():
    x = da.arange(5, chunks=2)
    b = db.from_sequence([1, 2, 3])

    assert raises(ValueError, lambda: compute(x, b))

    xx, bb = compute(x, b, get=dask.async.get_sync)
    assert np.allclose(xx, np.arange(5))
    assert bb == [1, 2, 3]


def test_visualize():
    pytest.importorskip('graphviz')
    try:
        d = tempfile.mkdtemp()
        x = da.arange(5, chunks=2)
        x.visualize(filename=os.path.join(d, 'mydask'))
        assert os.path.exists(os.path.join(d, 'mydask.png'))
    finally:
        shutil.rmtree(d)
