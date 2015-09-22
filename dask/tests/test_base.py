import tempfile
import os
import shutil
import pytest

pytest.importorskip('toolz')
from toolz import compose, partial, curry

import dask
from dask.base import compute, tokenize, normalize_token, normalize_function
from dask.utils import raises


def test_normalize():
    assert normalize_token((1, 2, 3)) == (1, 2, 3)
    assert normalize_token('a') == 'a'
    assert normalize_token({'a': 1, 'b': 2, 'c': 3}) ==\
            (('a', 1), ('b', 2), ('c', 3))


def test_normalize_function():
    def f1(a, b, c=1):
        pass
    cf1 = curry(f1)
    def f2(a, b=1, c=2):
        pass
    def f3(a):
        pass
    assert normalize_function(f2) == str(f2)
    f = lambda a: a
    assert normalize_function(f) == str(f)
    comp = compose(partial(f2, b=2), f3)
    assert normalize_function(comp) == ((str(f2), (), (('b', 2),)), str(f3))
    assert normalize_function(cf1) == (str(f1), (), ())
    assert normalize_function(cf1(2, c=2)) == (str(f1), (2,), (('c', 2),))
    assert normalize_token(cf1) == normalize_function(cf1)


def test_tokenize():
    a = (1, 2, 3)
    b = {'a': 1, 'b': 2, 'c': 3}
    assert tokenize(a) == '4889c6ccd7099fc2fd19f4be468fcfa0'
    assert tokenize(a, b) == tokenize(normalize_token(a), normalize_token(b))


def test_tokenize_numpy_array_consistent_on_values():
    np = pytest.importorskip('numpy')
    assert tokenize(np.random.RandomState(1234).random_sample(1000)) == \
           tokenize(np.random.RandomState(1234).random_sample(1000))


def test_tokenize_numpy_array_supports_uneven_sizes():
    np = pytest.importorskip('numpy')
    tokenize(np.random.random(7).astype(dtype='i2'))


def test_tokenize_discontiguous_numpy_array():
    np = pytest.importorskip('numpy')
    tokenize(np.random.random(8)[::2])


def test_tokenize_numpy_datetime():
    np = pytest.importorskip('numpy')
    tokenize(np.array(['2000-01-01T12:00:00'], dtype='M8[ns]'))


def test_tokenize_numpy_scalar():
    np = pytest.importorskip('numpy')
    tokenize(np.array(1.0, dtype='f8'))


def test_tokenize_numpy_array_on_object_dtype():
    np = pytest.importorskip('numpy')
    assert tokenize(np.array(['a', 'aa', 'aaa'], dtype=object)) == \
           tokenize(np.array(['a', 'aa', 'aaa'], dtype=object))
    assert tokenize(np.array(['a', None, 'aaa'], dtype=object)) == \
           tokenize(np.array(['a', None, 'aaa'], dtype=object))
    assert tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object)) == \
           tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object))


def test_tokenize_pandas():
    a = pd.DataFrame({'x': [1, 2, 3], 'y': ['4', 'asd', None]}, index=[1, 2, 3])
    b = pd.DataFrame({'x': [1, 2, 3], 'y': ['4', 'asd', None]}, index=[1, 2, 3])

    assert tokenize(a) == tokenize(b)
    b.index.name = 'foo'
    assert tokenize(a) != tokenize(b)

    a = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'a']})
    b = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'a']})
    a['z'] = a.y.astype('category')
    assert tokenize(a) != tokenize(b)
    b['z'] = a.y.astype('category')
    assert tokenize(a) == tokenize(b)


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
        x.visualize(filename=os.path.join(d, 'mydask.pdf'))
        assert os.path.exists(os.path.join(d, 'mydask.pdf'))
    finally:
        shutil.rmtree(d)
