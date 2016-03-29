# -*- coding: utf-8 -*-

import tempfile
import os
import shutil
import pytest
from operator import add, mul
import sys
from uuid import uuid4

import dask
from dask.threaded import get as threaded_get
from dask.base import (compute, tokenize, normalize_token, normalize_function,
                       is_dask_collection, visualize)
from dask.utils import raises, tmpfile, ignoring

from dask.compatibility import unicode


class Duck(object):
    """Duck-typed dask collection"""
    def __init__(self, val):
        self.key = str(uuid4())
        self.val = val

    _dask_graph_ = lambda self: {self.key: self.val}
    _dask_keys_ = lambda self: self.key
    _dask_finalize_ = staticmethod(lambda x: x)
    _dask_optimize_ = staticmethod(lambda d, k, **kws: d)
    _dask_default_get_ = staticmethod(threaded_get)


def test_is_dask_collection():
    duck = Duck(1)
    assert is_dask_collection(duck)
    assert not is_dask_collection("foo")
    db = pytest.importorskip("dask.bag")
    assert is_dask_collection(db.from_sequence([1, 2, 3], npartitions=2))


def test_normalize_function():
    toolz = pytest.importorskip('toolz')
    def f1(a, b, c=1):
        pass
    cf1 = toolz.curry(f1)
    def f2(a, b=1, c=2):
        pass
    def f3(a):
        pass
    assert normalize_function(f2) == str(f2)
    f = lambda a: a
    assert normalize_function(f) == str(f)
    comp = toolz.compose(toolz.partial(f2, b=2), f3)
    assert normalize_function(comp) == ((str(f2), (), (('b', 2),)), str(f3))
    assert normalize_function(cf1) == (str(f1), (), ())
    assert normalize_function(cf1(2, c=2)) == (str(f1), (2,), (('c', 2),))
    assert normalize_token(cf1) == normalize_function(cf1)


def test_tokenize():
    a = (1, 2, 3)
    assert isinstance(tokenize(a), (str, bytes))


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
    assert tokenize(np.array(1.0, dtype='f8')) == tokenize(np.array(1.0, dtype='f8'))
    assert (tokenize(np.array([(1, 2)], dtype=[('a', 'i4'), ('b', 'i8')])[0])
         == tokenize(np.array([(1, 2)], dtype=[('a', 'i4'), ('b', 'i8')])[0]))


def test_tokenize_numpy_array_on_object_dtype():
    np = pytest.importorskip('numpy')
    assert tokenize(np.array(['a', 'aa', 'aaa'], dtype=object)) == \
           tokenize(np.array(['a', 'aa', 'aaa'], dtype=object))
    assert tokenize(np.array(['a', None, 'aaa'], dtype=object)) == \
           tokenize(np.array(['a', None, 'aaa'], dtype=object))
    assert tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object)) == \
           tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object))
    if sys.version_info[0] == 2:
        assert tokenize(np.array([unicode("Rebeca Alón",encoding="utf-8")], dtype=object)) == \
               tokenize(np.array([unicode("Rebeca Alón",encoding="utf-8")], dtype=object))



def test_tokenize_numpy_memmap():
    np = pytest.importorskip('numpy')
    with tmpfile('.npy') as fn:
        x = np.arange(5)
        np.save(fn, x)
        y = tokenize(np.load(fn, mmap_mode='r'))

    with tmpfile('.npy') as fn:
        x = np.arange(5)
        np.save(fn, x)
        z = tokenize(np.load(fn, mmap_mode='r'))

    assert y != z


def test_normalize_builtins():
    for i in [1, 1.1, '1', slice(1, 2, 3)]:
        assert normalize_token(i) is i


def test_normalize_duck():
    duck = Duck(1)
    assert normalize_token(duck) == ('Duck', duck._dask_keys_())


def test_tokenize_pandas():
    pd = pytest.importorskip('pandas')
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


def test_tokenize_kwargs():
    assert tokenize(5, x=1) == tokenize(5, x=1)
    assert tokenize(5) != tokenize(5, x=1)
    assert tokenize(5, x=1) != tokenize(5, x=2)
    assert tokenize(5, x=1) != tokenize(5, y=1)


def test_tokenize_same_repr():
    class Foo(object):
        def __init__(self, x):
            self.x = x
        def __repr__(self):
            return 'a foo'

    assert tokenize(Foo(1)) != tokenize(Foo(2))


def test_tokenize_sequences():
    np = pytest.importorskip('numpy')
    assert tokenize([1]) != tokenize([2])
    assert tokenize([1]) != tokenize((1,))
    assert tokenize([1]) == tokenize([1])

    x = np.arange(2000)  # long enough to drop information in repr
    y = np.arange(2000)
    y[1000] = 0  # middle isn't printed in repr
    assert tokenize([x]) != tokenize([y])


def test_tokenize_ordered_dict():
    with ignoring(ImportError):
        from collections import OrderedDict
        a = OrderedDict([('a', 1), ('b', 2)])
        b = OrderedDict([('a', 1), ('b', 2)])
        c = OrderedDict([('b', 2), ('a', 1)])

        assert tokenize(a) == tokenize(b)
        assert tokenize(a) != tokenize(c)


def test_compute_duck():
    duck = Duck(1)
    assert compute(duck) == (1,)
    da = pytest.importorskip('dask.array')
    x = da.arange(5, chunks=2)
    out1, out2 = compute(duck, x)
    assert out1 == 1
    assert (out2 == x.compute()).all()


def test_compute_array():
    da = pytest.importorskip('dask.array')
    import numpy as np
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    darr1 = darr + 1
    darr2 = darr + 2
    out1, out2 = compute(darr1, darr2)
    assert np.allclose(out1, arr + 1)
    assert np.allclose(out2, arr + 2)


def test_compute_dataframe():
    dd = pytest.importorskip('dask.dataframe')
    import pandas as pd
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf1 = ddf.a + 1
    ddf2 = ddf.a + ddf.b
    out1, out2 = compute(ddf1, ddf2)
    pd.util.testing.assert_series_equal(out1, df.a + 1)
    pd.util.testing.assert_series_equal(out2, df.a + df.b)


def test_compute_array_dataframe():
    da = pytest.importorskip('dask.array')
    dd = pytest.importorskip('dask.dataframe')
    import numpy as np
    import pandas as pd
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5)) + 1
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2).a + 2
    arr_out, df_out = compute(darr, ddf)
    assert np.allclose(arr_out, arr + 1)
    pd.util.testing.assert_series_equal(df_out, df.a + 2)


def test_compute_array_bag():
    db = pytest.importorskip('dask.bag')
    da = pytest.importorskip('dask.array')
    import numpy as np
    x = da.arange(5, chunks=2)
    b = db.from_sequence([1, 2, 3])

    assert raises(ValueError, lambda: compute(x, b))

    xx, bb = compute(x, b, get=dask.async.get_sync)
    assert np.allclose(xx, np.arange(5))
    assert bb == [1, 2, 3]


def test_compute_with_literal():
    da = pytest.importorskip('dask.array')
    x = da.arange(5, chunks=2)
    y = 10

    xx, yy = compute(x, y)
    assert (xx == x.compute()).all()
    assert yy == y

    assert compute(5) == (5,)


def test_visualize():
    da = pytest.importorskip('dask.array')
    pytest.importorskip('graphviz')
    d = tempfile.mkdtemp()
    try:
        x = da.arange(5, chunks=2)
        x.visualize(filename=os.path.join(d, 'mydask'))
        assert os.path.exists(os.path.join(d, 'mydask.png'))
        x.visualize(filename=os.path.join(d, 'mydask.pdf'))
        assert os.path.exists(os.path.join(d, 'mydask.pdf'))
        visualize(x, 1, 2, filename=os.path.join(d, 'mydask.png'))
        assert os.path.exists(os.path.join(d, 'mydask.png'))
        dsk = {'a': 1, 'b': (add, 'a', 2), 'c': (mul, 'a', 1)}
        visualize(x, dsk, filename=os.path.join(d, 'mydask.png'))
        assert os.path.exists(os.path.join(d, 'mydask.png'))
    finally:
        shutil.rmtree(d)


def test_visualize_duck():
    pytest.importorskip('graphviz')
    d = tempfile.mkdtemp()
    try:
        duck = Duck(1)
        visualize(duck, filename=os.path.join(d, 'mydask'))
        assert os.path.exists(os.path.join(d, 'mydask.png'))
    finally:
        shutil.rmtree(d)
