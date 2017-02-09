# -*- coding: utf-8 -*-

import os
import pytest
from operator import add, mul
import subprocess
import sys

import dask
from dask import delayed
from dask.base import (compute, tokenize, normalize_token, normalize_function,
                       visualize, persist)
from dask.delayed import Delayed
from dask.utils import tmpdir, tmpfile, ignoring
from dask.utils_test import inc, dec
from dask.compatibility import unicode


def import_or_none(path):
    with ignoring():
        return pytest.importorskip(path)
    return None


tz = pytest.importorskip('toolz')
da = import_or_none('dask.array')
db = import_or_none('dask.bag')
dd = import_or_none('dask.dataframe')
np = import_or_none('numpy')
pd = import_or_none('pandas')


def test_normalize_function():

    def f1(a, b, c=1):
        pass

    def f2(a, b=1, c=2):
        pass

    def f3(a):
        pass

    assert normalize_function(f2)

    assert normalize_function(lambda a: a)

    assert (normalize_function(tz.partial(f2, b=2)) ==
            normalize_function(tz.partial(f2, b=2)))

    assert (normalize_function(tz.partial(f2, b=2)) !=
            normalize_function(tz.partial(f2, b=3)))

    assert (normalize_function(tz.partial(f1, b=2)) !=
            normalize_function(tz.partial(f2, b=2)))

    assert (normalize_function(tz.compose(f2, f3)) ==
            normalize_function(tz.compose(f2, f3)))

    assert (normalize_function(tz.compose(f2, f3)) !=
            normalize_function(tz.compose(f2, f1)))

    assert normalize_function(tz.curry(f2)) == normalize_function(tz.curry(f2))
    assert normalize_function(tz.curry(f2)) != normalize_function(tz.curry(f1))
    assert (normalize_function(tz.curry(f2, b=1)) ==
            normalize_function(tz.curry(f2, b=1)))
    assert (normalize_function(tz.curry(f2, b=1)) !=
            normalize_function(tz.curry(f2, b=2)))


def test_tokenize():
    a = (1, 2, 3)
    assert isinstance(tokenize(a), (str, bytes))


@pytest.mark.skipif('not np')
def test_tokenize_numpy_array_consistent_on_values():
    assert (tokenize(np.random.RandomState(1234).random_sample(1000)) ==
            tokenize(np.random.RandomState(1234).random_sample(1000)))


@pytest.mark.skipif('not np')
def test_tokenize_numpy_array_supports_uneven_sizes():
    tokenize(np.random.random(7).astype(dtype='i2'))


@pytest.mark.skipif('not np')
def test_tokenize_discontiguous_numpy_array():
    tokenize(np.random.random(8)[::2])


@pytest.mark.skipif('not np')
def test_tokenize_numpy_datetime():
    tokenize(np.array(['2000-01-01T12:00:00'], dtype='M8[ns]'))


@pytest.mark.skipif('not np')
def test_tokenize_numpy_scalar():
    assert tokenize(np.array(1.0, dtype='f8')) == tokenize(np.array(1.0, dtype='f8'))
    assert (tokenize(np.array([(1, 2)], dtype=[('a', 'i4'), ('b', 'i8')])[0]) ==
            tokenize(np.array([(1, 2)], dtype=[('a', 'i4'), ('b', 'i8')])[0]))


@pytest.mark.skipif('not np')
def test_tokenize_numpy_array_on_object_dtype():
    assert (tokenize(np.array(['a', 'aa', 'aaa'], dtype=object)) ==
            tokenize(np.array(['a', 'aa', 'aaa'], dtype=object)))
    assert (tokenize(np.array(['a', None, 'aaa'], dtype=object)) ==
            tokenize(np.array(['a', None, 'aaa'], dtype=object)))
    assert (tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object)) ==
            tokenize(np.array([(1, 'a'), (1, None), (1, 'aaa')], dtype=object)))
    if sys.version_info[0] == 2:
        assert (tokenize(np.array([unicode("Rebeca Alón", encoding="utf-8")], dtype=object)) ==
                tokenize(np.array([unicode("Rebeca Alón", encoding="utf-8")], dtype=object)))


@pytest.mark.skipif('not np')
def test_tokenize_numpy_memmap():
    with tmpfile('.npy') as fn:
        x = np.arange(5)
        np.save(fn, x)
        y = tokenize(np.load(fn, mmap_mode='r'))

    with tmpfile('.npy') as fn:
        x = np.arange(5)
        np.save(fn, x)
        z = tokenize(np.load(fn, mmap_mode='r'))

    assert y != z

    with tmpfile('.npy') as fn:
        x = np.random.normal(size=(10, 10))
        np.save(fn, x)
        mm = np.load(fn, mmap_mode='r')
        mm2 = np.load(fn, mmap_mode='r')
        a = tokenize(mm[0, :])
        b = tokenize(mm[1, :])
        c = tokenize(mm[0:3, :])
        d = tokenize(mm[:, 0])
        assert len(set([a, b, c, d])) == 4
        assert tokenize(mm) == tokenize(mm2)
        assert tokenize(mm[1, :]) == tokenize(mm2[1, :])


@pytest.mark.skipif('not np')
def test_tokenize_numpy_memmap_no_filename():
    # GH 1562:
    with tmpfile('.npy') as fn1, tmpfile('.npy') as fn2:
        x = np.arange(5)
        np.save(fn1, x)
        np.save(fn2, x)

        a = np.load(fn1, mmap_mode='r')
        b = a + a
        assert tokenize(b) == tokenize(b)


def test_normalize_base():
    for i in [1, 1.1, '1', slice(1, 2, 3)]:
        assert normalize_token(i) is i


@pytest.mark.skipif('not pd')
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


@pytest.mark.skipif('not np')
def test_tokenize_sequences():
    assert tokenize([1]) != tokenize([2])
    assert tokenize([1]) != tokenize((1,))
    assert tokenize([1]) == tokenize([1])

    x = np.arange(2000)  # long enough to drop information in repr
    y = np.arange(2000)
    y[1000] = 0  # middle isn't printed in repr
    assert tokenize([x]) != tokenize([y])


def test_tokenize_dict():
    assert tokenize({'x': 1, 1: 'x'}) == tokenize({'x': 1, 1: 'x'})


def test_tokenize_set():
    assert tokenize({1, 2, 'x', (1, 'x')}) == tokenize({1, 2, 'x', (1, 'x')})


def test_tokenize_ordered_dict():
    with ignoring(ImportError):
        from collections import OrderedDict
        a = OrderedDict([('a', 1), ('b', 2)])
        b = OrderedDict([('a', 1), ('b', 2)])
        c = OrderedDict([('b', 2), ('a', 1)])

        assert tokenize(a) == tokenize(b)
        assert tokenize(a) != tokenize(c)


@pytest.mark.skipif('not np')
def test_tokenize_object_array_with_nans():
    a = np.array([u'foo', u'Jos\xe9', np.nan], dtype='O')
    assert tokenize(a) == tokenize(a)


@pytest.mark.skipif('not db')
def test_compute_no_opt():
    # Bag does `fuse` by default. Test that with `optimize_graph=False` that
    # doesn't get called. We check this by using a callback to track the keys
    # that are computed.
    from dask.callbacks import Callback
    b = db.from_sequence(range(100), npartitions=4)
    add1 = tz.partial(add, 1)
    mul2 = tz.partial(mul, 2)
    o = b.map(add1).map(mul2)
    # Check that with the kwarg, the optimization doesn't happen
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        o.compute(get=dask.get, optimize_graph=False)
    assert len([k for k in keys if 'mul' in k[0]]) == 4
    assert len([k for k in keys if 'add' in k[0]]) == 4
    # Check that without the kwarg, the optimization does happen
    keys = []
    with Callback(pretask=lambda key, *args: keys.append(key)):
        o.compute(get=dask.get)
    # Names of fused tasks have been merged, and the original key is an alias.
    # Otherwise, the lengths below would be 4 and 0.
    assert len([k for k in keys if 'mul' in k[0]]) == 8
    assert len([k for k in keys if 'add' in k[0]]) == 4
    assert len([k for k in keys if 'add-map-mul' in k[0]]) == 4  # See? Renamed


@pytest.mark.skipif('not da')
def test_compute_array():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5))
    darr1 = darr + 1
    darr2 = darr + 2
    out1, out2 = compute(darr1, darr2)
    assert np.allclose(out1, arr + 1)
    assert np.allclose(out2, arr + 2)


@pytest.mark.skipif('not da')
def test_persist_array():
    from dask.array.utils import assert_eq
    arr = np.arange(100).reshape((10, 10))
    x = da.from_array(arr, chunks=(5, 5))
    x = (x + 1) - x.mean(axis=0)
    y = x.persist()

    assert_eq(x, y)
    assert set(y.dask).issubset(x.dask)
    assert len(y.dask) == y.npartitions


@pytest.mark.skipif('not dd')
def test_compute_dataframe():
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2)
    ddf1 = ddf.a + 1
    ddf2 = ddf.a + ddf.b
    out1, out2 = compute(ddf1, ddf2)
    pd.util.testing.assert_series_equal(out1, df.a + 1)
    pd.util.testing.assert_series_equal(out2, df.a + df.b)


@pytest.mark.skipif('not dd or not da')
def test_compute_array_dataframe():
    arr = np.arange(100).reshape((10, 10))
    darr = da.from_array(arr, chunks=(5, 5)) + 1
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [5, 5, 3, 3]})
    ddf = dd.from_pandas(df, npartitions=2).a + 2
    arr_out, df_out = compute(darr, ddf)
    assert np.allclose(arr_out, arr + 1)
    pd.util.testing.assert_series_equal(df_out, df.a + 2)


@pytest.mark.skipif('not da or not db')
def test_compute_array_bag():
    x = da.arange(5, chunks=2)
    b = db.from_sequence([1, 2, 3])

    pytest.raises(ValueError, lambda: compute(x, b))

    xx, bb = compute(x, b, get=dask.async.get_sync)
    assert np.allclose(xx, np.arange(5))
    assert bb == [1, 2, 3]


@pytest.mark.skipif('not da')
def test_compute_with_literal():
    x = da.arange(5, chunks=2)
    y = 10

    xx, yy = compute(x, y)
    assert (xx == x.compute()).all()
    assert yy == y

    assert compute(5) == (5,)


def test_compute_nested():
    a = delayed(1) + 5
    b = a + 1
    c = a + 2
    assert (compute({'a': a, 'b': [1, 2, b]}, (c, 2)) ==
            ({'a': 6, 'b': [1, 2, 7]}, (8, 2)))

    res = compute([a, b], c, traverse=False)
    assert res[0][0] is a
    assert res[0][1] is b
    assert res[1] == 8


@pytest.mark.skipif('not da')
@pytest.mark.skipif(sys.flags.optimize == 2,
                    reason="graphviz exception with Python -OO flag")
def test_visualize():
    pytest.importorskip('graphviz')
    with tmpdir() as d:
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


def test_use_cloudpickle_to_tokenize_functions_in__main__():
    import sys
    from textwrap import dedent

    defn = dedent("""
    def inc():
        return x
    """)

    __main__ = sys.modules['__main__']
    exec(compile(defn, '<test>', 'exec'), __main__.__dict__)
    f = __main__.inc

    t = normalize_token(f)
    assert b'__main__' not in t


def test_optimizations_keyword():
    def inc_to_dec(dsk, keys):
        for key in dsk:
            if dsk[key][0] == inc:
                dsk[key] = (dec,) + dsk[key][1:]
        return dsk

    x = dask.delayed(inc)(1)
    assert x.compute() == 2

    with dask.set_options(optimizations=[inc_to_dec]):
        assert x.compute() == 0

    assert x.compute() == 2


def test_default_imports():
    """
    Startup time: `import dask` should not import too many modules.
    """
    code = """if 1:
        import dask
        import sys

        print(sorted(sys.modules))
        """

    out = subprocess.check_output([sys.executable, '-c', code])
    modules = set(eval(out.decode()))
    assert 'dask' in modules
    blacklist = ['dask.array', 'dask.dataframe', 'numpy', 'pandas',
                 'partd', 's3fs', 'distributed']
    for mod in blacklist:
        assert mod not in modules


def test_persist_literals():
    assert persist(1, 2, 3) == (1, 2, 3)


def test_persist_delayed():
    x1 = delayed(1)
    x2 = delayed(inc)(x1)
    x3 = delayed(inc)(x2)

    xx, = persist(x3)
    assert isinstance(xx, Delayed)
    assert xx.key == x3.key
    assert len(xx.dask) == 1

    assert x3.compute() == xx.compute()


@pytest.mark.skipif('not da or not db')
def test_persist_array_bag():
    x = da.arange(5, chunks=2) + 1
    b = db.from_sequence([1, 2, 3]).map(inc)

    with pytest.raises(ValueError):
        persist(x, b)

    xx, bb = persist(x, b, get=dask.async.get_sync)

    assert isinstance(xx, da.Array)
    assert isinstance(bb, db.Bag)

    assert xx.name == x.name
    assert bb.name == b.name
    assert len(xx.dask) == xx.npartitions < len(x.dask)
    assert len(bb.dask) == bb.npartitions < len(b.dask)

    assert np.allclose(x, xx)
    assert list(b) == list(bb)
