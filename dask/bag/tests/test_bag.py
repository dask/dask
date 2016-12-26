# coding=utf-8
from __future__ import absolute_import, division, print_function

import pytest
import math
import os
import sys
from collections import Iterator
from distutils.version import LooseVersion

import partd
from toolz import merge, join, filter, identity, valmap, groupby, pluck

import dask
import dask.bag as db
from dask.bag.core import (Bag, lazify, lazify_task, map, collect,
                           reduceby, reify, partition, inline_singleton_lists,
                           optimize, from_delayed)
from dask.async import get_sync
from dask.compatibility import BZ2File, GzipFile, PY2
from dask.utils import filetexts, tmpfile, tmpdir, open
from dask.utils_test import inc, add


dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)


def iseven(x):
    return x % 2 == 0


def isodd(x):
    return x % 2 == 1


def test_Bag():
    assert b.name == 'x'
    assert b.npartitions == 3


def test_keys():
    assert sorted(b._keys()) == sorted(dsk.keys())


def test_map():
    c = b.map(inc)
    expected = merge(dsk, dict(((c.name, i), (reify, (map, inc, (b.name, i))))
                               for i in range(b.npartitions)))
    assert c.dask == expected
    assert c.name == b.map(inc).name


def test_map_function_with_multiple_arguments():
    b = db.from_sequence([(1, 10), (2, 20), (3, 30)], npartitions=3)
    assert list(b.map(lambda x, y: x + y).compute(get=dask.get)) == [11, 22, 33]
    assert list(b.map(list).compute()) == [[1, 10], [2, 20], [3, 30]]


class A(object):
    def __init__(self, a, b, c):
        pass


class B(object):
    def __init__(self, a):
        pass


def test_map_with_constructors():
    assert db.from_sequence([[1, 2, 3]]).map(A).compute()
    assert db.from_sequence([1, 2, 3]).map(B).compute()
    assert db.from_sequence([[1, 2, 3]]).map(B).compute()

    failed = False
    try:
        db.from_sequence([[1,]]).map(A).compute()
    except TypeError:
        failed = True
    assert failed


def test_map_with_builtins():
    b = db.from_sequence(range(3))
    assert ' '.join(b.map(str)) == '0 1 2'
    assert b.map(str).map(tuple).compute() == [('0',), ('1',), ('2',)]
    assert b.map(str).map(tuple).map(any).compute() == [True, True, True]

    b2 = b.map(lambda n: [(n, n + 1), (2 * (n - 1), -n)])
    assert b2.map(dict).compute() == [{0: 1, -2: 0}, {1: 2, 0: -1}, {2: -2}]
    assert b.map(lambda n: (n, n + 1)).map(pow).compute() == [0, 1, 8]
    assert b.map(bool).compute() == [False, True, True]
    assert db.from_sequence([(1, 'real'), ('1', 'real')]).map(hasattr).compute() == \
        [True, False]


def test_map_with_kwargs():
    b = db.from_sequence(range(100), npartitions=10)
    assert b.map(lambda x, factor=0: x * factor,
                 factor=2).sum().compute() == 9900.0
    assert b.map(lambda x, total=0: x / total,
                 total=b.sum()).sum().compute() == 1.0
    assert b.map(lambda x, factor=0, total=0: x * factor / total,
                 total=b.sum(),
                 factor=2).sum().compute() == 2.0


def test_filter():
    c = b.filter(iseven)
    expected = merge(dsk, dict(((c.name, i),
                                (reify, (filter, iseven, (b.name, i))))
                               for i in range(b.npartitions)))
    assert c.dask == expected
    assert c.name == b.filter(iseven).name


def test_remove():
    f = lambda x: x % 2 == 0
    c = b.remove(f)
    assert list(c) == [1, 3] * 3
    assert c.name == b.remove(f).name


def test_iter():
    assert sorted(list(b)) == sorted(L)
    assert sorted(list(b.map(inc))) == sorted(list(range(1, 6)) * 3)


@pytest.mark.parametrize('func', [str, repr])
def test_repr(func):
    assert str(b.npartitions) in func(b)
    assert b.name[:5] in func(b)


def test_pluck():
    d = {('x', 0): [(1, 10), (2, 20)],
         ('x', 1): [(3, 30), (4, 40)]}
    b = Bag(d, 'x', 2)
    assert set(b.pluck(0)) == set([1, 2, 3, 4])
    assert set(b.pluck(1)) == set([10, 20, 30, 40])
    assert set(b.pluck([1, 0])) == set([(10, 1), (20, 2), (30, 3), (40, 4)])
    assert b.pluck([1, 0]).name == b.pluck([1, 0]).name


def test_pluck_with_default():
    b = db.from_sequence(['Hello', '', 'World'])
    pytest.raises(IndexError, lambda: list(b.pluck(0)))
    assert list(b.pluck(0, None)) == ['H', None, 'W']
    assert b.pluck(0, None).name == b.pluck(0, None).name
    assert b.pluck(0).name != b.pluck(0, None).name


def test_unzip():
    b = db.from_sequence(range(100)).map(lambda x: (x, x + 1, x + 2))
    one, two, three = b.unzip(3)
    assert list(one) == list(range(100))
    assert list(three) == [i + 2 for i in range(100)]
    assert one.name == b.unzip(3)[0].name
    assert one.name != two.name


def test_fold():
    c = b.fold(add)
    assert c.compute() == sum(L)
    assert c.key == b.fold(add).key

    c2 = b.fold(add, initial=10)
    assert c2.key != c.key
    assert c2.compute() == sum(L) + 10 * b.npartitions
    assert c2.key == b.fold(add, initial=10).key

    c = db.from_sequence(range(5), npartitions=3)

    def binop(acc, x):
        acc = acc.copy()
        acc.add(x)
        return acc

    d = c.fold(binop, set.union, initial=set())
    assert d.compute() == set(c)
    assert d.key == c.fold(binop, set.union, initial=set()).key

    d = db.from_sequence('hello')
    assert set(d.fold(lambda a, b: ''.join([a, b]), initial='').compute()) == set('hello')

    e = db.from_sequence([[1], [2], [3]], npartitions=2)
    with dask.set_options(get=get_sync):
        assert set(e.fold(add, initial=[]).compute()) == set([1, 2, 3])


def test_distinct():
    assert sorted(b.distinct()) == [0, 1, 2, 3, 4]
    assert b.distinct().name == b.distinct().name
    assert 'distinct' in b.distinct().name
    assert b.distinct().count().compute() == 5


def test_frequencies():
    c = b.frequencies()
    assert dict(c) == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}
    c2 = b.frequencies(split_every=2)
    assert dict(c2) == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}
    assert c.name == b.frequencies().name
    assert c.name != c2.name
    assert c2.name == b.frequencies(split_every=2).name


def test_topk():
    assert list(b.topk(4)) == [4, 4, 4, 3]
    c = b.topk(4, key=lambda x: -x)
    assert list(c) == [0, 0, 0, 1]
    c2 = b.topk(4, key=lambda x: -x, split_every=2)
    assert list(c2) == [0, 0, 0, 1]
    assert c.name != c2.name
    assert b.topk(4).name == b.topk(4).name


def test_topk_with_non_callable_key():
    b = db.from_sequence([(1, 10), (2, 9), (3, 8)], npartitions=2)
    assert list(b.topk(2, key=1)) == [(1, 10), (2, 9)]
    assert list(b.topk(2, key=0)) == [(3, 8), (2, 9)]
    assert b.topk(2, key=1).name == b.topk(2, key=1).name


def test_topk_with_multiarg_lambda():
    b = db.from_sequence([(1, 10), (2, 9), (3, 8)], npartitions=2)
    assert list(b.topk(2, key=lambda a, b: b)) == [(1, 10), (2, 9)]


def test_lambdas():
    assert list(b.map(lambda x: x + 1)) == list(b.map(inc))


def test_reductions():
    assert int(b.count()) == 15
    assert int(b.sum()) == 30
    assert int(b.max()) == 4
    assert int(b.min()) == 0
    assert b.any().compute() is True
    assert b.all().compute() is False
    assert b.all().key == b.all().key
    assert b.all().key != b.any().key


def test_reduction_names():
    assert b.sum().name.startswith('sum')
    assert b.reduction(sum, sum).name.startswith('sum')
    assert any(isinstance(k, str) and k.startswith('max')
               for k in b.reduction(sum, max).dask)
    assert b.reduction(sum, sum, name='foo').name.startswith('foo')


def test_tree_reductions():
    b = db.from_sequence(range(12))
    c = b.reduction(sum, sum, split_every=2)
    d = b.reduction(sum, sum, split_every=6)
    e = b.reduction(sum, sum, split_every=5)

    assert c.compute() == d.compute() == e.compute()

    assert len(c.dask) > len(d.dask)

    c = b.sum(split_every=2)
    d = b.sum(split_every=5)

    assert c.compute() == d.compute()
    assert len(c.dask) > len(d.dask)

    assert c.key != d.key
    assert c.key == b.sum(split_every=2).key
    assert c.key != b.sum().key


def test_mean():
    assert b.mean().compute(get=dask.get) == 2.0
    assert float(b.mean()) == 2.0


def test_non_splittable_reductions():
    np = pytest.importorskip('numpy')
    data = list(range(100))
    c = db.from_sequence(data, npartitions=10)
    assert c.mean().compute() == np.mean(data)
    assert c.std().compute(get=dask.get) == np.std(data)


def test_std():
    assert b.std().compute(get=dask.get) == math.sqrt(2.0)
    assert float(b.std()) == math.sqrt(2.0)


def test_var():
    assert b.var().compute(get=dask.get) == 2.0
    assert float(b.var()) == 2.0


def test_join():
    c = b.join([1, 2, 3], on_self=isodd, on_other=iseven)
    assert list(c) == list(join(iseven, [1, 2, 3], isodd, list(b)))
    assert (list(b.join([1, 2, 3], isodd)) ==
            list(join(isodd, [1, 2, 3], isodd, list(b))))
    assert c.name == b.join([1, 2, 3], on_self=isodd, on_other=iseven).name


def test_foldby():
    c = b.foldby(iseven, add, 0, add, 0)
    assert (reduceby, iseven, add, (b.name, 0), 0) in list(c.dask.values())
    assert set(c) == set(reduceby(iseven, lambda acc, x: acc + x, L, 0).items())
    assert c.name == b.foldby(iseven, add, 0, add, 0).name

    c = b.foldby(iseven, lambda acc, x: acc + x)
    assert set(c) == set(reduceby(iseven, lambda acc, x: acc + x, L, 0).items())


def test_map_partitions():
    assert list(b.map_partitions(len)) == [5, 5, 5]
    assert b.map_partitions(len).name == b.map_partitions(len).name
    assert b.map_partitions(lambda a: len(a) + 1).name != b.map_partitions(len).name


def test_map_partitions_with_kwargs():
    b = db.from_sequence(range(100), npartitions=10)
    assert b.map_partitions(
        lambda X, factor=0: [x * factor for x in X],
        factor=2).sum().compute() == 9900.0
    assert b.map_partitions(
        lambda X, total=0: [x / total for x in X],
        total=b.sum()).sum().compute() == 1.0
    assert b.map_partitions(
        lambda X, factor=0, total=0: [x * factor / total for x in X],
        total=b.sum(),
        factor=2).sum().compute() == 2.0


def test_random_sample_size():
    """
    Number of randomly sampled elements are in the expected range.
    """
    a = db.from_sequence(range(1000), npartitions=5)
    # we expect a size of approx. 100, but leave large margins to avoid
    # random failures
    assert 10 < len(list(a.random_sample(0.1, 42))) < 300


def test_random_sample_prob_range():
    """
    Specifying probabilities outside the range [0, 1] raises ValueError.
    """
    a = db.from_sequence(range(50), npartitions=5)
    with pytest.raises(ValueError):
        a.random_sample(-1)
    with pytest.raises(ValueError):
        a.random_sample(1.1)


def test_random_sample_repeated_computation():
    """
    Repeated computation of a defined random sampling operation
    generates identical results.
    """
    a = db.from_sequence(range(50), npartitions=5)
    b = a.random_sample(0.2)
    assert list(b) == list(b)  # computation happens here


def test_random_sample_different_definitions():
    """
    Repeatedly defining a random sampling operation yields different results
    upon computation if no random seed is specified.
    """
    a = db.from_sequence(range(50), npartitions=5)
    assert list(a.random_sample(0.5)) != list(a.random_sample(0.5))
    assert a.random_sample(0.5).name != a.random_sample(0.5).name


def test_random_sample_random_state():
    """
    Sampling with fixed random seed generates identical results.
    """
    a = db.from_sequence(range(50), npartitions=5)
    b = a.random_sample(0.5, 1234)
    c = a.random_sample(0.5, 1234)
    assert list(b) == list(c)


def test_lazify_task():
    task = (sum, (reify, (map, inc, [1, 2, 3])))
    assert lazify_task(task) == (sum, (map, inc, [1, 2, 3]))

    task = (reify, (map, inc, [1, 2, 3]))
    assert lazify_task(task) == task

    a = (reify, (map, inc, (reify, (filter, iseven, 'y'))))
    b = (reify, (map, inc, (filter, iseven, 'y')))
    assert lazify_task(a) == b


f = lambda x: x


def test_lazify():
    a = {'x': (reify, (map, inc, (reify, (filter, iseven, 'y')))),
         'a': (f, 'x'), 'b': (f, 'x')}
    b = {'x': (reify, (map, inc, (filter, iseven, 'y'))),
         'a': (f, 'x'), 'b': (f, 'x')}
    assert lazify(a) == b


def test_inline_singleton_lists():
    inp = {'b': (list, 'a'),
           'c': (f, 'b', 1)}
    out = {'c': (f, (list, 'a'), 1)}
    assert inline_singleton_lists(inp) == out

    out = {'c': (f, 'a', 1)}
    assert optimize(inp, ['c']) == out

    inp = {'b': (list, 'a'),
           'c': (f, 'b', 1),
           'd': (f, 'b', 2)}
    assert inline_singleton_lists(inp) == inp

    inp = {'b': (4, 5)} # doesn't inline constants
    assert inline_singleton_lists(inp) == inp


def test_take():
    assert list(b.take(2)) == [0, 1]
    assert b.take(2) == (0, 1)
    assert isinstance(b.take(2, compute=False), Bag)


def test_take_npartitions():
    assert list(b.take(6, npartitions=2)) == [0, 1, 2, 3, 4, 0]
    assert b.take(6, npartitions=-1) == (0, 1, 2, 3, 4, 0)
    assert b.take(3, npartitions=-1) == (0, 1, 2)
    with pytest.raises(ValueError):
        b.take(1, npartitions=5)


@pytest.mark.skipif(sys.version_info[:2] == (3,3),
                    reason="Python3.3 uses pytest2.7.2, w/o warns method")
def test_take_npartitions_warn():
    with pytest.warns(None):
        b.take(100)

    with pytest.warns(None):
        b.take(7)

    with pytest.warns(None):
        b.take(7, npartitions=2)


def test_map_is_lazy():
    from dask.bag.core import map
    assert isinstance(map(lambda x: x, [1, 2, 3]), Iterator)


def test_can_use_dict_to_make_concrete():
    assert isinstance(dict(b.frequencies()), dict)


def test_from_castra():
    pytest.importorskip('castra')
    pd = pytest.importorskip('pandas')
    dd = pytest.importorskip('dask.dataframe')
    blosc = pytest.importorskip('blosc')
    if LooseVersion(blosc.__version__) == '1.3.0':
        pytest.skip()
    df = pd.DataFrame({'x': list(range(100)),
                       'y': [str(i) for i in range(100)]})
    a = dd.from_pandas(df, 10)

    with tmpfile('.castra') as fn:
        c = a.to_castra(fn)
        default = db.from_castra(c)
        with_columns = db.from_castra(c, 'x')
        with_index = db.from_castra(c, 'x', index=True)
        assert (list(default) == [{'x': i, 'y': str(i)}
                                  for i in range(100)] or
                list(default) == [(i, str(i)) for i in range(100)])
        assert list(with_columns) == list(range(100))
        assert list(with_index) == list(zip(range(100), range(100)))
        assert default.name != with_columns.name != with_index.name
        assert with_index.name == db.from_castra(c, 'x', index=True).name


@pytest.mark.slow
def test_from_url():
    a = db.from_url(['http://google.com', 'http://github.com'])
    assert a.npartitions == 2

    b = db.from_url('http://raw.githubusercontent.com/dask/dask/master/README.rst')
    assert b.npartitions == 1
    assert b'Dask\n' in b.take(10)


def test_read_text():
    with filetexts({'a1.log': 'A\nB', 'a2.log': 'C\nD'}) as fns:
        assert (set(line.strip() for line in db.read_text(fns)) ==
                set('ABCD'))
        assert (set(line.strip() for line in db.read_text('a*.log')) ==
                set('ABCD'))

    pytest.raises(ValueError, lambda: db.read_text('non-existent-*-path'))


def test_read_text_large():
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(('Hello, world!' + os.linesep).encode() * 100)
        b = db.read_text(fn, blocksize=100)
        c = db.read_text(fn)
        assert len(b.dask) > 5
        assert list(map(str, b.str.strip())) == list(map(str, c.str.strip()))

        d = db.read_text([fn], blocksize=100)
        assert list(b) == list(d)


def test_read_text_encoding():
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write((u'你好！' + os.linesep).encode('gb18030') * 100)
        b = db.read_text(fn, blocksize=100, encoding='gb18030')
        c = db.read_text(fn, encoding='gb18030')
        assert len(b.dask) > 5
        assert (list(b.str.strip().map(lambda x: x.encode('utf-8'))) ==
                list(c.str.strip().map(lambda x: x.encode('utf-8'))))

        d = db.read_text([fn], blocksize=100, encoding='gb18030')
        assert list(b) == list(d)


def test_read_text_large_gzip():
    with tmpfile('gz') as fn:
        f = GzipFile(fn, 'wb')
        f.write(b'Hello, world!\n' * 100)
        f.close()

        with pytest.raises(ValueError):
            db.read_text(fn, blocksize=50, linedelimiter='\n')

        c = db.read_text(fn)
        assert c.npartitions == 1


@pytest.mark.slow
def test_from_s3():
    # note we don't test connection modes with aws_access_key and
    # aws_secret_key because these are not on travis-ci
    pytest.importorskip('s3fs')

    five_tips = (u'total_bill,tip,sex,smoker,day,time,size\n',
                 u'16.99,1.01,Female,No,Sun,Dinner,2\n',
                 u'10.34,1.66,Male,No,Sun,Dinner,3\n',
                 u'21.01,3.5,Male,No,Sun,Dinner,3\n',
                 u'23.68,3.31,Male,No,Sun,Dinner,2\n')

    # test compressed data
    e = db.read_text('s3://tip-data/t*.gz', storage_options=dict(anon=True))
    assert e.take(5) == five_tips

    # test all keys in bucket
    c = db.read_text('s3://tip-data/*', storage_options=dict(anon=True))
    assert c.npartitions == 4


def test_from_sequence():
    b = db.from_sequence([1, 2, 3, 4, 5], npartitions=3)
    assert len(b.dask) == 3
    assert set(b) == set([1, 2, 3, 4, 5])


def test_from_long_sequence():
    L = list(range(1001))
    b = db.from_sequence(L)
    assert set(b) == set(L)


def test_product():
    b2 = b.product(b)
    assert b2.npartitions == b.npartitions**2
    assert set(b2) == set([(i, j) for i in L for j in L])

    x = db.from_sequence([1, 2, 3, 4])
    y = db.from_sequence([10, 20, 30])
    z = x.product(y)
    assert set(z) == set([(i, j) for i in [1, 2, 3, 4] for j in [10, 20, 30]])

    assert z.name != b2.name
    assert z.name == x.product(y).name


def test_partition_collect():
    with partd.Pickle() as p:
        partition(identity, range(6), 3, p)
        assert set(p.get(0)) == set([0, 3])
        assert set(p.get(1)) == set([1, 4])
        assert set(p.get(2)) == set([2, 5])

        assert sorted(collect(identity, 0, p, '')) == [(0, [0]), (3, [3])]


def test_groupby():
    c = b.groupby(identity)
    result = dict(c)
    assert result == {0: [0, 0 ,0],
                      1: [1, 1, 1],
                      2: [2, 2, 2],
                      3: [3, 3, 3],
                      4: [4, 4, 4]}
    assert c.npartitions == b.npartitions
    assert c.name == b.groupby(identity).name
    assert c.name != b.groupby(lambda x: x + 1).name


def test_groupby_with_indexer():
    b = db.from_sequence([[1, 2, 3], [1, 4, 9], [2, 3, 4]])
    result = dict(b.groupby(0))
    assert valmap(sorted, result) == {1: [[1, 2, 3], [1, 4, 9]],
                                      2: [[2, 3, 4]]}


def test_groupby_with_npartitions_changed():
    result = b.groupby(lambda x: x, npartitions=1)
    result2 = dict(result)
    assert result2 == {0: [0, 0 ,0],
                       1: [1, 1, 1],
                       2: [2, 2, 2],
                       3: [3, 3, 3],
                       4: [4, 4, 4]}

    assert result.npartitions == 1


def test_concat():
    a = db.from_sequence([1, 2, 3])
    b = db.from_sequence([4, 5, 6])
    c = db.concat([a, b])
    assert list(c) == [1, 2, 3, 4, 5, 6]

    assert c.name == db.concat([a, b]).name
    assert b.concat().name != a.concat().name
    assert b.concat().name == b.concat().name

    b = db.from_sequence([1, 2, 3]).map(lambda x: x * [1, 2, 3])
    assert list(b.concat()) == [1, 2, 3] * sum([1, 2, 3])


def test_concat_after_map():
    a = db.from_sequence([1, 2])
    b = db.from_sequence([4, 5])
    result = db.concat([a.map(inc), b])
    assert list(result) == [2, 3, 4, 5]


def test_args():
    c = b.map(lambda x: x + 1)
    d = Bag(*c._args)

    assert list(c) == list(d)
    assert c.npartitions == d.npartitions


def test_to_dataframe():
    pytest.importorskip('dask.dataframe')
    pd = pytest.importorskip('pandas')
    b = db.from_sequence([(1, 2), (10, 20), (100, 200)], npartitions=2)

    df = b.to_dataframe()
    assert list(df.columns) == list(pd.DataFrame(list(b)).columns)

    df = b.to_dataframe(columns=['a', 'b'])
    assert df.npartitions == b.npartitions
    assert list(df.columns) == ['a', 'b']

    assert df.a.compute().values.tolist() == list(b.pluck(0))
    assert df.b.compute().values.tolist() == list(b.pluck(1))

    b = db.from_sequence([{'a':   1, 'b':   2},
                          {'a':  10, 'b':  20},
                          {'a': 100, 'b': 200}], npartitions=2)

    df2 = b.to_dataframe()

    assert (df2.compute().values == df.compute().values).all()

    assert df2._name == b.to_dataframe()._name
    assert df2._name != df._name

    meta = pd.DataFrame({'a': [1], 'b': [2]}).iloc[0:0]
    df3 = b.to_dataframe(columns=meta)
    assert df2._name == df3._name
    assert (df3.compute().values == df2.compute().values).all()

    b = db.from_sequence([1, 2, 3, 4, 5], npartitions=2)
    df4 = b.to_dataframe()
    assert len(df4.columns) == 1
    assert list(df4.compute()) == list(pd.DataFrame(list(b)))


ext_open = [('gz', GzipFile), ('', open)]
if not PY2:
    ext_open.append(('bz2', BZ2File))


@pytest.mark.parametrize('ext,myopen', ext_open)
def test_to_textfiles(ext, myopen):
    b = db.from_sequence(['abc', '123', 'xyz'], npartitions=2)
    with tmpdir() as dir:
        c = b.to_textfiles(os.path.join(dir, '*.' + ext), compute=False)
        dask.compute(*c, get=dask.get)
        assert os.path.exists(os.path.join(dir, '1.' + ext))

        f = myopen(os.path.join(dir, '1.' + ext), 'rb')
        text = f.read()
        if hasattr(text, 'decode'):
            text = text.decode()
        assert 'xyz' in text
        f.close()


def test_to_textfiles_name_function_preserves_order():
    seq = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p']
    b = db.from_sequence(seq, npartitions=16)
    with tmpdir() as dn:
        b.to_textfiles(dn)

        out = db.read_text(os.path.join(dn, "*"), encoding='ascii').map(str).map(str.strip).compute()
        assert seq == out


@pytest.mark.skipif(sys.version_info[:2] == (3,3), reason="Python3.3 uses pytest2.7.2, w/o warns method")
def test_to_textfiles_name_function_warn():
    seq = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p']
    a = db.from_sequence(seq, npartitions=16)
    with tmpdir() as dn:
        with pytest.warns(None):
            a.to_textfiles(dn, name_function=str)


def test_to_textfiles_encoding():
    b = db.from_sequence([u'汽车', u'苹果', u'天气'], npartitions=2)
    for ext, myopen in [('gz', GzipFile), ('bz2', BZ2File), ('', open)]:
        if ext == 'bz2' and PY2:
            continue
        with tmpdir() as dir:
            c = b.to_textfiles(os.path.join(dir, '*.' + ext), encoding='gb18030', compute=False)
            dask.compute(*c)
            assert os.path.exists(os.path.join(dir, '1.' + ext))

            f = myopen(os.path.join(dir, '1.' + ext), 'rb')
            text = f.read()
            if hasattr(text, 'decode'):
                text = text.decode('gb18030')
            assert u'天气' in text
            f.close()


def test_to_textfiles_inputs():
    B = db.from_sequence(['abc', '123', 'xyz'], npartitions=2)
    with tmpfile() as a:
        with tmpfile() as b:
            B.to_textfiles([a, b])
            assert os.path.exists(a)
            assert os.path.exists(b)

    with tmpdir() as dirname:
        B.to_textfiles(dirname)
        assert os.path.exists(dirname)
        assert os.path.exists(os.path.join(dirname, '0.part'))
    pytest.raises(ValueError, lambda: B.to_textfiles(5))


def test_to_textfiles_endlines():
    b = db.from_sequence(['a', 'b', 'c'], npartitions=1)
    with tmpfile() as fn:
        b.to_textfiles([fn])
        with open(fn, 'r') as f:
            result = f.readlines()
        assert result == ['a\n', 'b\n', 'c']


def test_string_namespace():
    b = db.from_sequence(['Alice Smith', 'Bob Jones', 'Charlie Smith'],
                         npartitions=2)

    assert 'split' in dir(b.str)
    assert 'match' in dir(b.str)

    assert list(b.str.lower()) == ['alice smith', 'bob jones', 'charlie smith']
    assert list(b.str.split(' ')) == [['Alice', 'Smith'],
                                      ['Bob', 'Jones'],
                                      ['Charlie', 'Smith']]
    assert list(b.str.match('*Smith')) == ['Alice Smith', 'Charlie Smith']

    pytest.raises(AttributeError, lambda: b.str.sfohsofhf)
    assert b.str.match('*Smith').name == b.str.match('*Smith').name
    assert b.str.match('*Smith').name != b.str.match('*John').name


def test_string_namespace_with_unicode():
    b = db.from_sequence([u'Alice Smith', u'Bob Jones', 'Charlie Smith'],
                         npartitions=2)
    assert list(b.str.lower()) == ['alice smith', 'bob jones', 'charlie smith']


def test_str_empty_split():
    b = db.from_sequence([u'Alice Smith', u'Bob Jones', 'Charlie Smith'],
                         npartitions=2)
    assert list(b.str.split()) == [['Alice', 'Smith'],
                                   ['Bob', 'Jones'],
                                   ['Charlie', 'Smith']]


def test_map_with_iterator_function():
    b = db.from_sequence([[1, 2, 3], [4, 5, 6]], npartitions=2)

    def f(L):
        for x in L:
            yield x + 1

    c = b.map(f)

    assert list(c) == [[2, 3, 4], [5, 6, 7]]


def test_ensure_compute_output_is_concrete():
    b = db.from_sequence([1, 2, 3])
    result = b.map(lambda x: x + 1).compute()
    assert not isinstance(result, Iterator)


class BagOfDicts(db.Bag):
    def get(self, key, default=None):
        return self.map(lambda d: d.get(key, default))

    def set(self, key, value):
        def setter(d):
            d[key] = value
            return d
        return self.map(setter)


def test_bag_class_extend():
    dictbag = BagOfDicts(*db.from_sequence([{'a': {'b': 'c'}}])._args)
    assert dictbag.get('a').get('b').compute()[0] == 'c'
    assert dictbag.get('a').set('d', 'EXTENSIBILITY!!!').compute()[0] == \
        {'b': 'c', 'd': 'EXTENSIBILITY!!!'}
    assert isinstance(dictbag.get('a').get('b'), BagOfDicts)


def test_gh715():
    bin_data = u'\u20ac'.encode('utf-8')
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(bin_data)
        a = db.read_text(fn)
        assert a.compute()[0] == bin_data.decode('utf-8')


def test_bag_compute_forward_kwargs():
    x = db.from_sequence([1, 2, 3]).map(lambda a: a + 1)
    x.compute(bogus_keyword=10)


def test_to_delayed():
    from dask.delayed import Delayed

    b = db.from_sequence([1, 2, 3, 4, 5, 6], npartitions=3)
    a, b, c = b.map(inc).to_delayed()
    assert all(isinstance(x, Delayed) for x in [a, b, c])
    assert b.compute() == [4, 5]

    b = db.from_sequence([1, 2, 3, 4, 5, 6], npartitions=3)
    t = b.sum().to_delayed()
    assert isinstance(t, Delayed)
    assert t.compute() == 21


def test_from_delayed():
    from dask.delayed import delayed
    a, b, c = delayed([1, 2, 3]), delayed([4, 5, 6]), delayed([7, 8, 9])
    bb = from_delayed([a, b, c])
    assert bb.name == from_delayed([a, b, c]).name

    assert isinstance(bb, Bag)
    assert list(bb) == [1, 2, 3, 4, 5, 6, 7, 8, 9]

    asum_value = delayed(lambda X: sum(X))(a)
    asum_item = db.Item.from_delayed(asum_value)
    assert asum_value.compute() == asum_item.compute() == 6


def test_range():
    for npartitions in [1, 7, 10, 28]:
        b = db.range(100, npartitions=npartitions)
        assert len(b.dask) == npartitions
        assert b.npartitions == npartitions
        assert list(b) == list(range(100))


@pytest.mark.parametrize("npartitions", [1, 7, 10, 28])
def test_zip(npartitions, hi=1000):
    evens = db.from_sequence(range(0, hi, 2), npartitions=npartitions)
    odds = db.from_sequence(range(1, hi, 2), npartitions=npartitions)
    pairs = db.zip(evens, odds)
    assert pairs.npartitions == npartitions
    assert list(pairs) == list(zip(range(0, hi, 2), range(1, hi, 2)))


def test_repartition():
    for x, y in [(10, 5), (7, 3), (5, 1), (5, 4)]:
        b = db.from_sequence(range(20), npartitions=x)
        c = b.repartition(y)

        assert b.npartitions == x
        assert c.npartitions == y
        assert list(b) == c.compute(get=dask.get)

    try:
        b.repartition(100)
    except NotImplementedError as e:
        assert '100' in str(e)


@pytest.mark.skipif('not db.core._implement_accumulate')
def test_accumulate():
    parts = [[1, 2, 3], [4, 5], [], [6, 7]]
    dsk = dict((('test', i), p) for (i, p) in enumerate(parts))
    b = db.Bag(dsk, 'test', len(parts))
    r = b.accumulate(add)
    assert r.name == b.accumulate(add).name
    assert r.name != b.accumulate(add, -1).name
    assert r.compute() == [1, 3, 6, 10, 15, 21, 28]
    assert b.accumulate(add, -1).compute() == [-1, 0, 2, 5, 9, 14, 20, 27]
    assert b.accumulate(add).map(inc).compute() == [2, 4, 7, 11, 16, 22, 29]

    b = db.from_sequence([1, 2, 3], npartitions=1)
    assert b.accumulate(add).compute() == [1, 3, 6]


def test_groupby_tasks():
    b = db.from_sequence(range(160), npartitions=4)
    out = b.groupby(lambda x: x % 10, max_branch=4, method='tasks')
    partitions = dask.get(out.dask, out._keys())

    for a in partitions:
        for b in partitions:
            if a is not b:
                assert not set(pluck(0, a)) & set(pluck(0, b))

    b = db.from_sequence(range(1000), npartitions=100)
    out = b.groupby(lambda x: x % 123, method='tasks')
    assert len(out.dask) < 100**2
    partitions = dask.get(out.dask, out._keys())

    for a in partitions:
        for b in partitions:
            if a is not b:
                assert not set(pluck(0, a)) & set(pluck(0, b))

    b = db.from_sequence(range(10000), npartitions=345)
    out = b.groupby(lambda x: x % 2834, max_branch=24, method='tasks')
    partitions = dask.get(out.dask, out._keys())

    for a in partitions:
        for b in partitions:
            if a is not b:
                assert not set(pluck(0, a)) & set(pluck(0, b))


def test_groupby_tasks_names():
    b = db.from_sequence(range(160), npartitions=4)
    func = lambda x: x % 10
    func2 = lambda x: x % 20
    assert (set(b.groupby(func, max_branch=4, method='tasks').dask) ==
            set(b.groupby(func, max_branch=4, method='tasks').dask))
    assert (set(b.groupby(func, max_branch=4, method='tasks').dask) !=
            set(b.groupby(func, max_branch=2, method='tasks').dask))
    assert (set(b.groupby(func, max_branch=4, method='tasks').dask) !=
            set(b.groupby(func2, max_branch=4, method='tasks').dask))


@pytest.mark.parametrize('size,npartitions,groups', [(1000, 20, 100),
                                                     (12345, 234, 1042)])
def test_groupby_tasks_2(size, npartitions, groups):
    func = lambda x: x % groups
    b = db.range(size, npartitions=npartitions).groupby(func, method='tasks')
    result = b.compute(get=dask.get)
    assert dict(result) == groupby(func, range(size))


def test_groupby_tasks_3():
    func = lambda x: x % 10
    b = db.range(20, npartitions=5).groupby(func, method='tasks', max_branch=2)
    result = b.compute(get=dask.get)
    assert dict(result) == groupby(func, range(20))
    # assert b.npartitions == 5


def test_to_textfiles_empty_partitions():
    with tmpdir() as d:
        b = db.range(5, npartitions=5).filter(lambda x: x == 1).map(str)
        b.to_textfiles(os.path.join(d, '*.txt'))
        assert len(os.listdir(d)) == 5


def test_reduction_empty():
    b = db.from_sequence(range(10), npartitions=100)
    assert b.filter(lambda x: x % 2 == 0).max().compute(get=dask.get) == 8
    assert b.filter(lambda x: x % 2 == 0).min().compute(get=dask.get) == 0


class StrictReal(int):
    def __eq__(self, other):
        assert isinstance(other, StrictReal)
        return self.real == other.real

    def __ne__(self, other):
        assert isinstance(other, StrictReal)
        return self.real != other.real


def test_reduction_with_non_comparable_objects():
    b = db.from_sequence([StrictReal(x) for x in range(10)], partition_size=2)
    assert b.fold(max, max).compute(get=dask.get) == StrictReal(9)


def test_reduction_with_sparse_matrices():
    sp = pytest.importorskip('scipy.sparse')
    b = db.from_sequence([sp.csr_matrix([0]) for x in range(4)], partition_size=2)

    def sp_reduce(a, b):
        return sp.vstack([a, b])

    assert b.fold(sp_reduce, sp_reduce).compute(get=dask.get).shape == (4, 1)


def test_empty():
    list(db.from_sequence([])) == []


def test_bag_picklable():
    from pickle import loads, dumps

    b = db.from_sequence(range(100))
    b2 = loads(dumps(b))
    assert b.compute() == b2.compute()

    s = b.sum()
    s2 = loads(dumps(s))
    assert s.compute() == s2.compute()


def test_msgpack_unicode():
    b = db.from_sequence([{"a": 1}]).groupby("a")
    result = b.compute(get=dask.async.get_sync)
    assert dict(result) == {1: [{'a': 1}]}


def test_bag_with_single_callable():
    f = lambda: None
    b = db.from_sequence([f])
    assert list(b.compute(get=dask.get)) == [f]


def test_optimize_fuse_keys():
    x = db.range(10, npartitions=2)
    y = x.map(inc)
    z = y.map(inc)

    dsk = z._optimize(z.dask, z._keys())
    assert not set(y.dask) & set(dsk)

    dsk = z._optimize(z.dask, z._keys(), fuse_keys=y._keys())
    assert all(k in dsk for k in y._keys())


def test_reductions_are_lazy():
    current = [None]

    def part():
        for i in range(10):
            current[0] = i
            yield i

    def func(part):
        assert current[0] == 0
        return sum(part)

    b = Bag({('foo', 0): part()}, 'foo', 1)

    res = b.reduction(func, sum)

    assert res.compute(get=dask.get) == sum(range(10))


def test_repeated_groupby():
    b = db.range(10, npartitions=4)
    c = b.groupby(lambda x: x % 3)
    assert valmap(len, dict(c)) == valmap(len, dict(c))
