from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('dill')

from toolz import (merge, join, pipe, filter, identity, merge_with, take,
        partial, valmap)
import math
from dask.bag.core import (Bag, lazify, lazify_task, fuse, map, collect,
        reduceby, bz2_stream, stream_decompress, reify, partition,
        _parse_s3_URI, inline_singleton_lists, optimize)
from dask.utils import filetexts, tmpfile, raises
from dask.async import get_sync
import dask
import dask.bag as db
import shutil
import os
import gzip
import bz2
import partd
from tempfile import mkdtemp

from collections import Iterator

dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)

def inc(x):
    return x + 1

def iseven(x):
    return x % 2 == 0

def isodd(x):
    return x % 2 == 1

def add(x, y):
    return x + y


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

    b2 = b.map(lambda n: [(n, n+1), (2*(n-1), -n)])
    assert b2.map(dict).compute() == [{0: 1, -2: 0}, {1: 2, 0: -1}, {2: -2}]
    assert b.map(lambda n: (n, n+1)).map(pow).compute() == [0, 1, 8]
    assert b.map(bool).compute() == [False, True, True]
    assert db.from_sequence([(1, 'real'), ('1', 'real')]).map(hasattr).compute() == \
        [True, False]


def test_filter():
    c = b.filter(iseven)
    expected = merge(dsk, dict(((c.name, i),
                                (reify, (filter, iseven, (b.name, i))))
                               for i in range(b.npartitions)))
    assert c.dask == expected


def test_remove():
    assert list(b.remove(lambda x: x % 2 == 0)) == [1, 3] * 3


def test_iter():
    assert sorted(list(b)) == sorted(L)
    assert sorted(list(b.map(inc))) == sorted(list(range(1, 6)) * 3)


def test_pluck():
    d = {('x', 0): [(1, 10), (2, 20)],
         ('x', 1): [(3, 30), (4, 40)]}
    b = Bag(d, 'x', 2)
    assert set(b.pluck(0)) == set([1, 2, 3, 4])
    assert set(b.pluck(1)) == set([10, 20, 30, 40])
    assert set(b.pluck([1, 0])) == set([(10, 1), (20, 2), (30, 3), (40, 4)])


def test_pluck_with_default():
    b = db.from_sequence(['Hello', '', 'World'])
    assert raises(IndexError, lambda: list(b.pluck(0)))
    assert list(b.pluck(0, None)) == ['H', None, 'W']


def test_fold():
    assert b.fold(add).compute() == sum(L)
    assert b.fold(add, initial=10).compute() == sum(L) + 10 * b.npartitions

    c = db.from_sequence(range(5), npartitions=3)
    def binop(acc, x):
        acc = acc.copy()
        acc.add(x)
        return acc

    assert c.fold(binop, set.union, initial=set()).compute() == set(c)

    d = db.from_sequence('hello')
    assert set(d.fold(lambda a, b: ''.join([a, b]), initial='').compute()) == set('hello')

    e = db.from_sequence([[1], [2], [3]], npartitions=2)
    with dask.set_options(get=get_sync):
        assert set(e.fold(add, initial=[]).compute()) == set([1, 2, 3])


def test_distinct():
    assert sorted(b.distinct()) == [0, 1, 2, 3, 4]


def test_frequencies():
    assert dict(list(b.frequencies())) == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}


def test_topk():
    assert list(b.topk(4)) == [4, 4, 4, 3]
    assert list(b.topk(4, key=lambda x: -x).compute(get=dask.get)) == \
            [0, 0, 0, 1]


def test_topk_with_non_callable_key():
    b = db.from_sequence([(1, 10), (2, 9), (3, 8)], npartitions=2)
    assert list(b.topk(2, key=1)) == [(1, 10), (2, 9)]
    assert list(b.topk(2, key=0)) == [(3, 8), (2, 9)]


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
    assert int(b.any()) == True
    assert int(b.all()) == False  # some zeros exist

def test_mean():
    assert b.mean().compute(get=dask.get) == 2.0
    assert float(b.mean()) == 2.0

def test_std():
    assert b.std().compute(get=dask.get) == math.sqrt(2.0)
    assert float(b.std()) == math.sqrt(2.0)

def test_var():
    assert b.var().compute(get=dask.get) == 2.0
    assert float(b.var()) == 2.0


def test_join():
    assert list(b.join([1, 2, 3], on_self=isodd, on_other=iseven)) == \
            list(join(iseven, [1, 2, 3], isodd, list(b)))
    assert list(b.join([1, 2, 3], isodd)) == \
            list(join(isodd, [1, 2, 3], isodd, list(b)))

def test_foldby():
    c = b.foldby(iseven, add, 0, add, 0)
    assert (reduceby, iseven, add, (b.name, 0), 0) in list(c.dask.values())
    assert set(c) == set(reduceby(iseven, lambda acc, x: acc + x, L, 0).items())

    c = b.foldby(iseven, lambda acc, x: acc + x)
    assert set(c) == set(reduceby(iseven, lambda acc, x: acc + x, L, 0).items())


def test_map_partitions():
    assert list(b.map_partitions(len)) == [5, 5, 5]


def test_lazify_task():
    task = (sum, (reify, (map, inc, [1, 2, 3])))
    assert lazify_task(task) == (sum, (map, inc, [1, 2, 3]))

    task = (reify, (map, inc, [1, 2, 3]))
    assert lazify_task(task) == task

    a = (reify, (map, inc,
                      (reify, (filter, iseven, 'y'))))
    b = (reify, (map, inc,
                              (filter, iseven, 'y')))
    assert lazify_task(a) == b


f = lambda x: x


def test_lazify():
    a = {'x': (reify, (map, inc,
                            (reify, (filter, iseven, 'y')))),
         'a': (f, 'x'), 'b': (f, 'x')}
    b = {'x': (reify, (map, inc,
                                    (filter, iseven, 'y'))),
         'a': (f, 'x'), 'b': (f, 'x')}
    assert lazify(a) == b


def test_inline_singleton_lists():
    inp = {'b': (list, 'a'),
           'c': (f, 'b', 1)}
    out = {'c': (f, (list, 'a'), 1)}
    assert inline_singleton_lists(inp) == out

    out = {'c': (f,        'a' , 1)}
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


def test_map_is_lazy():
    from dask.bag.core import map
    assert isinstance(map(lambda x: x, [1, 2, 3]), Iterator)

def test_can_use_dict_to_make_concrete():
    assert isinstance(dict(b.frequencies()), dict)


@pytest.mark.slow
def test_from_url():
    a = db.from_url(['http://google.com', 'http://github.com'])
    assert a.npartitions == 2

    b = db.from_url('http://raw.githubusercontent.com/ContinuumIO/dask/master/README.rst')
    assert b.npartitions == 1
    assert b'Dask\n' in b.take(10)


def test_from_filenames():
    with filetexts({'a1.log': 'A\nB', 'a2.log': 'C\nD'}) as fns:
        assert set(line.strip() for line in db.from_filenames(fns)) == \
                set('ABCD')
        assert set(line.strip() for line in db.from_filenames('a*.log')) == \
                set('ABCD')

    assert raises(ValueError, lambda: db.from_filenames('non-existent-*-path'))


def test_from_filenames_gzip():
    b = db.from_filenames(['foo.json.gz', 'bar.json.gz'])

    assert (set(b.dask.values()) ==
            set([(list, (gzip.open, os.path.abspath('foo.json.gz'))),
                 (list, (gzip.open, os.path.abspath('bar.json.gz')))]))


def test_from_filenames_bz2():
    b = db.from_filenames(['foo.json.bz2', 'bar.json.bz2'])

    assert (set(b.dask.values()) ==
            set([(list, (bz2.BZ2File, os.path.abspath('foo.json.bz2'))),
                 (list, (bz2.BZ2File, os.path.abspath('bar.json.bz2')))]))


def test_from_filenames_large():
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(('Hello, world!' + os.linesep).encode() * 100)
        b = db.from_filenames(fn, chunkbytes=100)
        c = db.from_filenames(fn)
        assert len(b.dask) > 5
        assert list(map(str, b)) == list(map(str, c))

        d = db.from_filenames([fn], chunkbytes=100)
        assert list(b) == list(d)


def test_from_filenames_large_gzip():
    with tmpfile('gz') as fn:
        f = gzip.open(fn, 'wb')
        f.write(b'Hello, world!\n' * 100)
        f.close()

        b = db.from_filenames(fn, chunkbytes=100)
        c = db.from_filenames(fn)
        assert len(b.dask) > 5
        assert list(b) == [s.decode() for s in c]


@pytest.mark.slow
def test_from_s3():
    # note we don't test connection modes with aws_access_key and
    # aws_secret_key because these are not on travis-ci
    boto = pytest.importorskip('boto')

    five_tips = (u'total_bill,tip,sex,smoker,day,time,size\n',
                 u'16.99,1.01,Female,No,Sun,Dinner,2\n',
                 u'10.34,1.66,Male,No,Sun,Dinner,3\n',
                 u'21.01,3.5,Male,No,Sun,Dinner,3\n',
                 u'23.68,3.31,Male,No,Sun,Dinner,2\n')

    # test compressed data
    e = db.from_s3('tip-data', 't*.gz')
    assert e.take(5) == five_tips

    # test wit specific key
    b = db.from_s3('tip-data', 't?ps.csv')
    assert b.npartitions == 1

    # test all keys in bucket
    c = db.from_s3('tip-data')
    assert c.npartitions == 4

    d = db.from_s3('s3://tip-data')
    assert d.npartitions == 4

    e = db.from_s3('tip-data', 'tips.bz2')
    assert e.take(5) == five_tips


def test__parse_s3_URI():
    bn, p = _parse_s3_URI('s3://mybucket/mykeys', '*')
    assert (bn == 'mybucket') and (p == 'mykeys')

    bn, p = _parse_s3_URI('s3://snow/g?obes', '*')
    assert (bn == 'snow') and (p == 'g?obes')

    bn, p = _parse_s3_URI('s3://tupper/wea*', '*')
    assert (bn == 'tupper') and (p == 'wea*')

    bn, p = _parse_s3_URI('s3://sand/', 'cast?es')
    assert (bn == 'sand') and (p == 'cast?es')


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



def test_partition_collect():
    with partd.Pickle() as p:
        partition(identity, range(6), 3, p)
        assert set(p.get(0)) == set([0, 3])
        assert set(p.get(1)) == set([1, 4])
        assert set(p.get(2)) == set([2, 5])

        assert sorted(collect(identity, 0, p, '')) == \
                [(0, [0]), (3, [3])]


def test_groupby():
    c = b.groupby(lambda x: x)
    result = dict(c)
    assert result == {0: [0, 0 ,0],
                      1: [1, 1, 1],
                      2: [2, 2, 2],
                      3: [3, 3, 3],
                      4: [4, 4, 4]}
    assert b.groupby(lambda x: x).npartitions == b.npartitions


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
    try:
        import dask.dataframe
        import pandas as pd
    except ImportError:
        return
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

def test_to_textfiles():
    b = db.from_sequence(['abc', '123', 'xyz'], npartitions=2)
    dir = mkdtemp()
    for ext, myopen in [('gz', gzip.open), ('bz2', bz2.BZ2File), ('', open)]:
        c = b.to_textfiles(os.path.join(dir, '*.' + ext))
        assert c.npartitions == b.npartitions
        try:
            c.compute(get=dask.get)
            assert os.path.exists(os.path.join(dir, '1.' + ext))

            f = myopen(os.path.join(dir, '1.' + ext), 'r')
            text = f.read()
            if hasattr(text, 'decode'):
                text = text.decode()
            assert 'xyz' in text
            f.close()
        finally:
            if os.path.exists(dir):
                shutil.rmtree(dir)


def test_to_textfiles_inputs():
    B = db.from_sequence(['abc', '123', 'xyz'], npartitions=2)
    with tmpfile() as a:
        with tmpfile() as b:
            B.to_textfiles([a, b]).compute()
            assert os.path.exists(a)
            assert os.path.exists(b)

    with tmpfile() as dirname:
        B.to_textfiles(dirname).compute()
        assert os.path.exists(dirname)
        assert os.path.exists(os.path.join(dirname, '0.part'))
    assert raises(ValueError, lambda: B.to_textfiles(5))


def test_bz2_stream():
    text = '\n'.join(map(str, range(10000)))
    compressed = bz2.compress(text.encode())
    assert (list(take(100, bz2_stream(compressed))) ==
            list(map(lambda x: str(x) + '\n', range(100))))


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

    assert raises(AttributeError, lambda: b.str.sfohsofhf)


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


def test_stream_decompress():
    data = 'abc\ndef\n123'.encode()
    assert [s.strip() for s in stream_decompress('', data)] == \
            ['abc', 'def', '123']
    assert [s.strip() for s in stream_decompress('bz2', bz2.compress(data))] == \
            ['abc', 'def', '123']
    with tmpfile() as fn:
        f = gzip.open(fn, 'wb')
        f.write(data)
        f.close()
        with open(fn, 'rb') as f:
            compressed = f.read()
    assert [s.strip() for s in stream_decompress('gz', compressed)] == \
            [b'abc', b'def', b'123']


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

