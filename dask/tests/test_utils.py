import pickle
import functools

import numpy as np
import pytest

from dask.sharedict import ShareDict
from dask.utils import (takes_multiple_arguments, Dispatch, random_state_data,
                        infer_storage_options, eq_strict, memory_repr,
                        methodcaller, M, skip_doctest, SerializableLock,
                        funcname, ndeepmap, ensure_dict, package_of)
from dask.utils_test import inc


def test_takes_multiple_arguments():
    assert takes_multiple_arguments(map)
    assert not takes_multiple_arguments(sum)

    def multi(a, b, c):
        return a, b, c

    class Singular(object):
        def __init__(self, a):
            pass

    class Multi(object):
        def __init__(self, a, b):
            pass

    assert takes_multiple_arguments(multi)
    assert not takes_multiple_arguments(Singular)
    assert takes_multiple_arguments(Multi)

    def f():
        pass

    assert not takes_multiple_arguments(f)

    def vararg(*args):
        pass

    assert takes_multiple_arguments(vararg)
    assert not takes_multiple_arguments(vararg, varargs=False)


def test_dispatch():
    foo = Dispatch()
    foo.register(int, lambda a: a + 1)
    foo.register(float, lambda a: a - 1)
    foo.register(tuple, lambda a: tuple(foo(i) for i in a))
    foo.register(object, lambda a: a)

    class Bar(object):
        pass
    b = Bar()
    assert foo(1) == 2
    assert foo(1.0) == 0.0
    assert foo(b) == b
    assert foo((1, 2.0, b)) == (2, 1.0, b)


@pytest.mark.slow
def test_random_state_data():
    seed = 37
    state = np.random.RandomState(seed)
    n = 100000

    # Use an integer
    states = random_state_data(n, seed)
    assert len(states) == n

    # Use RandomState object
    states2 = random_state_data(n, state)
    for s1, s2 in zip(states, states2):
        assert (s1 == s2).all()

    # Consistent ordering
    states = random_state_data(10, 1234)
    states2 = random_state_data(20, 1234)[:10]

    for s1, s2 in zip(states, states2):
        assert (s1 == s2).all()


def test_infer_storage_options():
    so = infer_storage_options('/mnt/datasets/test.csv')
    assert so.pop('protocol') == 'file'
    assert so.pop('path') == '/mnt/datasets/test.csv'
    assert not so

    assert infer_storage_options('./test.csv')['path'] == './test.csv'
    assert infer_storage_options('../test.csv')['path'] == '../test.csv'

    so = infer_storage_options('C:\\test.csv')
    assert so.pop('protocol') == 'file'
    assert so.pop('path') == 'C:\\test.csv'
    assert not so

    assert infer_storage_options('d:\\test.csv')['path'] == 'd:\\test.csv'
    assert infer_storage_options('\\test.csv')['path'] == '\\test.csv'
    assert infer_storage_options('.\\test.csv')['path'] == '.\\test.csv'
    assert infer_storage_options('test.csv')['path'] == 'test.csv'

    so = infer_storage_options(
        'hdfs://username:pwd@Node:123/mnt/datasets/test.csv?q=1#fragm',
        inherit_storage_options={'extra': 'value'})
    assert so.pop('protocol') == 'hdfs'
    assert so.pop('username') == 'username'
    assert so.pop('password') == 'pwd'
    assert so.pop('host') == 'Node'
    assert so.pop('port') == 123
    assert so.pop('path') == '/mnt/datasets/test.csv'
    assert so.pop('url_query') == 'q=1'
    assert so.pop('url_fragment') == 'fragm'
    assert so.pop('extra') == 'value'
    assert not so

    so = infer_storage_options('hdfs://User-name@Node-name.com/mnt/datasets/test.csv')
    assert so.pop('username') == 'User-name'
    assert so.pop('host') == 'Node-name.com'

    assert infer_storage_options('s3://Bucket-name.com/test.csv')['host'] == 'Bucket-name.com'
    assert infer_storage_options('http://127.0.0.1:8080/test.csv')['host'] == '127.0.0.1'

    with pytest.raises(KeyError):
        infer_storage_options('file:///bucket/file.csv', {'path': 'collide'})
    with pytest.raises(KeyError):
        infer_storage_options('hdfs:///bucket/file.csv', {'protocol': 'collide'})


@pytest.mark.parametrize('urlpath, expected_path', (
    (r'c:\foo\bar', r'c:\foo\bar'),
    (r'C:\\foo\bar', r'C:\\foo\bar'),
    (r'c:/foo/bar', r'c:/foo/bar'),
    (r'file:///c|\foo\bar', r'c:\foo\bar'),
    (r'file:///C|/foo/bar', r'C:/foo/bar'),
    (r'file:///C:/foo/bar', r'C:/foo/bar'),
))
def test_infer_storage_options_c(urlpath, expected_path):
    so = infer_storage_options(urlpath)
    assert so['protocol'] == 'file'
    assert so['path'] == expected_path


def test_eq_strict():
    assert eq_strict('a', 'a')
    assert not eq_strict(b'a', u'a')


def test_memory_repr():
    for power, mem_repr in enumerate(['1.0 bytes', '1.0 KB', '1.0 MB', '1.0 GB']):
        assert memory_repr(1024 ** power) == mem_repr


def test_method_caller():
    a = [1, 2, 3, 3, 3]
    f = methodcaller('count')
    assert f(a, 3) == a.count(3)
    assert methodcaller('count') is f
    assert M.count is f
    assert pickle.loads(pickle.dumps(f)) is f
    assert 'count' in dir(M)

    assert 'count' in str(methodcaller('count'))
    assert 'count' in repr(methodcaller('count'))


def test_skip_doctest():
    example = """>>> xxx
>>>
>>> # comment
>>> xxx"""

    res = skip_doctest(example)
    assert res == """>>> xxx    # doctest: +SKIP
>>>
>>> # comment
>>> xxx    # doctest: +SKIP"""

    assert skip_doctest(None) == ''


def test_SerializableLock():
    a = SerializableLock()
    b = SerializableLock()
    with a:
        pass

    with a:
        with b:
            pass

    with a:
        assert not a.acquire(False)

    a2 = pickle.loads(pickle.dumps(a))
    a3 = pickle.loads(pickle.dumps(a))
    a4 = pickle.loads(pickle.dumps(a2))

    for x in [a, a2, a3, a4]:
        for y in [a, a2, a3, a4]:
            with x:
                assert not y.acquire(False)

    b2 = pickle.loads(pickle.dumps(b))
    b3 = pickle.loads(pickle.dumps(b2))

    for x in [a, a2, a3, a4]:
        for y in [b, b2, b3]:
            with x:
                with y:
                    pass
            with y:
                with x:
                    pass


def test_SerializableLock_name_collision():
    a = SerializableLock('a')
    b = SerializableLock('b')
    c = SerializableLock('a')
    d = SerializableLock()

    assert a.lock is not b.lock
    assert a.lock is c.lock
    assert d.lock not in (a.lock, b.lock, c.lock)


def test_funcname():
    def foo(a, b, c):
        pass

    assert funcname(foo) == 'foo'
    assert funcname(functools.partial(foo, a=1)) == 'foo'
    assert funcname(M.sum) == 'sum'
    assert funcname(lambda: 1) == 'lambda'

    class Foo(object):
        pass

    assert funcname(Foo) == 'Foo'
    assert 'Foo' in funcname(Foo())


def test_funcname_toolz():
    toolz = pytest.importorskip('toolz')

    @toolz.curry
    def foo(a, b, c):
        pass

    assert funcname(foo) == 'foo'
    assert funcname(foo(1)) == 'foo'


def test_funcname_multipledispatch():
    md = pytest.importorskip('multipledispatch')

    @md.dispatch(int, int, int)
    def foo(a, b, c):
        pass

    assert funcname(foo) == 'foo'
    assert funcname(functools.partial(foo, a=1)) == 'foo'


def test_ndeepmap():
    L = 1
    assert ndeepmap(0, inc, L) == 2

    L = [1]
    assert ndeepmap(0, inc, L) == 2

    L = [1, 2, 3]
    assert ndeepmap(1, inc, L) == [2, 3, 4]

    L = [[1, 2], [3, 4]]
    assert ndeepmap(2, inc, L) == [[2, 3], [4, 5]]

    L = [[[1, 2], [3, 4, 5]], [[6], []]]
    assert ndeepmap(3, inc, L) == [[[2, 3], [4, 5, 6]], [[7], []]]


def test_ensure_dict():
    d = {'x': 1}
    assert ensure_dict(d) is d
    sd = ShareDict()
    sd.update(d)
    assert type(ensure_dict(sd)) is dict
    assert ensure_dict(sd) == d

    class mydict(dict):
        pass

    md = mydict()
    md['x'] = 1
    assert type(ensure_dict(md)) is dict
    assert ensure_dict(md) == d


def test_package_of():
    import math
    assert package_of(math.sin) is math
    try:
        import numpy
    except ImportError:
        pass
    else:
        assert package_of(numpy.memmap) is numpy
