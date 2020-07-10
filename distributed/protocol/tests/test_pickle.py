from functools import partial
import gc
from operator import add
import weakref
import sys

import pytest

from distributed.protocol import deserialize, serialize
from distributed.protocol.pickle import HIGHEST_PROTOCOL, dumps, loads

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle
    except ImportError:
        import pickle
else:
    import pickle


def test_pickle_data():
    data = [1, b"123", "123", [123], {}, set()]
    for d in data:
        assert loads(dumps(d)) == d
        assert deserialize(*serialize(d, serializers=("pickle",))) == d


def test_pickle_out_of_band():
    class MemoryviewHolder:
        def __init__(self, mv):
            self.mv = memoryview(mv)

        def __reduce_ex__(self, protocol):
            if protocol >= 5:
                return MemoryviewHolder, (pickle.PickleBuffer(self.mv),)
            else:
                return MemoryviewHolder, (self.mv.tobytes(),)

    mv = memoryview(b"123")
    mvh = MemoryviewHolder(mv)

    if HIGHEST_PROTOCOL >= 5:
        l = []
        d = dumps(mvh, buffer_callback=l.append)
        mvh2 = loads(d, buffers=l)

        assert len(l) == 1
        assert isinstance(l[0], pickle.PickleBuffer)
        assert memoryview(l[0]) == mv
    else:
        mvh2 = loads(dumps(mvh))

    assert isinstance(mvh2, MemoryviewHolder)
    assert isinstance(mvh2.mv, memoryview)
    assert mvh2.mv == mv

    h, f = serialize(mvh, serializers=("pickle",))
    mvh3 = deserialize(h, f)

    assert isinstance(mvh3, MemoryviewHolder)
    assert isinstance(mvh3.mv, memoryview)
    assert mvh3.mv == mv

    if HIGHEST_PROTOCOL >= 5:
        assert len(f) == 2
        assert isinstance(f[0], bytes)
        assert isinstance(f[1], memoryview)
        assert f[1] == mv
    else:
        assert len(f) == 1
        assert isinstance(f[0], bytes)


def test_pickle_numpy():
    np = pytest.importorskip("numpy")
    x = np.ones(5)
    assert (loads(dumps(x)) == x).all()
    assert (deserialize(*serialize(x, serializers=("pickle",))) == x).all()

    x = np.ones(5000)
    assert (loads(dumps(x)) == x).all()
    assert (deserialize(*serialize(x, serializers=("pickle",))) == x).all()

    x = np.array([np.arange(3), np.arange(4, 6)], dtype=object)
    x2 = loads(dumps(x))
    assert x.shape == x2.shape
    assert x.dtype == x2.dtype
    assert x.strides == x2.strides
    for e_x, e_x2 in zip(x.flat, x2.flat):
        np.testing.assert_equal(e_x, e_x2)
    h, f = serialize(x, serializers=("pickle",))
    if HIGHEST_PROTOCOL >= 5:
        assert len(f) == 3
    else:
        assert len(f) == 1
    x3 = deserialize(h, f)
    assert x.shape == x3.shape
    assert x.dtype == x3.dtype
    assert x.strides == x3.strides
    for e_x, e_x3 in zip(x.flat, x3.flat):
        np.testing.assert_equal(e_x, e_x3)

    if HIGHEST_PROTOCOL >= 5:
        x = np.ones(5000)

        l = []
        d = dumps(x, buffer_callback=l.append)
        assert len(l) == 1
        assert isinstance(l[0], pickle.PickleBuffer)
        assert memoryview(l[0]) == memoryview(x)
        assert (loads(d, buffers=l) == x).all()

        h, f = serialize(x, serializers=("pickle",))
        assert len(f) == 2
        assert isinstance(f[0], bytes)
        assert isinstance(f[1], memoryview)
        assert (deserialize(h, f) == x).all()


@pytest.mark.xfail(
    sys.version_info[:2] == (3, 8),
    reason="Sporadic failure on Python 3.8",
    strict=False,
)
def test_pickle_functions():
    def make_closure():
        value = 1

        def f(x):  # closure
            return x + value

        return f

    def funcs():
        yield make_closure()
        yield (lambda x: x + 1)
        yield partial(add, 1)

    for func in funcs():
        wr = weakref.ref(func)

        func2 = loads(dumps(func))
        wr2 = weakref.ref(func2)
        assert func2(1) == func(1)

        func3 = deserialize(*serialize(func, serializers=("pickle",)))
        wr3 = weakref.ref(func3)
        assert func3(1) == func(1)

        del func, func2, func3
        gc.collect()
        assert wr() is None
        assert wr2() is None
        assert wr3() is None
