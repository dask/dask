from distributed.protocol.pickle import dumps, loads

import pytest

from operator import add
from functools import partial


def test_pickle_data():
    data = [1, b'123', '123', [123], {}, set()]
    for d in data:
        assert loads(dumps(d)) == d


def test_pickle_numpy():
    np = pytest.importorskip('numpy')
    x = np.ones(5)
    assert (loads(dumps(x)) == x).all()

    x = np.ones(5000)
    assert (loads(dumps(x)) == x).all()


def test_pickle_functions():
    value = 1
    def f(x):  # closure
        return x + value

    for func in [f, lambda x: x + 1, partial(add, 1)]:
        assert loads(dumps(func))(1) == func(1)
