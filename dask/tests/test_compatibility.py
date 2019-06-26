import functools

from dask.compatibility import getargspec


def test_getargspec():
    def func(x, y):
        pass

    assert getargspec(func).args == ["x", "y"]

    func2 = functools.partial(func, 2)
    # this is a bit of a lie, but maybe close enough
    assert getargspec(func2).args == ["x", "y"]

    def wrapper(*args, **kwargs):
        pass

    wrapper.__wrapped__ = func
    assert getargspec(wrapper).args == ["x", "y"]

    class MyType(object):
        def __init__(self, x, y):
            pass

    assert getargspec(MyType).args == ["self", "x", "y"]
