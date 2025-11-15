import pickle

import pytest

from zict.common import locked
from zict.tests.utils_test import SimpleDict


def test_close_on_del():
    closed = False

    class D(SimpleDict):
        def close(self):
            nonlocal closed
            closed = True

    d = D()
    del d
    assert closed


def test_context():
    closed = False

    class D(SimpleDict):
        def close(self):
            nonlocal closed
            closed = True

    d = D()
    with d as d2:
        assert d2 is d
    assert closed


def test_update():
    items = []

    class D(SimpleDict):
        def _do_update(self, items_):
            nonlocal items
            items = items_

    d = D()
    d.update({"x": 1})
    assert list(items) == [("x", 1)]
    d.update(iter([("x", 2)]))
    assert list(items) == [("x", 2)]
    d.update({"x": 3}, y=4)
    assert list(items) == [("x", 3), ("y", 4)]
    d.update(x=5)
    assert list(items) == [("x", 5)]

    # Special kwargs can't overwrite positional-only parameters
    d.update(self=1, other=2)
    assert list(items) == [("self", 1), ("other", 2)]


def test_discard():
    class D(SimpleDict):
        def __getitem__(self, key):
            raise AssertionError()

    d = D()
    d["x"] = 1
    d["z"] = 2
    d.discard("x")
    d.discard("y")
    assert d.data == {"z": 2}


def test_pickle():
    d = SimpleDict()
    d["x"] = 1
    d2 = pickle.loads(pickle.dumps(d))
    assert d2.data == {"x": 1}


def test_lock(is_locked):
    class CustomError(Exception):
        pass

    class D(SimpleDict):
        @locked
        def f(self, crash):
            assert is_locked(self)
            with self.unlock():
                assert not is_locked(self)
            assert is_locked(self)

            # context manager re-acquires the lock on failure
            with pytest.raises(CustomError):
                with self.unlock():
                    raise CustomError()
            assert is_locked(self)

            if crash:
                raise CustomError()

    d = D()
    assert not is_locked(d)
    d.f(crash=False)
    assert not is_locked(d)

    # decorator releases the lock on failure
    with pytest.raises(CustomError):
        d.f(crash=True)
    assert not is_locked(d)
