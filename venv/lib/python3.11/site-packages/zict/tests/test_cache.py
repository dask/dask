import gc
import threading
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor

import pytest

from zict import Cache, WeakValueMapping
from zict.tests import utils_test


def test_cache_get_set_del():
    d = Cache({}, {})

    # getitem (bad key)
    with pytest.raises(KeyError):
        d[0]
    assert (d.data, d.cache) == ({}, {})

    # setitem(no update); cache is empty
    d[1] = 10
    assert (d.data, d.cache) == ({1: 10}, {1: 10})

    # getitem; cache is full
    assert d[1] == 10
    assert (d.data, d.cache) == ({1: 10}, {1: 10})

    # getitem; cache is empty
    d.cache.clear()
    assert d[1] == 10
    assert (d.data, d.cache) == ({1: 10}, {1: 10})

    # setitem(update); cache is full
    d[1] = 20
    assert (d.data, d.cache) == ({1: 20}, {1: 20})

    # setitem(update); cache is empty
    d.cache.clear()
    d[1] = 30
    assert (d.data, d.cache) == ({1: 30}, {1: 30})

    # delitem; cache is full
    del d[1]
    assert (d.data, d.cache) == ({}, {})

    # delitem; cache is empty
    d[1] = 10
    d.cache.clear()
    del d[1]
    assert (d.data, d.cache) == ({}, {})

    # delitem (bad key)
    with pytest.raises(KeyError):
        del d[0]
    assert (d.data, d.cache) == ({}, {})


def test_do_not_read_from_data():
    """__len__, __iter__, __contains__, and keys() do not populate the cache"""

    class D(UserDict):
        def __getitem__(self, key):
            raise AssertionError()

    d = Cache(D({1: 10, 2: 20}), {})
    assert len(d) == 2
    assert list(d) == [1, 2]
    assert 1 in d
    assert 3 not in d
    assert d.keys() == {1, 2}
    assert d.cache == {}


def test_no_update_on_set():
    d = Cache({}, {}, update_on_set=False)
    d[1] = 10
    assert (d.data, d.cache) == ({1: 10}, {})
    assert d[1] == 10
    assert (d.data, d.cache) == ({1: 10}, {1: 10})
    d[1] = 20
    assert (d.data, d.cache) == ({1: 20}, {})
    assert d[1] == 20
    assert (d.data, d.cache) == ({1: 20}, {1: 20})


def test_slow_fails():
    """data.__setitem__ raises; e.g. disk full"""

    class D(UserDict):
        def __setitem__(self, key, value):
            if value == "fail":
                self.pop(key, None)
                raise ValueError()
            super().__setitem__(key, value)

    d = Cache(D(), {})

    # setitem(no update); cache is empty
    with pytest.raises(ValueError):
        d[1] = "fail"
    assert (d.data.data, d.cache) == ({}, {})

    # setitem(update); cache is empty
    d[1] = 10
    d.cache.clear()
    assert (d.data.data, d.cache) == ({1: 10}, {})
    with pytest.raises(ValueError):
        d[1] = "fail"
    assert (d.data.data, d.cache) == ({}, {})

    # setitem(update); cache is full
    d[1] = 10
    assert (d.data.data, d.cache) == ({1: 10}, {1: 10})
    with pytest.raises(ValueError):
        d[1] = "fail"
    assert (d.data.data, d.cache) == ({}, {})


def test_weakvaluemapping():
    class C:
        pass

    d = WeakValueMapping()
    a = C()
    d["a"] = a
    assert d["a"] is a
    del a
    gc.collect()  # Needed in pypy
    assert "a" not in d

    # str does not support weakrefs
    b = "bbb"
    d["b"] = b
    assert "b" not in d


def test_mapping():
    """
    Test mapping interface for Cache().
    """
    buff = Cache({}, {})
    utils_test.check_mapping(buff)
    utils_test.check_closing(buff)

    buff.clear()
    assert not buff.cache
    assert not buff.data
    assert not buff._last_updated


@pytest.mark.parametrize("get_when", ("before", "after"))
@pytest.mark.parametrize("set_when", ("before", "after"))
@pytest.mark.parametrize(
    "starts_first,seed,update_on_set",
    [
        ("get", True, False),
        ("set", False, False),
        ("set", False, True),
        ("set", True, False),
        ("set", True, True),
    ],
)
@pytest.mark.parametrize("ends_first", ("get", "set"))
def test_multithread_race_condition_set_get(
    get_when, set_when, starts_first, seed, update_on_set, ends_first
):
    """Test race conditions between __setitem__ and __getitem__ on the same key"""
    in_get = threading.Event()
    block_get = threading.Event()
    in_set = threading.Event()
    block_set = threading.Event()

    class Slow(UserDict):
        def __getitem__(self, k):
            if get_when == "before":
                try:
                    v = self.data[k]
                finally:
                    in_get.set()
                assert block_get.wait(timeout=5)
                return v
            else:
                in_get.set()
                assert block_get.wait(timeout=5)
                return self.data[k]

        def __setitem__(self, k, v):
            if set_when == "before":
                self.data[k] = v
                in_set.set()
                assert block_set.wait(timeout=5)
            else:
                in_set.set()
                assert block_set.wait(timeout=5)
                self.data[k] = v

    z = Cache(Slow(), {}, update_on_set=update_on_set)
    if seed:
        block_set.set()
        z["x"] = 1
        in_set.clear()
        block_set.clear()
        assert z.data.data == {"x": 1}
        assert set(z._last_updated) == {"x"}
        if update_on_set:
            assert z.cache == {"x": 1}
        else:
            assert z.cache == {}

    with ThreadPoolExecutor(2) as ex:
        if starts_first == "get":
            get_fut = ex.submit(z.__getitem__, "x")
            assert in_get.wait(timeout=5)
            set_fut = ex.submit(z.__setitem__, "x", 2)
            assert in_set.wait(timeout=5)
        else:
            set_fut = ex.submit(z.__setitem__, "x", 2)
            assert in_set.wait(timeout=5)
            get_fut = ex.submit(z.__getitem__, "x")
            assert in_get.wait(timeout=5)

        if ends_first == "get":
            block_get.set()
            try:
                assert get_fut.result() in (1, 2)
            except KeyError:
                pass
            block_set.set()
            set_fut.result()
        else:
            block_set.set()
            set_fut.result()
            block_get.set()
            try:
                assert get_fut.result() in (1, 2)
            except KeyError:
                pass

    assert z.data.data == {"x": 2}
    # The cache is either not populated or up to date
    assert z.cache in ({}, {"x": 2})
    assert set(z._last_updated) == {"x"}


@pytest.mark.parametrize("get_when", ("before", "after"))
def test_multithread_race_condition_del_get(get_when):
    """Test race conditions between __delitem__ and __getitem__ on the same key"""
    in_get = threading.Event()
    block_get = threading.Event()

    class Slow(UserDict):
        def __getitem__(self, k):
            if get_when == "before":
                v = self.data[k]
                in_get.set()
                assert block_get.wait(timeout=5)
                return v
            else:
                in_get.set()
                assert block_get.wait(timeout=5)
                return self.data[k]

    z = Cache(Slow(), {}, update_on_set=False)
    z["x"] = 1
    assert z.data.data == {"x": 1}
    assert set(z._last_updated) == {"x"}
    assert z.cache == {}

    with ThreadPoolExecutor(1) as ex:
        get_fut = ex.submit(z.__getitem__, "x")
        assert in_get.wait(timeout=5)
        del z["x"]

        block_get.set()
        if get_when == "before":
            assert get_fut.result() == 1
        else:
            with pytest.raises(KeyError):
                get_fut.result()

    assert not z.data.data
    assert not z.cache
    assert not z._last_updated


@pytest.mark.parametrize("set_when", ("before", "after"))
@pytest.mark.parametrize(
    "seed_data,seed_cache", [(False, False), (True, False), (True, True)]
)
@pytest.mark.parametrize("update_on_set", [False, True])
def test_multithread_race_condition_del_set(
    set_when, seed_data, seed_cache, update_on_set
):
    """Test race conditions between __delitem__ and __setitem__ on the same key"""
    in_set = threading.Event()
    block_set = threading.Event()

    class Slow(UserDict):
        def __setitem__(self, k, v):
            if set_when == "before":
                self.data[k] = v
                in_set.set()
                assert block_set.wait(timeout=5)
            else:
                in_set.set()
                assert block_set.wait(timeout=5)
                self.data[k] = v

    z = Cache(Slow(), {}, update_on_set=update_on_set)
    if seed_data:
        block_set.set()
        z["x"] = 1
        in_set.clear()
        block_set.clear()
        if seed_cache and not update_on_set:
            _ = z["x"]

    with ThreadPoolExecutor(1) as ex:
        set_fut = ex.submit(z.__setitem__, "x", 2)
        assert in_set.wait(timeout=5)
        try:
            del z["x"]
        except KeyError:
            pass
        block_set.set()
        set_fut.result()

    assert z.data.data in ({"x": 2}, {})
    assert z.cache in (z.data.data, {})
    assert z._last_updated.keys() == z.data.data.keys()


@pytest.mark.parametrize("set1_when", ("before", "after"))
@pytest.mark.parametrize("set2_when", ("before", "after"))
@pytest.mark.parametrize("starts_first", (1, 2))
@pytest.mark.parametrize("ends_first", (1, 2))
@pytest.mark.parametrize(
    "seed_data,seed_cache", [(False, False), (True, False), (True, True)]
)
@pytest.mark.parametrize("update_on_set", [False, True])
def test_multithread_race_condition_set_set(
    set1_when, set2_when, starts_first, ends_first, seed_data, seed_cache, update_on_set
):
    """Test __setitem__ in race condition with itself"""
    when = {1: set1_when, 2: set2_when}
    in_set = {1: threading.Event(), 2: threading.Event()}
    block_set = {1: threading.Event(), 2: threading.Event()}

    class Slow(UserDict):
        def __setitem__(self, k, v):
            if v == 0:
                # seed
                self.data[k] = v
                return

            if when[v] == "before":
                self.data[k] = v
                in_set[v].set()
                assert block_set[v].wait(timeout=5)
            else:
                in_set[v].set()
                assert block_set[v].wait(timeout=5)
                self.data[k] = v

    z = Cache(Slow(), {}, update_on_set=update_on_set)
    if seed_data:
        z["x"] = 0
        if seed_cache and not update_on_set:
            _ = z["x"]

    with ThreadPoolExecutor(2) as ex:
        futures = {}
        futures[starts_first] = ex.submit(z.__setitem__, "x", starts_first)
        assert in_set[starts_first].wait(timeout=5)
        starts_second = 2 if starts_first == 1 else 1
        futures[starts_second] = ex.submit(z.__setitem__, "x", starts_second)
        assert in_set[starts_second].wait(timeout=5)

        block_set[ends_first].set()
        futures[ends_first].result()
        ends_second = 2 if ends_first == 1 else 1
        block_set[ends_second].set()
        futures[ends_second].result()

    assert z.data.data in ({"x": 1}, {"x": 2})
    assert z.cache in (z.data.data, {})
    assert set(z._last_updated) == {"x"}


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
@pytest.mark.parametrize("update_on_set", [False, True])
def test_stress_different_keys_threadsafe(update_on_set):
    buff = Cache(utils_test.SlowDict(0.001), {}, update_on_set=update_on_set)
    utils_test.check_different_keys_threadsafe(buff)
    assert not buff.cache
    assert not buff.data
    assert not buff._last_updated
    utils_test.check_mapping(buff)


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
@pytest.mark.parametrize("update_on_set", [False, True])
def test_stress_same_key_threadsafe(update_on_set):
    buff = Cache(utils_test.SlowDict(0.001), {}, update_on_set=update_on_set)
    utils_test.check_same_key_threadsafe(buff)
    assert not buff.cache
    assert not buff.data
    assert not buff._last_updated
    utils_test.check_mapping(buff)
