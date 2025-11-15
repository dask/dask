import random
import threading
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor

import pytest

from zict import Sieve
from zict.tests import utils_test


def test_simple():
    a = {}
    b = {}
    c = {}

    def selector(k, v):
        return len(v) % 3

    mappings = {0: a, 1: b, 2: c}

    d = Sieve(mappings, selector)
    assert len(d) == 0

    d["u"] = b"the"
    d["v"] = b"big"
    d["w"] = b"brown"
    d["x"] = b"fox"
    d["y"] = b"jumps"
    d["z"] = b"over"

    assert d["u"] == b"the"
    assert d["v"] == b"big"
    assert len(d) == 6

    assert sorted(d) == ["u", "v", "w", "x", "y", "z"]
    assert sorted(d.keys()) == ["u", "v", "w", "x", "y", "z"]
    assert sorted(d.values()) == sorted(
        [b"the", b"big", b"brown", b"fox", b"jumps", b"over"]
    )

    assert a == {"u": b"the", "v": b"big", "x": b"fox"}
    assert b == {"z": b"over"}
    assert c == {"w": b"brown", "y": b"jumps"}

    # Changing existing keys can move values from one mapping to another.
    d["w"] = b"lazy"
    d["x"] = b"dog"
    assert d["w"] == b"lazy"
    assert d["x"] == b"dog"
    assert len(d) == 6
    assert sorted(d.values()) == sorted(
        [b"the", b"big", b"lazy", b"dog", b"jumps", b"over"]
    )

    assert a == {"u": b"the", "v": b"big", "x": b"dog"}
    assert b == {"w": b"lazy", "z": b"over"}
    assert c == {"y": b"jumps"}

    del d["v"]
    del d["w"]
    assert len(d) == 4
    assert "v" not in d
    assert "w" not in d
    assert sorted(d.values()) == sorted([b"the", b"dog", b"jumps", b"over"])


def test_mapping():
    """
    Test mapping interface for Sieve().
    """
    a = {}
    b = {}

    def selector(key, value):
        return sum(bytearray(value)) & 1

    mappings = {0: a, 1: b}
    z = Sieve(mappings, selector)
    utils_test.check_mapping(z)
    utils_test.check_closing(z)

    z.clear()
    assert z.mappings == {0: {}, 1: {}}
    assert not z.key_to_mapping


@pytest.mark.parametrize("method", ("__setitem__", "update"))
@pytest.mark.parametrize("set_when", ("before", "after"))
@pytest.mark.parametrize("seed", [False, "same", "different"])
def test_multithread_race_condition_del_set(method, set_when, seed):
    """Test race conditions between __delitem__ and __setitem__/update on the same key"""
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

    z = Sieve({0: {}, 1: Slow()}, selector=lambda k, v: v % 2)
    if seed == "same":
        block_set.set()
        z["x"] = 1  # mapping 1
        in_set.clear()
        block_set.clear()
    elif seed == "different":
        z["x"] = 0  # mapping 0

    with ThreadPoolExecutor(1) as ex:
        if method == "__setitem__":
            set_fut = ex.submit(z.__setitem__, "x", 3)  # mapping 1
        else:
            assert method == "update"
            set_fut = ex.submit(z.update, {"x": 3})  # mapping 1
        assert in_set.wait(timeout=5)
        try:
            del z["x"]
        except KeyError:
            pass
        block_set.set()
        set_fut.result()

    assert not z.mappings[0]
    assert not z.mappings[1]
    assert not z.key_to_mapping


@pytest.mark.parametrize("set1_when", ("before", "after"))
@pytest.mark.parametrize("set2_when", ("before", "after"))
@pytest.mark.parametrize("set1_method", ("__setitem__", "update"))
@pytest.mark.parametrize("set2_method", ("__setitem__", "update"))
@pytest.mark.parametrize("starts_first", (1, 2))
@pytest.mark.parametrize("ends_first", (1, 2))
@pytest.mark.parametrize("seed", [False, 0, 1, 2])
def test_multithread_race_condition_set_set(
    set1_when, set2_when, set1_method, set2_method, starts_first, ends_first, seed
):
    """Test __setitem__/update in race condition with __setitem__/update"""
    when = {1: set1_when, 2: set2_when}
    method = {1: set1_method, 2: set2_method}
    in_set = {1: threading.Event(), 2: threading.Event()}
    block_set = {1: threading.Event(), 2: threading.Event()}

    class Slow(UserDict):
        def __setitem__(self, k, v):
            if v < 3:
                # seed
                self.data[k] = v
                return
            mkey = v % 3

            if when[mkey] == "before":
                self.data[k] = v
                in_set[mkey].set()
                assert block_set[mkey].wait(timeout=5)
            else:
                in_set[mkey].set()
                assert block_set[mkey].wait(timeout=5)
                self.data[k] = v

    z = Sieve({0: {}, 1: Slow(), 2: Slow()}, selector=lambda k, v: v % 3)
    if seed is not False:
        z["x"] = seed

    with ThreadPoolExecutor(2) as ex:
        futures = {}
        starts_second = 2 if starts_first == 1 else 1
        for idx in (starts_first, starts_second):
            if method[idx] == "__setitem__":
                futures[idx] = ex.submit(z.__setitem__, "x", idx + 3)
            else:
                assert method[idx] == "update"
                futures[idx] = ex.submit(z.update, {"x": idx + 3})
            assert in_set[idx].wait(timeout=5)

        block_set[ends_first].set()
        futures[ends_first].result()
        ends_second = 2 if ends_first == 1 else 1
        block_set[ends_second].set()
        futures[ends_second].result()

    assert dict(z) in ({"x": 4}, {"x": 5})
    assert z.mappings[0] == {}
    if z["x"] == 4:
        assert z.mappings[1] == {"x": 4}
        assert z.mappings[2] == {}
        assert z.key_to_mapping == {"x": z.mappings[1]}
    else:
        assert z.mappings[1] == {}
        assert z.mappings[2] == {"x": 5}
        assert z.key_to_mapping == {"x": z.mappings[2]}


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
def test_stress_different_keys_threadsafe():
    a = {}
    b = {}
    z = Sieve({0: a, 1: b}, lambda k, v: random.choice([0, 1]))
    utils_test.check_different_keys_threadsafe(z)
    assert not a
    assert not b
    assert not z.key_to_mapping
    utils_test.check_mapping(z)


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
def test_stress_same_key_threadsafe():
    a = utils_test.SlowDict(0.001)
    b = utils_test.SlowDict(0.001)
    z = Sieve({0: a, 1: b}, lambda k, v: random.choice([0, 1]))
    utils_test.check_same_key_threadsafe(z)
    assert not a
    assert not b
    assert not z.key_to_mapping
    utils_test.check_mapping(z)
