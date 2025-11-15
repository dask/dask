from __future__ import annotations

import random
import string
import threading
import time
from collections import UserDict
from collections.abc import ItemsView, KeysView, MutableMapping, ValuesView
from concurrent.futures import ThreadPoolExecutor

import pytest

from zict.common import ZictBase

# How many times to repeat non-deterministic stress tests.
# You may set it as high as 50 if you wish to run in CI.
REPEAT_STRESS_TESTS = 1


def generate_random_strings(n, min_len, max_len):
    r = random.Random(42)
    out = []
    chars = string.ascii_lowercase + string.digits

    for _ in range(n):
        nchars = r.randint(min_len, max_len)
        s = "".join(r.choice(chars) for _ in range(nchars))
        out.append(s)

    return out


def to_bytestring(s):
    if isinstance(s, bytes):
        return s
    else:
        return s.encode("latin1")


def check_items(z: MutableMapping, expected_items: list[tuple[str, bytes]]) -> None:
    items = list(z.items())
    assert len(items) == len(expected_items)
    assert sorted(items) == sorted(expected_items)
    # All iterators should walk the mapping in the same order
    assert list(z.keys()) == [k for k, v in items]
    assert list(z.values()) == [v for k, v in items]
    assert list(z) == [k for k, v in items]

    # ItemsView, KeysView, ValuesView.__contains__()
    assert isinstance(z.keys(), KeysView)
    assert isinstance(z.values(), ValuesView)
    assert isinstance(z.items(), ItemsView)
    assert items[0] in z.items()
    assert items[0][0] in z.keys()
    assert items[0][0] in z
    assert items[0][1] in z.values()
    assert (object(), object()) not in z.items()
    assert object() not in z.keys()
    assert object() not in z
    assert object() not in z.values()


def stress_test_mapping_updates(z: MutableMapping) -> None:
    # Certain mappings shuffle between several underlying stores
    # during updates.  This stress tests the internal mapping
    # consistency.
    r = random.Random(42)

    keys = list(string.ascii_lowercase)
    values = [to_bytestring(s) for s in generate_random_strings(len(keys), 1, 10)]

    z.clear()
    assert len(z) == 0

    for k, v in zip(keys, values):
        z[k] = v
    assert len(z) == len(keys)
    assert sorted(z) == sorted(keys)
    assert sorted(z.items()) == sorted(zip(keys, values))

    for _ in range(3):
        r.shuffle(keys)
        r.shuffle(values)
        for k, v in zip(keys, values):
            z[k] = v
        check_items(z, list(zip(keys, values)))

        r.shuffle(keys)
        r.shuffle(values)
        z.update(zip(keys, values))
        check_items(z, list(zip(keys, values)))


def check_empty_mapping(z: MutableMapping) -> None:
    assert not z
    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []
    assert len(z) == 0
    assert "x" not in z
    assert "x" not in z.keys()
    assert ("x", b"123") not in z.items()
    assert b"123" not in z.values()


def check_mapping(z: MutableMapping) -> None:
    """See also test_zip.check_mapping"""
    assert type(z).__name__ in str(z)
    assert type(z).__name__ in repr(z)
    assert isinstance(z, MutableMapping)
    check_empty_mapping(z)

    z["abc"] = b"456"
    z["xyz"] = b"12"
    assert len(z) == 2
    assert z["abc"] == b"456"

    check_items(z, [("abc", b"456"), ("xyz", b"12")])

    assert "abc" in z
    assert "xyz" in z
    assert "def" not in z
    assert object() not in z

    with pytest.raises(KeyError):
        z["def"]

    z.update(xyz=b"707", uvw=b"000")
    check_items(z, [("abc", b"456"), ("xyz", b"707"), ("uvw", b"000")])
    z.update([("xyz", b"654"), ("uvw", b"999")])
    check_items(z, [("abc", b"456"), ("xyz", b"654"), ("uvw", b"999")])
    z.update({"xyz": b"321"})
    check_items(z, [("abc", b"456"), ("xyz", b"321"), ("uvw", b"999")])
    # Update with iterator (can read only once)
    z.update(iter([("foo", b"132"), ("bar", b"887")]))
    check_items(
        z,
        [
            ("abc", b"456"),
            ("xyz", b"321"),
            ("uvw", b"999"),
            ("foo", b"132"),
            ("bar", b"887"),
        ],
    )

    del z["abc"]
    with pytest.raises(KeyError):
        z["abc"]
    with pytest.raises(KeyError):
        del z["abc"]
    assert "abc" not in z
    assert set(z) == {"uvw", "xyz", "foo", "bar"}
    assert len(z) == 4

    z["def"] = b"\x00\xff"
    assert len(z) == 5
    assert z["def"] == b"\x00\xff"
    assert "def" in z

    stress_test_mapping_updates(z)


def check_different_keys_threadsafe(
    z: MutableMapping, allow_keyerror: bool = False
) -> None:
    barrier = threading.Barrier(2)
    counters = [0, 0]

    def worker(idx, key, value):
        barrier.wait()
        while any(c < 10 for c in counters):
            z[key] = value
            try:
                assert z[key] == value
                del z[key]
            except KeyError:
                if allow_keyerror:
                    continue  # Try again, don't inc i
                raise

            assert key not in z
            with pytest.raises(KeyError):
                _ = z[key]
            with pytest.raises(KeyError):
                del z[key]
            assert len(z) in (0, 1)
            counters[idx] += 1

    with ThreadPoolExecutor(2) as ex:
        f1 = ex.submit(worker, 0, "x", b"123")
        f2 = ex.submit(worker, 1, "y", b"456")
        f1.result()
        f2.result()

    assert not z


def check_same_key_threadsafe(z: MutableMapping) -> None:
    barrier = threading.Barrier(4)
    counters = [0, 0, 0, 0]

    def w_set():
        barrier.wait()
        while any(c < 10 for c in counters):
            z["x"] = b"123"
            counters[0] += 1

    def w_update():
        barrier.wait()
        while any(c < 10 for c in counters):
            z.update(x=b"456")
            counters[1] += 1

    def w_del():
        barrier.wait()
        while any(c < 10 for c in counters):
            try:
                del z["x"]
                counters[2] += 1
            except KeyError:
                pass

    def w_get():
        barrier.wait()
        while any(c < 10 for c in counters):
            try:
                assert z["x"] in (b"123", b"456")
                counters[3] += 1
            except KeyError:
                pass

    with ThreadPoolExecutor(4) as ex:
        futures = [
            ex.submit(w_set),
            ex.submit(w_update),
            ex.submit(w_del),
            ex.submit(w_get),
        ]
        for f in futures:
            f.result()

    z.pop("x", None)


def check_closing(z: ZictBase) -> None:
    z.close()


def check_bad_key_types(z: MutableMapping, has_del: bool = True) -> None:
    """z does not accept any Hashable as keys.
    Test that it reacts correctly when confronted with an invalid key type.
    """
    bad = object()

    assert bad not in z
    assert bad not in z.keys()
    assert (bad, b"123") not in z.items()

    with pytest.raises(TypeError):
        z[bad] = b"123"
    with pytest.raises(TypeError):
        z.update({bad: b"123"})
    with pytest.raises(KeyError):
        z[bad]
    if has_del:
        with pytest.raises(KeyError):
            del z[bad]


def check_bad_value_types(z: MutableMapping) -> None:
    """z does not accept any Python object as values.
    Test that it reacts correctly when confronted with an invalid value type.
    """
    bad = object()

    assert bad not in z.values()
    assert ("x", bad) not in z.items()

    with pytest.raises(TypeError):
        z["x"] = bad
    with pytest.raises(TypeError):
        z.update({"x": bad})


class SimpleDict(ZictBase, UserDict):
    def __init__(self):
        ZictBase.__init__(self)
        UserDict.__init__(self)


class SlowDict(UserDict):
    def __init__(self, delay):
        self.delay = delay
        super().__init__(self)

    def __getitem__(self, key):
        time.sleep(self.delay)
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        time.sleep(self.delay)
        super().__setitem__(key, value)
