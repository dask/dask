from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from zict import InsertionSortedSet
from zict.tests import utils_test


def test_insertion_sorted_set():
    s = InsertionSortedSet()

    assert not s
    assert len(s) == 0
    assert list(s) == []
    assert s == set()
    assert s != []
    assert s == InsertionSortedSet()
    assert 1 not in s
    s.discard(1)
    with pytest.raises(KeyError):
        s.remove(1)
    with pytest.raises(KeyError):
        s.pop()
    with pytest.raises(KeyError):
        s.popleft()
    with pytest.raises(KeyError):
        s.popright()

    s.add(1)
    assert 1 in s
    assert 2 not in s
    assert len(s) == 1
    assert list(s) == [1]
    assert s == {1}
    assert s != [1]
    assert s & {1, 2} == {1}
    assert s | {1, 2} == {1, 2}
    assert s - {1, 2} == set()

    # Add already-existing element
    s.add(1)
    assert len(s) == 1
    assert list(s) == [1]

    s.remove(1)
    assert not s
    s.add(1)
    assert list(s) == [1]
    s.discard(1)
    assert not s
    s.add(1)
    assert s.pop() == 1
    s.add(1)
    s.clear()
    assert not s

    # Initialise from iterable
    s = InsertionSortedSet(iter([3, 1, 2, 5, 4, 6, 0]))
    assert list(s) == [3, 1, 2, 5, 4, 6, 0]

    # Adding already-existing element does not change order
    s.add(2)
    assert list(s) == [3, 1, 2, 5, 4, 6, 0]

    # Removing element does not change order
    s.remove(2)
    assert list(s) == [3, 1, 5, 4, 6, 0]

    s.add(2)  # Re-added elements are added to the end
    s.add(7)
    assert list(s) == [3, 1, 5, 4, 6, 0, 2, 7]

    assert [s.popleft() for _ in range(len(s))] == [3, 1, 5, 4, 6, 0, 2, 7]

    s |= [3, 1, 5, 4, 6, 0, 2, 7]
    assert [s.popright() for _ in range(len(s))] == [7, 2, 0, 6, 4, 5, 1, 3]

    # pop() is an alias to popright()
    s |= [3, 1, 5, 4, 6, 0, 2, 7]
    assert [s.pop() for _ in range(len(s))] == [7, 2, 0, 6, 4, 5, 1, 3]


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
@pytest.mark.parametrize("method,size", [("popleft", 100_000), ("popright", 5_000_000)])
def test_insertion_sorted_set_threadsafe(method, size):
    s = InsertionSortedSet(range(size))
    m = getattr(s, method)
    barrier = Barrier(2)

    def t():
        barrier.wait()
        n = 0
        prev = -1 if method == "popleft" else size
        while True:
            try:
                v = m()
                assert v > prev if method == "popleft" else v < prev, (v, prev, len(s))
                prev = v
                n += 1
            except KeyError:
                assert not s
                return n

    with ThreadPoolExecutor(2) as ex:
        f1 = ex.submit(t)
        f2 = ex.submit(t)
        # On Linux, these are in the 38_000 ~ 62_000 range.
        # On Windows, we've seen as little as 2300.
        assert f1.result() > 100
        assert f2.result() > 100
