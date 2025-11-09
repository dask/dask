from __future__ import annotations

import heapq
import itertools
import weakref
from collections import OrderedDict, UserDict
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping, MutableSet
from typing import Any, TypeVar, cast

T = TypeVar("T", bound=Hashable)
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class LRU(UserDict[K, V]):
    """Limited size mapping, evicting the least recently looked-up key when full"""

    def __init__(self, maxsize: float) -> None:
        super().__init__()
        self.data = OrderedDict()
        self.maxsize = maxsize

    def __getitem__(self, key: K) -> V:
        value = super().__getitem__(key)
        cast(OrderedDict, self.data).move_to_end(key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        if len(self) >= self.maxsize:
            cast(OrderedDict, self.data).popitem(last=False)
        super().__setitem__(key, value)


class HeapSet(MutableSet[T]):
    """A set-like where the `pop` method returns the smallest item, as sorted by an
    arbitrary key function. Ties are broken by oldest first.

    Values must be compatible with :mod:`weakref`.

    Parameters
    ----------
    key: Callable
        A function that takes a single element of the collection as a parameter and
        returns a sorting key. The key does not need to be hashable and does not need to
        support :mod:`weakref`.

    Note
    ----
    The key returned for each element should not to change over time. If it does, the
    position in the heap won't change, even if the element is re-added, and it *may* not
    change even if it's discarded and then re-added later.
    """

    __slots__ = ("key", "_data", "_heap", "_inc", "_sorted")
    key: Callable[[T], Any]
    _data: set[T]
    _heap: list[tuple[Any, int, weakref.ref[T]]]
    _inc: int
    _sorted: bool

    def __init__(self, *, key: Callable[[T], Any]):
        self.key = key
        self._data = set()
        self._inc = 0
        self._heap = []
        self._sorted = True

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items>"

    def __reduce__(self) -> tuple[Callable, tuple]:
        heap = [(k, i, v) for k, i, vref in self._heap if (v := vref()) in self._data]
        return HeapSet._unpickle, (self.key, self._inc, heap)

    @staticmethod
    def _unpickle(
        key: Callable[[T], Any], inc: int, heap: list[tuple[Any, int, T]]
    ) -> HeapSet[T]:
        self = object.__new__(HeapSet)
        self.key = key
        self._data = {v for _, _, v in heap}
        self._inc = inc
        self._heap = [(k, i, weakref.ref(v)) for k, i, v in heap]
        heapq.heapify(self._heap)
        self._sorted = not heap
        return self

    def __contains__(self, value: object) -> bool:
        return value in self._data

    def __len__(self) -> int:
        return len(self._data)

    def add(self, value: T) -> None:
        if value in self._data:
            return
        k = self.key(value)
        vref = weakref.ref(value)
        heapq.heappush(self._heap, (k, self._inc, vref))
        self._sorted = False
        self._data.add(value)
        self._inc += 1

    def discard(self, value: T) -> None:
        self._data.discard(value)
        if not self._data:
            self.clear()

    def peek(self) -> T:
        """Return the smallest element without removing it"""
        if not self._data:
            raise KeyError("peek into empty set")
        while True:
            value = self._heap[0][2]()
            if value in self._data:
                return value
            heapq.heappop(self._heap)
            self._sorted = False

    def peekn(self, n: int) -> Iterator[T]:
        """Iterate over the n smallest elements without removing them.
        This is O(1) for n == 1; O(n*logn) otherwise.
        """
        if n <= 0 or not self:
            return  # empty iterator
        if n == 1:
            yield self.peek()
        else:
            # NOTE: we could pop N items off the queue, then push them back.
            # But copying the list N times is probably slower than just sorting it
            # with fast C code.
            # If we had a `heappop` that sliced the list instead of popping from it,
            # we could implement an optimized version for small `n`s.
            yield from itertools.islice(self.sorted(), n)

    def pop(self) -> T:
        if not self._data:
            raise KeyError("pop from an empty set")
        while True:
            _, _, vref = heapq.heappop(self._heap)
            self._sorted = False
            value = vref()
            if value in self._data:
                self._data.discard(value)
                if not self._data:
                    self.clear()
                return value

    def peekright(self) -> T:
        """Return one of the largest elements (not necessarily the largest!) without
        removing it. It's guaranteed that ``self.peekright() >= self.peek()``.
        """
        if not self._data:
            raise KeyError("peek into empty set")
        while True:
            value = self._heap[-1][2]()
            if value in self._data:
                return value
            del self._heap[-1]

    def popright(self) -> T:
        """Remove and return one of the largest elements (not necessarily the largest!)
        It's guaranteed that ``self.popright() >= self.peek()``.
        """
        if not self._data:
            raise KeyError("pop from an empty set")
        while True:
            _, _, vref = self._heap.pop()
            value = vref()
            if value in self._data:
                self._data.discard(value)
                if not self._data:
                    self.clear()
                return value

    def __iter__(self) -> Iterator[T]:
        """Iterate over all elements. This is a O(n) operation which returns the
        elements in pseudo-random order.
        """
        return iter(self._data)

    def sorted(self) -> Iterator[T]:
        """Iterate over all elements. This is a O(n*logn) operation which returns the
        elements in order, from smallest to largest according to the key and insertion
        order.
        """
        if not self._sorted:
            self._heap.sort()  # A sorted list maintains the heap invariant
            self._sorted = True
        seen = set()
        for _, _, vref in self._heap:
            value = vref()
            if value in self._data and value not in seen:
                yield value
                seen.add(value)

    def clear(self) -> None:
        self._data.clear()
        self._heap.clear()
        self._sorted = True


def sum_mappings(ds: Iterable[Mapping[K, V] | Iterable[tuple[K, V]]], /) -> dict[K, V]:
    """Sum the values of the given mappings, key by key"""
    out: dict[K, V] = {}
    for d in ds:
        if isinstance(d, Mapping):
            d = d.items()
        for k, v in d:
            try:
                out[k] += v  # type: ignore
            except KeyError:
                out[k] = v
    return out
