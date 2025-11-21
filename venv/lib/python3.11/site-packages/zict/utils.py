from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import MutableSet  # TODO import from collections.abc (needs Python >=3.9)

from zict.common import T


class InsertionSortedSet(MutableSet[T]):
    """A set-like that retains insertion order, like a dict. Thread-safe.

    Equality does not compare order or class, but only compares against the contents of
    any other set-like, coherently with dict and the AbstractSet design.
    """

    _d: dict[T, None]
    __slots__ = ("_d",)

    def __init__(self, other: Iterable[T] = ()) -> None:
        self._d = dict.fromkeys(other)

    def __contains__(self, item: object) -> bool:
        return item in self._d

    def __iter__(self) -> Iterator[T]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def add(self, value: T) -> None:
        """Add element to the set. If the element is already in the set, retain original
        insertion order.
        """
        self._d[value] = None

    def discard(self, value: T) -> None:
        # Don't trust the thread-safety of self._d.pop(value, None)
        try:
            del self._d[value]
        except KeyError:
            pass

    def remove(self, value: T) -> None:
        del self._d[value]

    def popleft(self) -> T:
        """Pop the oldest-inserted key from the set"""
        while True:
            try:
                value = next(iter(self._d))
                del self._d[value]
                return value
            except StopIteration:
                raise KeyError("pop from an empty set")
            except (KeyError, RuntimeError):
                # Multithreaded race condition
                continue

    def popright(self) -> T:
        """Pop the latest-inserted key from the set"""
        return self._d.popitem()[0]

    pop = popright

    def clear(self) -> None:
        self._d.clear()
