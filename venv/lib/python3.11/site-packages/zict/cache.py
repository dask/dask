from __future__ import annotations

import weakref
from collections.abc import Iterator, MutableMapping
from typing import TYPE_CHECKING

from zict.common import KT, VT, ZictBase, close, discard, flush, locked


class Cache(ZictBase[KT, VT]):
    """Transparent write-through cache around a MutableMapping with an expensive
    __getitem__ method.

    Parameters
    ----------
    data: MutableMapping
        Persistent, slow to read mapping to be cached
    cache: MutableMapping
        Fast cache for reads from data. This mapping may lose keys on its own; e.g. it
        could be a LRU.
    update_on_set: bool, optional
        If True (default), the cache will be updated both when writing and reading.
        If False, update the cache when reading, but just invalidate it when writing.

    Notes
    -----
    If you call methods of this class from multiple threads, access will be fast as long
    as all methods of ``cache``, plus ``data.__delitem__``, are fast. Other methods of
    ``data`` are not protected by locks.

    Examples
    --------
    Keep the latest 100 accessed values in memory
    >>> from zict import Cache, File, LRU, WeakValueMapping
    >>> d = Cache(File('myfile'), LRU(100, {}))  # doctest: +SKIP

    Read data from disk every time, unless it was previously accessed and it's still in
    use somewhere else in the application
    >>> d = Cache(File('myfile'), WeakValueMapping())  # doctest: +SKIP
    """

    data: MutableMapping[KT, VT]
    cache: MutableMapping[KT, VT]
    update_on_set: bool
    _gen: int
    _last_updated: dict[KT, int]

    def __init__(
        self,
        data: MutableMapping[KT, VT],
        cache: MutableMapping[KT, VT],
        update_on_set: bool = True,
    ):
        super().__init__()
        self.data = data
        self.cache = cache
        self.update_on_set = update_on_set
        self._gen = 0
        self._last_updated = {}

    @locked
    def __getitem__(self, key: KT) -> VT:
        try:
            return self.cache[key]
        except KeyError:
            pass
        gen = self._last_updated[key]

        with self.unlock():
            value = self.data[key]

        # Could another thread have called __setitem__ or __delitem__ on the
        # same key in the meantime? If not, update the cache
        if gen == self._last_updated.get(key):
            self.cache[key] = value
            self._last_updated[key] += 1
        return value

    @locked
    def __setitem__(self, key: KT, value: VT) -> None:
        # If the item was already in cache and data.__setitem__ fails, e.g. because
        # it's a File and the disk is full, make sure that the cache is invalidated.
        discard(self.cache, key)
        gen = self._gen
        gen += 1
        self._last_updated[key] = self._gen = gen

        with self.unlock():
            self.data[key] = value

        if key not in self._last_updated:
            # Another thread called __delitem__ in the meantime
            discard(self.data, key)
        elif gen != self._last_updated[key]:
            # Another thread called __setitem__ in the meantime. We have no idea which
            # of the two ended up actually setting self.data.
            # Case 1: the other thread did not enter this locked code block yet.
            #         Prevent it from setting the cache.
            self._last_updated[key] += 1
            # Case 2: the other thread already exited this locked code block and set the
            #         cache. Invalidate it.
            discard(self.cache, key)
        else:
            # No race condition
            self._last_updated[key] += 1
            if self.update_on_set:
                self.cache[key] = value

    @locked
    def __delitem__(self, key: KT) -> None:
        del self.data[key]
        del self._last_updated[key]
        discard(self.cache, key)

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.data)

    def __contains__(self, key: object) -> bool:
        # Do not let MutableMapping call self.data[key]
        return key in self.data

    def flush(self) -> None:
        flush(self.cache, self.data)

    def close(self) -> None:
        close(self.cache, self.data)


if TYPE_CHECKING:
    # TODO remove this branch and just use [] in the implementation below (needs Python >=3.9)
    class WeakValueMapping(weakref.WeakValueDictionary[KT, VT]):
        ...

else:

    class WeakValueMapping(weakref.WeakValueDictionary):
        """Variant of weakref.WeakValueDictionary which silently ignores objects that
        can't be referenced by a weakref.ref
        """

        def __setitem__(self, key: KT, value: VT) -> None:
            try:
                super().__setitem__(key, value)
            except TypeError:
                pass
