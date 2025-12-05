from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from typing import Generic, TypeVar

from zict.common import KT, VT, ZictBase, close, discard, flush, locked

MKT = TypeVar("MKT")


class Sieve(ZictBase[KT, VT], Generic[KT, VT, MKT]):
    """Store values in different mappings based on a selector's
    output.

    This creates a MutableMapping combining several underlying
    MutableMappings for storage.  Items are dispatched based on
    a selector function provided by the user.

    Parameters
    ----------
    mappings: dict of {mapping key: MutableMapping}
    selector: callable (key, value) -> mapping key

    Notes
    -----
    If you call methods of this class from multiple threads, access will be fast as long
    as the ``__contains__`` and ``__delitem__`` methods of all underlying mappins are
    fast. ``__getitem__`` and ``__setitem__`` methods of the underlying mappings are not
    protected by locks.

    Examples
    --------
    >>> small = {}
    >>> large = DataBase()                        # doctest: +SKIP
    >>> mappings = {True: small, False: large}    # doctest: +SKIP
    >>> def is_small(key, value):                 # doctest: +SKIP
    ...     return sys.getsizeof(value) < 10000   # doctest: +SKIP
    >>> d = Sieve(mappings, is_small)             # doctest: +SKIP
    """

    mappings: Mapping[MKT, MutableMapping[KT, VT]]
    selector: Callable[[KT, VT], MKT]
    key_to_mapping: dict[KT, MutableMapping[KT, VT]]
    gen: int

    def __init__(
        self,
        mappings: Mapping[MKT, MutableMapping[KT, VT]],
        selector: Callable[[KT, VT], MKT],
    ):
        super().__init__()
        self.mappings = mappings
        self.selector = selector
        self.key_to_mapping = {}
        self.gen = 0

    def __getitem__(self, key: KT) -> VT:
        # Note that this may raise KeyError if you call it in the middle of __setitem__
        # or update for an already existing key
        return self.key_to_mapping[key][key]

    @locked
    def __setitem__(self, key: KT, value: VT) -> None:
        discard(self, key)
        mkey = self.selector(key, value)
        mapping = self.mappings[mkey]
        self.key_to_mapping[key] = mapping
        self.gen += 1
        gen = self.gen

        with self.unlock():
            mapping[key] = value

        if gen != self.gen and self.key_to_mapping.get(key) is not mapping:
            # Multithreaded race condition
            discard(mapping, key)

    @locked
    def __delitem__(self, key: KT) -> None:
        mapping = self.key_to_mapping.pop(key)
        self.gen += 1
        discard(mapping, key)

    @locked
    def _do_update(self, items: Iterable[tuple[KT, VT]]) -> None:
        # Optimized update() implementation issuing a single update()
        # call per underlying mapping.
        updates = defaultdict(list)
        self.gen += 1
        gen = self.gen

        for key, value in items:
            old_mapping = self.key_to_mapping.pop(key, None)
            if old_mapping is not None:
                discard(old_mapping, key)
            mkey = self.selector(key, value)
            mapping = self.mappings[mkey]
            updates[mkey].append((key, value))
            self.key_to_mapping[key] = mapping

        with self.unlock():
            for mkey, mitems in updates.items():
                mapping = self.mappings[mkey]
                mapping.update(mitems)

        if gen != self.gen:
            # Multithreaded race condition
            for mkey, mitems in updates.items():
                mapping = self.mappings[mkey]
                for key, _ in mitems:
                    if self.key_to_mapping.get(key) is not mapping:
                        discard(mapping, key)

    def __len__(self) -> int:
        return len(self.key_to_mapping)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.key_to_mapping)

    def __contains__(self, key: object) -> bool:
        return key in self.key_to_mapping

    def __str__(self) -> str:
        return f"Sieve<{self.mappings}>"

    __repr__ = __str__

    def flush(self) -> None:
        flush(*self.mappings.values())

    def close(self) -> None:
        close(*self.mappings.values())
