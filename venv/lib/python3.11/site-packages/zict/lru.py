from __future__ import annotations

from collections.abc import (
    Callable,
    Collection,
    ItemsView,
    Iterator,
    KeysView,
    MutableMapping,
    ValuesView,
)

from zict.common import KT, VT, NoDefault, ZictBase, close, flush, locked, nodefault
from zict.utils import InsertionSortedSet


class LRU(ZictBase[KT, VT]):
    """Evict Least Recently Used Elements.

    Parameters
    ----------
    n: int or float
        Number of elements to keep, or total weight if ``weight`` is used.
        Any individual key that is heavier than n will be automatically evicted as soon
        as it is inserted.

        It can be updated after initialization. See also: ``offset`` attribute.
    d: MutableMapping
        Dict-like in which to hold elements. There are no expectations on its internal
        ordering. Iteration on the LRU follows the order of the underlying mapping.
    on_evict: callable or list of callables
        Function:: k, v -> action to call on key/value pairs prior to eviction
        If an exception occurs during an on_evict callback (e.g a callback tried
        storing to disk and raised a disk full error) the key will remain in the LRU.
    on_cancel_evict: callable or list of callables
        Function:: k, v -> action to call on key/value pairs if they're deleted or
        updated from a thread while the on_evict callables are being executed in
        another.
        If you're not accessing the LRU from multiple threads, ignore this parameter.
    weight: callable
        Function:: k, v -> number to determine the size of keeping the item in
        the mapping.  Defaults to ``(k, v) -> 1``

    Notes
    -----
    If you call methods of this class from multiple threads, access will be fast as long
    as all methods of ``d`` are fast. Callbacks are not protected by locks and can be
    arbitrarily slow.

    Examples
    --------
    >>> lru = LRU(2, {}, on_evict=lambda k, v: print("Lost", k, v))
    >>> lru['x'] = 1
    >>> lru['y'] = 2
    >>> lru['z'] = 3
    Lost x 1
    """

    d: MutableMapping[KT, VT]
    order: InsertionSortedSet[KT]
    heavy: InsertionSortedSet[KT]
    on_evict: list[Callable[[KT, VT], None]]
    on_cancel_evict: list[Callable[[KT, VT], None]]
    weight: Callable[[KT, VT], float]
    #: Maximum weight before eviction is triggered, as set during initialization.
    #: Updating this attribute doesn't trigger eviction by itself; you should call
    #: :meth:`evict_until_below_target` explicitly afterwards.
    n: float
    #: Offset to add to ``total_weight`` to determine if key/value pairs should be
    #: evicted. It always starts at zero and can be updated afterwards. Updating this
    #: attribute doesn't trigger eviction by itself; you should call
    #: :meth:`evict_until_below_target` explicitly afterwards.
    #: Increasing ``offset`` is not the same as reducing ``n``, as the latter will also
    #: reduce the threshold below which a value is considered "heavy" and qualifies for
    #: immediate eviction.
    offset: float
    weights: dict[KT, float]
    closed: bool
    total_weight: float
    _cancel_evict: dict[KT, bool]

    def __init__(
        self,
        n: float,
        d: MutableMapping[KT, VT],
        *,
        on_evict: Callable[[KT, VT], None]
        | list[Callable[[KT, VT], None]]
        | None = None,
        on_cancel_evict: Callable[[KT, VT], None]
        | list[Callable[[KT, VT], None]]
        | None = None,
        weight: Callable[[KT, VT], float] = lambda k, v: 1,
    ):
        super().__init__()
        self.d = d
        self.n = n
        self.offset = 0

        if callable(on_evict):
            on_evict = [on_evict]
        self.on_evict = on_evict or []
        if callable(on_cancel_evict):
            on_cancel_evict = [on_cancel_evict]
        self.on_cancel_evict = on_cancel_evict or []

        self.weight = weight
        self.weights = {k: weight(k, v) for k, v in d.items()}
        self.total_weight = sum(self.weights.values())
        self.order = InsertionSortedSet(d)
        self.heavy = InsertionSortedSet(k for k, v in self.weights.items() if v >= n)
        self.closed = False
        self._cancel_evict = {}

    @locked
    def __getitem__(self, key: KT) -> VT:
        result = self.d[key]
        self.order.remove(key)
        self.order.add(key)
        return result

    @locked
    def get_all_or_nothing(self, keys: Collection[KT]) -> dict[KT, VT]:
        """If all keys exist in the LRU, update their FIFO priority and return their
        values; this would be the same as ``{k: lru[k] for k in keys}``.
        If any keys are missing, however, raise KeyError for the first one missing and
        do not bring any of the available keys to the top of the LRU.
        """
        result = {key: self.d[key] for key in keys}
        for key in keys:
            self.order.remove(key)
            self.order.add(key)
        return result

    def __setitem__(self, key: KT, value: VT) -> None:
        self.set_noevict(key, value)
        try:
            self.evict_until_below_target()
        except Exception:
            if self.weights.get(key, 0) > self.n and key not in self.heavy:
                # weight(value) > n and evicting the key we just inserted failed.
                # Evict the rest of the LRU instead.
                try:
                    while len(self.d) > 1:
                        self.evict()
                except Exception:
                    pass
            raise

    @locked
    def set_noevict(self, key: KT, value: VT) -> None:
        """Variant of ``__setitem__`` that does not evict if the total weight exceeds n.
        Unlike ``__setitem__``, this method does not depend on the ``on_evict``
        functions to be thread-safe for its own thread-safety. It also is not prone to
        re-raising exceptions from the ``on_evict`` callbacks.
        """
        self.discard(key)
        weight = self.weight(key, value)
        if key in self._cancel_evict:
            self._cancel_evict[key] = True
        self.d[key] = value
        self.order.add(key)
        if weight > self.n:
            self.heavy.add(key)  # Mark this key to be evicted first
        self.weights[key] = weight
        self.total_weight += weight

    def evict_until_below_target(self, n: float | None = None) -> None:
        """Evict key/value pairs until the total weight falls below n

        Parameters
        ----------
        n: float, optional
            Total weight threshold to achieve. Defaults to self.n.
        """
        if n is None:
            n = self.n
        while self.total_weight + self.offset > n and not self.closed:
            try:
                self.evict()
            except KeyError:
                return  # Multithreaded race condition

    @locked
    def evict(
        self, key: KT | NoDefault = nodefault
    ) -> tuple[KT, VT, float] | tuple[None, None, float]:
        """Evict least recently used key, or least recently inserted key with individual
        weight > n, if any. You may also evict a specific key.

        This is typically called from internal use, but can be externally
        triggered as well.

        Returns
        -------
        Tuple of (key, value, weight)

        Or (None, None, 0) if the key that was being evicted was updated or deleted from
        another thread while the on_evict callbacks were being executed. This outcome is
        only possible in multithreaded access.
        """
        if key is nodefault:
            try:
                key = next(iter(self.heavy or self.order))
            except StopIteration:
                raise KeyError("evict(): dictionary is empty")

        if key in self._cancel_evict:
            return None, None, 0

        # For the purpose of multithreaded access, it's important that the value remains
        # in self.d until all callbacks are successful.
        # When this is used inside a Buffer, there must never be a moment when the key
        # is neither in fast nor in slow.
        value = self.d[key]

        # If we are evicting a heavy key we just inserted and one of the callbacks
        # fails, put it at the bottom of the LRU instead of the top. This way lighter
        # keys will have a chance to be evicted first and make space.
        self.heavy.discard(key)

        self._cancel_evict[key] = False
        try:
            with self.unlock():
                # This may raise; e.g. if a callback tries storing to a full disk
                for cb in self.on_evict:
                    cb(key, value)

                if self._cancel_evict[key]:
                    for cb in self.on_cancel_evict:
                        cb(key, value)
                    return None, None, 0
        finally:
            del self._cancel_evict[key]

        del self.d[key]
        self.order.remove(key)
        weight = self.weights.pop(key)
        self.total_weight -= weight

        return key, value, weight

    @locked
    def __delitem__(self, key: KT) -> None:
        if key in self._cancel_evict:
            self._cancel_evict[key] = True
        del self.d[key]
        self.order.remove(key)
        self.heavy.discard(key)
        self.total_weight -= self.weights.pop(key)

    def keys(self) -> KeysView[KT]:
        return self.d.keys()

    def values(self) -> ValuesView[VT]:
        return self.d.values()

    def items(self) -> ItemsView[KT, VT]:
        return self.d.items()

    def __len__(self) -> int:
        return len(self.d)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.d)

    def __contains__(self, key: object) -> bool:
        return key in self.d

    def __str__(self) -> str:
        sub = str(self.d) if not isinstance(self.d, dict) else "dict"
        return f"<LRU: {self.total_weight + self.offset}/{self.n} on {sub}>"

    __repr__ = __str__

    def flush(self) -> None:
        flush(self.d)

    def close(self) -> None:
        self.closed = True
        close(self.d)
