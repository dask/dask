from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, MutableMapping
from typing import Generic, TypeVar

from zict.common import KT, VT, ZictBase, close, flush

WT = TypeVar("WT")


class Func(ZictBase[KT, VT], Generic[KT, VT, WT]):
    """Buffer a MutableMapping with a pair of input/output functions

    Parameters
    ----------
    dump: callable
        Function to call on value as we set it into the mapping
    load: callable
        Function to call on value as we pull it from the mapping
    d: MutableMapping

    Examples
    --------
    >>> def double(x):
    ...     return x * 2

    >>> def halve(x):
    ...     return x / 2

    >>> d = {}
    >>> f = Func(double, halve, d)
    >>> f['x'] = 10
    >>> d
    {'x': 20}
    >>> f['x']
    10.0
    """

    dump: Callable[[VT], WT]
    load: Callable[[WT], VT]
    d: MutableMapping[KT, WT]

    def __init__(
        self,
        dump: Callable[[VT], WT],
        load: Callable[[WT], VT],
        d: MutableMapping[KT, WT],
    ):
        super().__init__()
        self.dump = dump
        self.load = load
        self.d = d

    def __getitem__(self, key: KT) -> VT:
        return self.load(self.d[key])

    def __setitem__(self, key: KT, value: VT) -> None:
        self.d[key] = self.dump(value)

    def __contains__(self, key: object) -> bool:
        return key in self.d

    def __delitem__(self, key: KT) -> None:
        del self.d[key]

    def _do_update(self, items: Iterable[tuple[KT, VT]]) -> None:
        it = ((k, self.dump(v)) for k, v in items)
        self.d.update(it)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.d)

    def __len__(self) -> int:
        return len(self.d)

    def __str__(self) -> str:
        return f"<Func: {funcname(self.dump)}<->{funcname(self.load)} {self.d}>"

    __repr__ = __str__

    def flush(self) -> None:
        flush(self.d)

    def close(self) -> None:
        close(self.d)


def funcname(func: Callable) -> str:
    """Get the name of a function."""
    while hasattr(func, "func"):
        func = func.func
    try:
        return func.__name__
    except Exception:
        return str(func)
