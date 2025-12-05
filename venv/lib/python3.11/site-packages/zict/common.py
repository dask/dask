from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from enum import Enum
from functools import wraps
from itertools import chain
from typing import MutableMapping  # TODO move to collections.abc (needs Python >=3.9)
from typing import TYPE_CHECKING, Any, TypeVar, cast

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")

if TYPE_CHECKING:
    # TODO import ParamSpec from typing (needs Python >=3.10)
    # TODO import Self from typing (needs Python >=3.11)
    from typing_extensions import ParamSpec, Self

    P = ParamSpec("P")


class NoDefault(Enum):
    nodefault = None


nodefault = NoDefault.nodefault


class ZictBase(MutableMapping[KT, VT]):
    """Base class for zict mappings"""

    lock: threading.RLock

    def __init__(self) -> None:
        self.lock = threading.RLock()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state["lock"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__ = state
        self.lock = threading.RLock()

    def update(  # type: ignore[override]
        self,
        other: Mapping[KT, VT] | Iterable[tuple[KT, VT]] = (),
        /,
        **kwargs: VT,
    ) -> None:
        if hasattr(other, "items"):
            other = other.items()
        other = chain(other, kwargs.items())  # type: ignore
        self._do_update(other)

    def _do_update(self, items: Iterable[tuple[KT, VT]]) -> None:
        # Default implementation, can be overriden for speed
        for k, v in items:
            self[k] = v

    def discard(self, key: KT) -> None:
        """Flush *key* if possible.
        Not the same as ``m.pop(key, None)``, as it doesn't trigger ``__getitem__``.
        """
        discard(self, key)

    def close(self) -> None:
        """Release any system resources held by this object"""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    @contextmanager
    def unlock(self) -> Iterator[None]:
        """To be used in a method decorated by ``@locked``.
        Temporarily releases the mapping's RLock.
        """
        self.lock.release()
        try:
            yield
        finally:
            self.lock.acquire()


def close(*z: Any) -> None:
    """Close *z* if possible."""
    for zi in z:
        if hasattr(zi, "close"):
            zi.close()


def flush(*z: Any) -> None:
    """Flush *z* if possible."""
    for zi in z:
        if hasattr(zi, "flush"):
            zi.flush()


def discard(m: MutableMapping[KT, VT], key: KT) -> None:
    """Flush *key* if possible.
    Not the same as ``m.pop(key, None)``, as it doesn't trigger ``__getitem__``.
    """
    try:
        del m[key]
    except KeyError:
        pass


def locked(func: Callable[P, VT]) -> Callable[P, VT]:
    """Decorator for a method of ZictBase, which wraps the whole method in a
    instance-specific (but not key-specific) rlock.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> VT:
        self = cast(ZictBase, args[0])
        with self.lock:
            return func(*args, **kwargs)

    return wrapper
