from __future__ import annotations

import pathlib
import sys
from collections.abc import Iterable, Iterator
from typing import (  # TODO import from collections.abc (needs Python >=3.9)
    ItemsView,
    ValuesView,
)

from zict.common import ZictBase


def _encode_key(key: str) -> bytes:
    return key.encode("utf-8")


def _decode_key(key: bytes) -> str:
    return key.decode("utf-8")


class LMDB(ZictBase[str, bytes]):
    """Mutable Mapping interface to a LMDB database.

    Keys must be strings, values must be bytes

    Parameters
    ----------
    directory: str
    map_size: int
        On Linux and MacOS, maximum size of the database file on disk.
        Defaults to 1 TiB on 64 bit systems and 1 GiB on 32 bit ones.

        On Windows, preallocated total size of the database file on disk. Defaults to
        10 MiB to encourage explicitly setting it.

    Notes
    -----
    None of this class is thread-safe - not even normally trivial methods such as
    ``__len__ `` or ``__contains__``.

    Examples
    --------
    >>> z = LMDB('/tmp/somedir/')  # doctest: +SKIP
    >>> z['x'] = b'123'  # doctest: +SKIP
    >>> z['x']  # doctest: +SKIP
    b'123'
    """

    def __init__(self, directory: str | pathlib.Path, map_size: int | None = None):
        import lmdb

        super().__init__()
        if map_size is None:
            if sys.platform != "win32":
                map_size = min(2**40, sys.maxsize // 4)
            else:
                map_size = 10 * 2**20

        self.db = lmdb.open(
            str(directory),
            subdir=True,
            map_size=map_size,
            sync=False,
            writemap=True,
        )

    def __getitem__(self, key: str) -> bytes:
        if not isinstance(key, str):
            raise KeyError(key)
        with self.db.begin() as txn:
            value = txn.get(_encode_key(key))
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: bytes) -> None:
        if not isinstance(key, str):
            raise TypeError(key)
        if not isinstance(value, bytes):
            raise TypeError(value)
        with self.db.begin(write=True) as txn:
            txn.put(_encode_key(key), value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        with self.db.begin() as txn:
            return txn.cursor().set_key(_encode_key(key))

    def __iter__(self) -> Iterator[str]:
        cursor = self.db.begin().cursor()
        return (_decode_key(k) for k in cursor.iternext(keys=True, values=False))

    def items(self) -> ItemsView[str, bytes]:
        return LMDBItemsView(self)

    def values(self) -> ValuesView[bytes]:
        return LMDBValuesView(self)

    def _do_update(self, items: Iterable[tuple[str, bytes]]) -> None:
        # Optimized version of update() using a single putmulti() call.
        items_enc = []
        for key, value in items:
            if not isinstance(key, str):
                raise TypeError(key)
            if not isinstance(value, bytes):
                raise TypeError(value)
            items_enc.append((_encode_key(key), value))

        with self.db.begin(write=True) as txn:
            consumed, added = txn.cursor().putmulti(items_enc)
            assert consumed == added == len(items_enc)

    def __delitem__(self, key: str) -> None:
        if not isinstance(key, str):
            raise KeyError(key)
        with self.db.begin(write=True) as txn:
            if not txn.delete(_encode_key(key)):
                raise KeyError(key)

    def __len__(self) -> int:
        return self.db.stat()["entries"]

    def close(self) -> None:
        self.db.close()


class LMDBItemsView(ItemsView[str, bytes]):
    _mapping: LMDB  # FIXME CPython implementation detail
    __slots__ = ()

    def __contains__(self, item: object) -> bool:
        key: str
        value: object
        key, value = item  # type: ignore
        try:
            v = self._mapping[key]
        except KeyError:
            return False
        else:
            return v == value

    def __iter__(self) -> Iterator[tuple[str, bytes]]:
        cursor = self._mapping.db.begin().cursor()
        return ((_decode_key(k), v) for k, v in cursor.iternext(keys=True, values=True))


class LMDBValuesView(ValuesView[bytes]):
    _mapping: LMDB  # FIXME CPython implementation detail
    __slots__ = ()

    def __contains__(self, value: object) -> bool:
        return any(value == v for v in self)

    def __iter__(self) -> Iterator[bytes]:
        cursor = self._mapping.db.begin().cursor()
        return cursor.iternext(keys=False, values=True)
