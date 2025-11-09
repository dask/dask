from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class RLock:
    """asyncio reentrant lock, which allows the same owner (or two owners that compare
    as equal) to be inside the critical section at the same time.

    Note that here the owner is an explicit, generic object, whereas in
    :func:`threading.RLock` and :func:`multiprocessing.RLock` it's hardcoded
    respectively to the thread ID and process ID.

    **Usage**::

        lock = RLock()
        async with lock("my-owner"):
            ...

    **Tip**

    You can mix reentrant and non-reentrant owners; all you need to do is create an
    owner that doesn't compare as equal to other instances of itself::

        lock = RLock()

        async def non_reentrant():
            async with lock(object()):
                ...

        async def reentrant():
            async with lock("foo"):
                ...

    In the above example, at any time you may have inside the critical section
    at most one call to ``non_reentrant`` and no calls to ``reentrant``, or any number
    of calls to ``reentrant`` but no calls to ``non_reentrant``.
    """

    _owner: object
    _count: int
    _lock: asyncio.Lock

    def __init__(self):
        self._owner = None
        self._count = 0
        self._lock = asyncio.Lock()

    async def acquire(self, owner: object) -> None:
        if self._count == 0 or self._owner != owner:
            await self._lock.acquire()
            self._owner = owner
        self._count += 1

    def release(self, owner: object) -> None:
        if self._count == 0 or self._owner != owner:
            raise RuntimeError("release unlocked lock or mismatched owner")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    @asynccontextmanager
    async def __call__(self, owner: object) -> AsyncIterator[None]:
        await self.acquire(owner)
        try:
            yield
        finally:
            self.release(owner)
