from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

from distributed.metrics import context_meter

_T = TypeVar("_T", int, None)


class ResourceLimiter(Generic[_T]):
    """Limit an abstract resource

    This allows us to track usage of an abstract resource. If the usage of this
    resources goes beyond a defined limit, we can block further execution

    Example::

        limiter = ResourceLimiter(2)
        limiter.increase(1)
        limiter.increase(2)
        limiter.decrease(1)

        # This will block since we're still not below limit
        await limiter.wait_for_available()
    """

    limit: _T
    metrics_label: str | None

    _acquired: int
    _condition: asyncio.Condition
    _waiters: int

    def __init__(self, limit: _T, metrics_label: str | None = None):
        self.limit = limit
        self.metrics_label = metrics_label
        self._acquired = 0
        self._condition = asyncio.Condition()
        self._waiters = 0

    def __repr__(self) -> str:
        return f"<ResourceLimiter limit: {self.limit} available: {self.available}>"

    @property
    def available(self) -> _T:
        """How far can the value be increased before blocking"""
        if self.limit is None:
            return self.limit
        return max(0, self.limit - self._acquired)

    @property
    def full(self) -> bool:
        """Return True if the limit has been reached"""
        return self.available is not None and not self.available

    @property
    def empty(self) -> bool:
        """Return True if nothing has been acquired / the limiter is in a neutral state"""
        return self._acquired == 0

    async def wait_for_available(self) -> None:
        """Block until the counter drops below limit"""
        if self.limit is not None:
            # Include non-blocking calls in the calculation of the average
            # seconds / count
            context_meter.digest_metric(self.metrics_label, 1, "count")
        if not self.full:
            return

        with context_meter.meter(self.metrics_label):
            async with self._condition:
                self._waiters += 1
                await self._condition.wait_for(lambda: not self.full)
                self._waiters -= 1

    def increase(self, value: int) -> None:
        """Increase the internal counter by value"""
        self._acquired += value

    async def decrease(self, value: int) -> None:
        """Decrease the internal counter by value"""
        if value > self._acquired:
            raise RuntimeError(
                f"Cannot release more than what was acquired! release: {value} acquired: {self._acquired}"
            )
        self._acquired -= value
        async with self._condition:
            self._condition.notify_all()
