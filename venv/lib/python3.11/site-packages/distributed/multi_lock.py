from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Hashable

from dask.utils import parse_timedelta

from distributed.client import Client
from distributed.utils import TimeoutError, log_errors, wait_for
from distributed.worker import get_worker

logger = logging.getLogger(__name__)


class MultiLockExtension:
    """An extension for the scheduler to manage MultiLocks

    This adds the following routes to the scheduler

    *  multi_lock_acquire
    *  multi_lock_release

    The approach is to maintain `self.locks` that maps a lock (unique name given to
    `MultiLock(names=, ...)` at creation) to a list of users (instances of `MultiLock`)
    that "requests" the lock. Additionally, `self.requests` maps a user to its requested
    locks and `self.requests_left` maps a user to the number of locks still need.

    Every time a user `x` gets to the front in `self.locks[name] = [x, ...]` it means
    that `x` now holds the lock `name` and when it holds all the requested locks
    `acquire()` can return.

    Finally, `self.events` contains all the events users are waiting on to finish.
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.locks = defaultdict(list)  # lock -> users
        self.requests = {}  # user -> locks
        self.requests_left = {}  # user -> locks still needed
        self.events = {}

        self.scheduler.handlers.update(
            {"multi_lock_acquire": self.acquire, "multi_lock_release": self.release}
        )

    def _request_locks(self, locks: list[str], id: Hashable, num_locks: int) -> bool:
        """Request locks

        Parameters
        ----------
        locks: List[str]
            Names of the locks to request.
        id: Hashable
            Identifier of the `MultiLock` instance requesting the locks.
        num_locks: int
            Number of locks in `locks` requesting

        Return
        ------
        result: bool
            Whether `num_locks` requested locks are free immediately or not.
        """
        assert id not in self.requests
        self.requests[id] = set(locks)
        assert len(locks) >= num_locks and num_locks > 0
        self.requests_left[id] = num_locks

        locks = sorted(locks, key=lambda x: len(self.locks[x]))
        for i, lock in enumerate(locks):
            self.locks[lock].append(id)
            if len(self.locks[lock]) == 1:  # The lock was free
                self.requests_left[id] -= 1
                if self.requests_left[id] == 0:  # Got all locks needed
                    # Since we got all locks need, we can remove the rest of the requests
                    self.requests[id] -= set(locks[i + 1 :])
                    return True
        return False

    def _refain_locks(self, locks, id):
        """Cancel/release previously requested/acquired locks

        Parameters
        ----------
        locks: List[str]
            Names of the locks to refain.
        id: Hashable
            Identifier of the `MultiLock` instance refraining the locks.
        """
        waiters_ready = set()
        for lock in locks:
            if self.locks[lock][0] == id:
                self.locks[lock].pop(0)
                if self.locks[lock]:
                    new_first = self.locks[lock][0]
                    self.requests_left[new_first] -= 1
                    if self.requests_left[new_first] <= 0:
                        # Notice, `self.requests_left[new_first]` might go below zero
                        # if more locks are freed than requested.
                        self.requests_left[new_first] = 0
                        waiters_ready.add(new_first)
            else:
                self.locks[lock].remove(id)
            assert id not in self.locks[lock]
        del self.requests[id]
        del self.requests_left[id]

        for waiter in waiters_ready:
            self.scheduler.loop.add_callback(self.events[waiter].set)

    @log_errors
    async def acquire(self, locks=None, id=None, timeout=None, num_locks=None):
        if not self._request_locks(locks, id, num_locks):
            assert id not in self.events
            event = asyncio.Event()
            self.events[id] = event
            future = event.wait()
            if timeout is not None:
                future = wait_for(future, timeout)
            try:
                await future
            except TimeoutError:
                self._refain_locks(locks, id)
                return False
            finally:
                del self.events[id]
        # At this point `id` acquired all `locks`
        assert self.requests_left[id] == 0
        return True

    @log_errors
    def release(self, id=None):
        self._refain_locks(self.requests[id], id)


class MultiLock:
    """Distributed Centralized Lock

    Parameters
    ----------
    names
        Names of the locks to acquire. Choosing the same name allows two
        disconnected processes to coordinate a lock.
    client
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> lock = MultiLock(['x', 'y'])  # doctest: +SKIP
    >>> lock.acquire(timeout=1)  # doctest: +SKIP
    >>> # do things with protected resource 'x' and 'y'
    >>> lock.release()  # doctest: +SKIP
    """

    def __init__(self, names: list[str] | None = None, client: Client | None = None):
        try:
            self.client = client or Client.current()
        except ValueError:
            # Initialise new client
            self.client = get_worker().client

        self.names = names or []
        self.id = uuid.uuid4().hex
        self._locked = False

    def acquire(self, blocking=True, timeout=None, num_locks=None):
        """Acquire the lock

        Parameters
        ----------
        blocking : bool, optional
            If false, don't wait on the lock in the scheduler at all.
        timeout : string or number or timedelta, optional
            Seconds to wait on the lock in the scheduler.  This does not
            include local coroutine time, network transfer time, etc..
            It is forbidden to specify a timeout when blocking is false.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        num_locks : int, optional
            Number of locks needed. If None, all locks are needed

        Examples
        --------
        >>> lock = MultiLock(['x', 'y'])  # doctest: +SKIP
        >>> lock.acquire(timeout="1s")  # doctest: +SKIP

        Returns
        -------
        True or False whether or not it successfully acquired the lock
        """
        timeout = parse_timedelta(timeout)

        if not blocking:
            if timeout is not None:
                raise ValueError("can't specify a timeout for a non-blocking call")
            timeout = 0

        result = self.client.sync(
            self.client.scheduler.multi_lock_acquire,
            locks=self.names,
            id=self.id,
            timeout=timeout,
            num_locks=num_locks or len(self.names),
        )
        self._locked = True
        return result

    def release(self):
        """Release the lock if already acquired"""
        if not self.locked():
            raise ValueError("Lock is not yet acquired")
        ret = self.client.sync(self.client.scheduler.multi_lock_release, id=self.id)
        self._locked = False
        return ret

    def locked(self):
        return self._locked

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.release()

    def __reduce__(self):
        return (type(self), (self.names,))
