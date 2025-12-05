from __future__ import annotations

import logging
import uuid

from distributed.semaphore import Semaphore

logger = logging.getLogger(__name__)

_no_value = object()


class Lock(Semaphore):
    """Distributed Centralized Lock

    .. warning::

        This is using the ``distributed.Semaphore`` as a backend, which is
        susceptible to lease overbooking. For the Lock this means that if a
        lease is timing out, two or more instances could acquire the lock at the
        same time. To disable lease timeouts, set
        ``distributed.scheduler.locks.lease-timeout`` to `inf`, e.g.

        .. code-block:: python

            with dask.config.set({"distributed.scheduler.locks.lease-timeout": "inf"}):
                lock = Lock("x")
                ...

        Note, that without lease timeouts, the Lock may deadlock in case of
        cluster downscaling or worker failures.

    Parameters
    ----------
    name: string (optional)
        Name of the lock to acquire.  Choosing the same name allows two
        disconnected processes to coordinate a lock.  If not given, a random
        name will be generated.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> lock = Lock('x')  # doctest: +SKIP
    >>> lock.acquire(timeout=1)  # doctest: +SKIP
    >>> # do things with protected resource
    >>> lock.release()  # doctest: +SKIP
    """

    def __init__(
        self,
        name=None,
        client=_no_value,
        scheduler_rpc=None,
        loop=None,
    ):
        if client is not _no_value:
            import warnings

            warnings.warn(
                "The `client` parameter is deprecated. It is no longer necessary to pass a client to Lock.",
                DeprecationWarning,
                stacklevel=2,
            )

        self.name = name or "lock-" + uuid.uuid4().hex
        super().__init__(
            max_leases=1,
            name=name,
            scheduler_rpc=scheduler_rpc,
            loop=loop,
        )

    def acquire(self, blocking=True, timeout=None):
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

        Examples
        --------
        >>> lock = Lock('x')  # doctest: +SKIP
        >>> lock.acquire(timeout="1s")  # doctest: +SKIP

        Returns
        -------
        True or False whether or not it successfully acquired the lock
        """
        if not blocking:
            if timeout is not None:
                raise ValueError("can't specify a timeout for a non-blocking call")
            timeout = 0
        return super().acquire(timeout=timeout)

    async def _locked(self):
        val = await self.scheduler.semaphore_value(name=self.name)
        return val == 1

    def locked(self):
        return self.sync(self._locked)

    def __getstate__(self):
        return self.name

    def __setstate__(self, state):
        self.__init__(name=state)
