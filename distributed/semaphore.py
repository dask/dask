import uuid
from collections import defaultdict, deque
from functools import partial
import asyncio
import dask
from asyncio import TimeoutError
from .utils import PeriodicCallback, log_errors, parse_timedelta
from .worker import get_client
from .metrics import time
import warnings
import logging

logger = logging.getLogger(__name__)


class _Watch:
    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None

    def start(self):
        self.started_at = time()

    def leftover(self):
        if self.duration is None:
            return None
        else:
            elapsed = time() - self.started_at
            return max(0, self.duration - elapsed)


class SemaphoreExtension:
    """ An extension for the scheduler to manage Semaphores

    This adds the following routes to the scheduler

    * semaphore_acquire
    * semaphore_release
    * semaphore_create
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.leases = defaultdict(deque)
        self.events = defaultdict(asyncio.Event)
        self.max_leases = dict()
        self.leases_per_client = defaultdict(partial(defaultdict, deque))
        self.scheduler.handlers.update(
            {
                "semaphore_create": self.create,
                "semaphore_acquire": self.acquire,
                "semaphore_release": self.release,
                "semaphore_close": self.close,
            }
        )

        self.scheduler.extensions["semaphores"] = self
        self.pc_validate_leases = PeriodicCallback(
            self._validate_leases,
            1000
            * parse_timedelta(
                dask.config.get(
                    "distributed.scheduler.locks.lease-validation-interval"
                ),
                default="s",
            ),
            io_loop=self.scheduler.loop,
        )
        self.pc_validate_leases.start()
        self._validation_running = False

    # `comm` here is required by the handler interface
    def create(self, comm=None, name=None, max_leases=None):
        # We use `self.max_leases.keys()` as the point of truth to find out if a semaphore with a specific
        # `name` has been created.
        if name not in self.max_leases:
            assert isinstance(max_leases, int), max_leases
            self.max_leases[name] = max_leases
        else:
            if max_leases != self.max_leases[name]:
                raise ValueError(
                    "Inconsistent max leases: %s, expected: %s"
                    % (max_leases, self.max_leases[name])
                )

    async def _get_lease(self, client, name, identifier):
        result = True
        if len(self.leases[name]) < self.max_leases[name]:
            # naive: self.leases[resource] += 1
            # not naive:
            self.leases[name].append(identifier)
            self.leases_per_client[client][name].append(identifier)
        else:
            result = False
        return result

    def _semaphore_exists(self, name):
        if name not in self.max_leases:
            return False
        return True

    async def acquire(
        self, comm=None, name=None, client=None, timeout=None, identifier=None
    ):
        with log_errors():
            if not self._semaphore_exists(name):
                raise RuntimeError(f"Semaphore `{name}` not known or already closed.")

            if isinstance(name, list):
                name = tuple(name)
            w = _Watch(timeout)
            w.start()

            while True:
                # Reset the event and try to get a release. The event will be set if the state
                # is changed and helps to identify when it is worth to retry an acquire
                self.events[name].clear()

                # If we hit the timeout, this cancels the _get_lease
                future = asyncio.wait_for(
                    self._get_lease(client, name, identifier), timeout=w.leftover()
                )

                try:
                    result = await future
                except TimeoutError:
                    result = False

                # If acquiring fails, we wait for the event to be set, i.e. something has
                # been released and we can try to acquire again (continue loop)
                if not result:
                    future = asyncio.wait_for(
                        self.events[name].wait(), timeout=w.leftover()
                    )
                    try:
                        await future
                        continue
                    except TimeoutError:
                        result = False
                return result

    def release(self, comm=None, name=None, client=None, identifier=None):
        with log_errors():
            if not self._semaphore_exists(name):
                logger.warning(
                    f"Tried to release semaphore `{name}` but it is not known or already closed."
                )
                return
            if isinstance(name, list):
                name = tuple(name)
            if name in self.leases and identifier in self.leases[name]:
                self._release_value(name, client, identifier)
            else:
                raise ValueError(
                    f"Tried to release semaphore but it was already released: "
                    f"client={client}, name={name}, identifier={identifier}"
                )

    def _release_value(self, name, client, identifier):
        # Everything needs to be atomic here.
        self.leases_per_client[client][name].remove(identifier)
        self.leases[name].remove(identifier)
        self.events[name].set()

    def _release_client(self, client):
        semaphore_names = list(self.leases_per_client[client])
        for name in semaphore_names:
            ids = list(self.leases_per_client[client][name])
            for _id in list(ids):
                self._release_value(name=name, client=client, identifier=_id)
        del self.leases_per_client[client]

    def _validate_leases(self):
        if not self._validation_running:
            self._validation_running = True
            known_clients_with_leases = set(self.leases_per_client.keys())
            scheduler_clients = set(self.scheduler.clients.keys())
            for dead_client in known_clients_with_leases - scheduler_clients:
                self._release_client(dead_client)
            else:
                self._validation_running = False

    def close(self, comm=None, name=None):
        """Hard close the semaphore without warning clients which still hold a lease."""
        with log_errors():
            if not self._semaphore_exists(name):
                return

            del self.max_leases[name]
            if name in self.events:
                del self.events[name]
            if name in self.leases:
                del self.leases[name]

            for client, client_leases in self.leases_per_client.items():
                if name in client_leases:
                    warnings.warn(
                        f"Closing semaphore `{name}` but client `{client}` still has a lease open.",
                        RuntimeWarning,
                    )
                    del client_leases[name]


class Semaphore:
    """ Semaphore

    Parameters
    ----------
    max_leases: int (optional)
        The maximum amount of leases that may be granted at the same time. This
        effectively sets an upper limit to the amount of parallel access to a specific resource.
        Defaults to 1.
    name: string (optional)
        Name of the semaphore to acquire.  Choosing the same name allows two
        disconnected processes to coordinate.  If not given, a random
        name will be generated.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> from distributed import Semaphore
    >>> sem = Semaphore(max_leases=2, name='my_database')
    >>> def access_resource(s, sem):
    >>>     # This automatically acquires a lease from the semaphore (if available) which will be
    >>>     # released when leaving the context manager.
    >>>     with sem:
    >>>         pass
    >>>
    >>> futures = client.map(access_resource, range(10), sem=sem)
    >>> client.gather(futures)
    >>> # Once done, close the semaphore to clean up the state on scheduler side.
    >>> sem.close()

    Notes
    -----
    If a client attempts to release the semaphore but doesn't have a lease acquired, this will raise an exception.


    When a semaphore is closed, if, for that closed semaphore, a client attempts to:

    - Acquire a lease: an exception will be raised.
    - Release: a warning will be logged.
    - Close: nothing will happen.


    dask executes functions by default assuming they are pure, when using semaphore acquire/releases inside
    such a function, it must be noted that there *are* in fact side-effects, thus, the function can no longer be
    considered pure. If this is not taken into account, this may lead to unexpected behavior.

    """

    def __init__(self, max_leases=1, name=None, client=None):
        # NOTE: the `id` of the `Semaphore` instance will always be unique, even among different
        # instances for the same resource. The actual attribute that identifies a specific resource is `name`,
        # which will be the same for all instances of this class which limit the same resource.
        self.client = client or get_client()
        self.id = uuid.uuid4().hex
        self.name = name or "semaphore-" + uuid.uuid4().hex
        self.max_leases = max_leases

        if self.client.asynchronous:
            self._started = self.client.scheduler.semaphore_create(
                name=self.name, max_leases=max_leases
            )
        else:
            self.client.sync(
                self.client.scheduler.semaphore_create,
                name=self.name,
                max_leases=max_leases,
            )
            self._started = asyncio.sleep(0)

    def __await__(self):
        async def create_semaphore():
            await self._started
            return self

        return create_semaphore().__await__()

    def acquire(self, timeout=None):
        """
        Acquire a semaphore.

        If the internal counter is greater than zero, decrement it by one and return True immediately.
        If it is zero, wait until a release() is called and return True.
        """
        # TODO: This (may?) keep the HTTP request open until timeout runs out (forever if None).
        #  Can do this in batches of smaller timeouts.
        # TODO: what if connection breaks up?
        return self.client.sync(
            self.client.scheduler.semaphore_acquire,
            name=self.name,
            timeout=timeout,
            client=self.client.id,
            identifier=self.id,
        )

    def release(self):
        """
        Release a semaphore.

        Increment the internal counter by one.
        """

        """ Release the lock if already acquired """
        # TODO: what if connection breaks up?
        return self.client.sync(
            self.client.scheduler.semaphore_release,
            name=self.name,
            client=self.client.id,
            identifier=self.id,
        )

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.release()

    def __getstate__(self):
        # Do not serialize the address since workers may have different
        # addresses for the scheduler (e.g. if a proxy is between them)
        return (self.name, self.max_leases)

    def __setstate__(self, state):
        name, max_leases = state
        client = get_client()
        self.__init__(name=name, client=client, max_leases=max_leases)

    def close(self):
        return self.client.sync(self.client.scheduler.semaphore_close, name=self.name)
