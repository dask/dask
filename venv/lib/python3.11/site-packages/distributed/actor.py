from __future__ import annotations

import abc
import functools
import threading
from collections.abc import Awaitable, Generator
from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, Literal, NoReturn, TypeVar

from tornado.ioloop import IOLoop

from dask._task_spec import TaskRef

from distributed.client import Future
from distributed.protocol import to_serialize
from distributed.utils import LateLoopEvent, iscoroutinefunction, sync, thread_state
from distributed.worker import get_client, get_worker

_T = TypeVar("_T")


class Actor(TaskRef):
    """Controls an object on a remote worker

    An actor allows remote control of a stateful object living on a remote
    worker.  Method calls on this object trigger operations on the remote
    object and return BaseActorFutures on which we can block to get results.

    Examples
    --------
    >>> class Counter:
    ...    def __init__(self):
    ...        self.n = 0
    ...    def increment(self):
    ...        self.n += 1
    ...        return self.n

    >>> from dask.distributed import Client
    >>> client = Client()

    You can create an actor by submitting a class with the keyword
    ``actor=True``.

    >>> future = client.submit(Counter, actor=True)
    >>> counter = future.result()
    >>> counter
    <Actor: Counter, key=Counter-1234abcd>

    Calling methods on this object immediately returns deferred ``BaseActorFuture``
    objects.  You can call ``.result()`` on these objects to block and get the
    result of the function call.

    >>> future = counter.increment()
    >>> future.result()
    1
    >>> future = counter.increment()
    >>> future.result()
    2
    """

    def __init__(self, cls, address, key, worker=None):
        super().__init__(key)
        self._cls = cls
        self._address = address
        self._key = key
        self._future = None
        self._worker = worker
        self._client = None
        self._try_bind_worker_client()

    def _try_bind_worker_client(self):
        if not self._worker:
            try:
                self._worker = get_worker()
            except ValueError:
                self._worker = None
        if not self._client:
            try:
                self._client = get_client()
                self._future = Future(self._key, self._client)
                # ^ When running on a worker, only hold a weak reference to the key, otherwise the key could become unreleasable.
            except ValueError:
                self._client = None

    def __repr__(self):
        return f"<Actor: {self._cls.__name__}, key={self.key}>"

    def __reduce__(self):
        return (Actor, (self._cls, self._address, self.key))

    @property
    def _io_loop(self):
        if self._worker is None and self._client is None:
            self._try_bind_worker_client()
        if self._worker:
            return self._worker.loop
        else:
            return self._client.loop

    @property
    def _scheduler_rpc(self):
        if self._worker is None and self._client is None:
            self._try_bind_worker_client()
        if self._worker:
            return self._worker.scheduler
        else:
            return self._client.scheduler

    @property
    def _worker_rpc(self):
        if self._worker is None and self._client is None:
            self._try_bind_worker_client()
        if self._worker:
            return self._worker.rpc(self._address)
        else:
            if self._client.direct_to_workers:
                return self._client.rpc(self._address)
            else:
                return ProxyRPC(self._client.scheduler, self._address)

    @property
    def _asynchronous(self):
        if self._client:
            return self._client.asynchronous
        else:
            return threading.get_ident() == self._worker.thread_id

    def _sync(self, func, *args, **kwargs):
        if self._client:
            return self._client.sync(func, *args, **kwargs)
        else:
            if self._asynchronous:
                return func(*args, **kwargs)
            return sync(self._worker.loop, func, *args, **kwargs)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(attr for attr in dir(self._cls) if not attr.startswith("_"))
        return sorted(o)

    def __getattr__(self, key):
        if self._future and self._future.status not in ("finished", "pending"):
            raise RuntimeError(
                "Worker holding Actor was lost.  Status: " + self._future.status
            )
        self._try_bind_worker_client()
        if (
            self._worker
            and self._worker.address == self._address
            and getattr(thread_state, "actor", False)
        ):
            # actor calls actor on same worker
            actor = self._worker.actors[self.key]
            attr = getattr(actor, key)

            if iscoroutinefunction(attr):
                return attr

            elif callable(attr):
                return lambda *args, **kwargs: EagerActorFuture(attr(*args, **kwargs))
            else:
                return attr

        attr = getattr(self._cls, key)

        if callable(attr):

            @functools.wraps(attr)
            def func(*args, **kwargs):
                async def run_actor_function_on_worker():
                    try:
                        result = await self._worker_rpc.actor_execute(
                            function=key,
                            actor=self.key,
                            args=[to_serialize(arg) for arg in args],
                            kwargs={k: to_serialize(v) for k, v in kwargs.items()},
                        )
                    except OSError:
                        if self._future and not self._future.done():
                            await self._future
                            return await run_actor_function_on_worker()
                        else:
                            exc = OSError("Unable to contact Actor's worker")
                            return _Error(exc)
                    if result["status"] == "OK":
                        return _OK(result["result"])
                    return _Error(result["exception"])

                actor_future = ActorFuture(io_loop=self._io_loop)

                async def wait_then_set_result():
                    actor_future._set_result(await run_actor_function_on_worker())

                self._io_loop.add_callback(wait_then_set_result)
                return actor_future

            return func

        else:

            async def get_actor_attribute_from_worker():
                x = await self._worker_rpc.actor_attribute(
                    attribute=key, actor=self.key
                )
                if x["status"] == "OK":
                    return x["result"]
                else:
                    raise x["exception"]

            return self._sync(get_actor_attribute_from_worker)

    @property
    def client(self):
        return self._future.client


class ProxyRPC:
    """
    An rpc-like object that uses the scheduler's rpc to connect to a worker
    """

    def __init__(self, rpc, address):
        self.rpc = rpc
        self._address = address

    def __getattr__(self, key):
        async def func(**msg):
            msg["op"] = key
            result = await self.rpc.proxy(worker=self._address, msg=msg)
            return result

        return func


class BaseActorFuture(abc.ABC, Awaitable[_T]):
    """Future to an actor's method call

    Whenever you call a method on an Actor you get a BaseActorFuture immediately
    while the computation happens in the background.  You can call ``.result``
    to block and collect the full result

    See Also
    --------
    Actor
    """

    @abc.abstractmethod
    def result(self, timeout: str | timedelta | float | None = None) -> _T: ...

    @abc.abstractmethod
    def done(self) -> bool: ...

    def __repr__(self) -> Literal["<ActorFuture>"]:
        return "<ActorFuture>"


@dataclass(frozen=True, eq=False)
class EagerActorFuture(BaseActorFuture[_T]):
    """Future to an actor's method call when an actor calls another actor on the same worker"""

    _result: _T

    def __await__(self) -> Generator[object, None, _T]:
        return self._result
        yield  # type: ignore[unreachable]

    def result(self, timeout: object = None) -> _T:
        return self._result

    def done(self) -> Literal[True]:
        return True


@dataclass(frozen=True, eq=False)
class _OK(Generic[_T]):
    _v: _T

    def unwrap(self) -> _T:
        return self._v


@dataclass(frozen=True, eq=False)
class _Error:
    _e: Exception

    def unwrap(self) -> NoReturn:
        raise self._e


class ActorFuture(BaseActorFuture[_T]):
    def __init__(self, io_loop: IOLoop):
        self._io_loop = io_loop
        self._event = LateLoopEvent()
        self._out: _Error | _OK[_T] | None = None

    def __await__(self) -> Generator[object, None, _T]:
        return self._result().__await__()

    def done(self) -> bool:
        return self._event.is_set()

    async def _result(self) -> _T:
        await self._event.wait()
        out = self._out
        assert out is not None
        return out.unwrap()

    def _set_result(self, out: _Error | _OK[_T]) -> None:
        self._out = out
        self._event.set()

    def result(self, timeout: str | timedelta | float | None = None) -> _T:
        return sync(self._io_loop, self._result, callback_timeout=timeout)
