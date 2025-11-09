from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from contextlib import suppress

from tlz import merge

from dask.utils import parse_timedelta

from distributed.client import Future
from distributed.metrics import time
from distributed.utils import Deadline, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class VariableExtension:
    """An extension for the scheduler to manage Variables

    This adds the following routes to the scheduler

    *  variable-set
    *  variable-get
    *  variable-delete
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.variables = dict()
        self.waiting = defaultdict(set)
        self.waiting_conditions = defaultdict(asyncio.Condition)
        self.started = asyncio.Condition()

        self.scheduler.handlers.update(
            {"variable_set": self.set, "variable_get": self.get}
        )

        self.scheduler.stream_handlers["variable-future-received-confirm"] = (
            self.future_received_confirm
        )
        self.scheduler.stream_handlers["variable_delete"] = self.delete

    async def set(self, name=None, key=None, data=None, client=None, timeout=None):
        deadline = Deadline.after(parse_timedelta(timeout))
        if key is not None:
            record = {"type": "Future", "value": key}
            while key not in self.scheduler.tasks:
                await asyncio.sleep(0.01)
                if deadline.expired:
                    raise TimeoutError(f"Task {key} unknown to scheduler.")
            self.scheduler.client_desires_keys(keys=[key], client="variable-%s" % name)
        else:
            record = {"type": "msgpack", "value": data}
        try:
            old = self.variables[name]
        except KeyError:
            pass
        else:
            if old["type"] == "Future" and old["value"] != key:
                asyncio.ensure_future(self.release(old["value"], name))
        if name not in self.variables:
            async with self.started:
                self.started.notify_all()
        self.variables[name] = record

    async def release(self, key, name):
        while self.waiting[key, name]:
            async with self.waiting_conditions[name]:
                await self.waiting_conditions[name].wait()

        self.scheduler.client_releases_keys(keys=[key], client="variable-%s" % name)
        del self.waiting[key, name]

    async def future_received_confirm(
        self, name=None, key=None, token=None, client=None
    ):
        self.scheduler.client_desires_keys([key], client)
        self.waiting[key, name].remove(token)
        if not self.waiting[key, name]:
            async with self.waiting_conditions[name]:
                self.waiting_conditions[name].notify_all()

    async def get(self, name=None, client=None, timeout=None):
        start = time()
        while name not in self.variables:
            if timeout is not None:
                left = timeout - (time() - start)
            else:
                left = None
            if left and left < 0:
                raise TimeoutError()
            try:

                async def _():  # Python 3.6 is odd and requires special help here
                    await self.started.acquire()
                    await self.started.wait()

                await wait_for(_(), timeout=left)
            finally:
                self.started.release()

        record = self.variables[name]
        if record["type"] == "Future":
            key = record["value"]
            token = uuid.uuid4().hex
            ts = self.scheduler.tasks.get(key)
            state = ts.state if ts is not None else "lost"
            msg = {"token": token, "state": state}
            if state == "erred":
                msg["exception"] = ts.exception_blame.exception
                msg["traceback"] = ts.exception_blame.traceback
            record = merge(record, msg)
            self.waiting[key, name].add(token)
        return record

    @log_errors
    async def delete(self, name=None, client=None):
        try:
            old = self.variables[name]
        except KeyError:
            pass
        else:
            if old["type"] == "Future":
                await self.release(old["value"], name)
        with suppress(KeyError):
            del self.waiting_conditions[name]
        with suppress(KeyError):
            del self.variables[name]

        self.scheduler.remove_client("variable-%s" % name)


class Variable:
    """Distributed Global Variable

    This allows multiple clients to share futures and data between each other
    with a single mutable variable.  All metadata is sequentialized through the
    scheduler.  Race conditions can occur.

    Values must be either Futures or msgpack-encodable data (ints, lists,
    strings, etc..)  All data will be kept and sent through the scheduler, so
    it is wise not to send too much.  If you want to share a large amount of
    data then ``scatter`` it and share the future instead.

    Parameters
    ----------
    name: string (optional)
        Name used by other clients and the scheduler to identify the variable.
        If not given, a random name will be generated.
    client: Client (optional)
        Client used for communication with the scheduler.
        If not given, the default global client will be used.

    Examples
    --------
    >>> from dask.distributed import Client, Variable # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> x = Variable('x')  # doctest: +SKIP
    >>> x.set(123)  # docttest: +SKIP
    >>> x.get()  # docttest: +SKIP
    123
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> x.set(future)  # doctest: +SKIP

    See Also
    --------
    Queue: shared multi-producer/multi-consumer queue between clients
    """

    def __init__(self, name=None, client=None):
        self._client = client
        self.name = name or "variable-" + uuid.uuid4().hex

    @property
    def client(self):
        if not self._client:
            try:
                self._client = get_client()
            except ValueError:
                pass
        return self._client

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(
                f"{type(self)} object not properly initialized. This can happen"
                " if the object is being deserialized outside of the context of"
                " a Client or Worker."
            )

    async def _set(self, value, timeout):
        if isinstance(value, Future):
            await self.client.scheduler.variable_set(
                key=value.key, name=self.name, timeout=timeout
            )
        else:
            await self.client.scheduler.variable_set(
                data=value, name=self.name, timeout=timeout
            )

    def set(self, value, timeout="30 s", **kwargs):
        """Set the value of this variable

        Parameters
        ----------
        value : Future or object
            Must be either a Future or a msgpack-encodable value
        """
        self._verify_running()
        return self.client.sync(self._set, value, timeout=timeout, **kwargs)

    async def _get(self, timeout=None):
        d = await self.client.scheduler.variable_get(
            timeout=timeout, name=self.name, client=self.client.id
        )
        if d["type"] == "Future":
            value = Future(d["value"], self.client, state=d["state"])
            if d["state"] == "erred":
                value._state.set_error(d["exception"], d["traceback"])
            self.client._send_to_scheduler(
                {
                    "op": "variable-future-received-confirm",
                    "name": self.name,
                    "key": d["value"],
                    "token": d["token"],
                }
            )
        else:
            value = d["value"]
        return value

    def get(self, timeout=None, **kwargs):
        """Get the value of this variable

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Time in seconds to wait before timing out.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._get, timeout=timeout, **kwargs)

    def delete(self):
        """Delete this variable

        Caution, this affects all clients currently pointing to this variable.
        """
        self._verify_running()
        if self.client.status == "running":  # TODO: can leave zombie futures
            self.client._send_to_scheduler({"op": "variable_delete", "name": self.name})

    def __reduce__(self):
        return Variable, (self.name,)
