from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict

from dask.utils import parse_timedelta

from distributed.client import Future
from distributed.utils import Deadline, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class QueueExtension:
    """An extension for the scheduler to manage queues

    This adds the following routes to the scheduler

    *  queue_create
    *  queue_release
    *  queue_put
    *  queue_get
    *  queue_size
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.queues = dict()
        self.client_refcount = dict()
        self.future_refcount = defaultdict(int)

        self.scheduler.handlers.update(
            {
                "queue_create": self.create,
                "queue_put": self.put,
                "queue_get": self.get,
                "queue_qsize": self.qsize,
            }
        )

        self.scheduler.stream_handlers.update(
            {"queue-future-release": self.future_release, "queue_release": self.release}
        )

    def create(self, name=None, client=None, maxsize=0):
        logger.debug(f"Queue name: {name}")
        if name not in self.queues:
            self.queues[name] = asyncio.Queue(maxsize=maxsize)
            self.client_refcount[name] = 1
        else:
            self.client_refcount[name] += 1

    def release(self, name=None, client=None):
        if name not in self.queues:
            return

        self.client_refcount[name] -= 1
        if self.client_refcount[name] == 0:
            del self.client_refcount[name]
            futures = self.queues[name]._queue
            del self.queues[name]
            keys = [d["value"] for d in futures if d["type"] == "Future"]
            if keys:
                self.scheduler.client_releases_keys(keys=keys, client="queue-%s" % name)

    async def put(self, name=None, key=None, data=None, client=None, timeout=None):
        deadline = Deadline.after(timeout)
        if key is not None:
            while key not in self.scheduler.tasks:
                await asyncio.sleep(0.01)
                if deadline.expired:
                    raise TimeoutError(f"Task {key} unknown to scheduler.")

            record = {"type": "Future", "value": key}
            self.future_refcount[name, key] += 1
            self.scheduler.client_desires_keys(keys=[key], client="queue-%s" % name)
        else:
            record = {"type": "msgpack", "value": data}
        await wait_for(self.queues[name].put(record), timeout=deadline.remaining)

    def future_release(self, name=None, key=None, client=None):
        self.scheduler.client_desires_keys(keys=[key], client=client)
        self.future_refcount[name, key] -= 1
        if self.future_refcount[name, key] == 0:
            self.scheduler.client_releases_keys(keys=[key], client="queue-%s" % name)
            del self.future_refcount[name, key]

    async def get(self, name=None, client=None, timeout=None, batch=False):
        def process(record):
            """Add task status if known"""
            if record["type"] == "Future":
                record = record.copy()
                key = record["value"]
                ts = self.scheduler.tasks.get(key)
                state = ts.state if ts is not None else "lost"

                record["state"] = state
                if state == "erred":
                    record["exception"] = ts.exception_blame.exception
                    record["traceback"] = ts.exception_blame.traceback

            return record

        if batch:
            q = self.queues[name]
            out = []
            if batch is True:
                while not q.empty():
                    record = await q.get()
                    out.append(record)
            else:
                if timeout is not None:
                    msg = (
                        "Dask queues don't support simultaneous use of "
                        "integer batch sizes and timeouts"
                    )
                    raise NotImplementedError(msg)
                for _ in range(batch):
                    record = await q.get()
                    out.append(record)
            out = [process(o) for o in out]
            return out
        else:
            record = await wait_for(self.queues[name].get(), timeout=timeout)
            record = process(record)
            return record

    def qsize(self, name=None, client=None):
        return self.queues[name].qsize()


class Queue:
    """Distributed Queue

    This allows multiple clients to share futures or small bits of data between
    each other with a multi-producer/multi-consumer queue.  All metadata is
    sequentialized through the scheduler.

    Elements of the Queue must be either Futures or msgpack-encodable data
    (ints, strings, lists, dicts).  All data is sent through the scheduler so
    it is wise not to send large objects.  To share large objects scatter the
    data and share the future instead.

    .. warning::

       This object is experimental

    Parameters
    ----------
    name: string (optional)
        Name used by other clients and the scheduler to identify the queue. If
        not given, a random name will be generated.
    client: Client (optional)
        Client used for communication with the scheduler.
        If not given, the default global client will be used.
    maxsize: int (optional)
        Number of items allowed in the queue. If 0 (the default), the queue
        size is unbounded.

    Examples
    --------
    >>> from dask.distributed import Client, Queue  # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> queue = Queue('x')  # doctest: +SKIP
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> queue.put(future)  # doctest: +SKIP

    See Also
    --------
    Variable: shared variable between clients
    """

    def __init__(self, name=None, client=None, maxsize=0):
        self._client = client
        self.name = name or "queue-" + uuid.uuid4().hex
        self.maxsize = maxsize
        self._maybe_start()

    def _maybe_start(self):
        if self.client:
            if self.client.asynchronous:
                self._started = asyncio.ensure_future(self._start())
            else:
                self.client.sync(self._start)

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

    async def _start(self):
        await self.client.scheduler.queue_create(name=self.name, maxsize=self.maxsize)
        return self

    def __await__(self):
        self._maybe_start()
        if hasattr(self, "_started"):
            return self._started.__await__()
        else:

            async def _():
                return self

            return _().__await__()

    async def _put(self, value, timeout=None):
        if isinstance(value, Future):
            await self.client.scheduler.queue_put(
                key=value.key, timeout=timeout, name=self.name
            )
        else:
            await self.client.scheduler.queue_put(
                data=value, timeout=timeout, name=self.name
            )

    def put(self, value, timeout=None, **kwargs):
        """Put data into the queue

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Time in seconds to wait before timing out.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._put, value, timeout=timeout, **kwargs)

    def get(self, timeout=None, batch=False, **kwargs):
        """Get data from the queue

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Time in seconds to wait before timing out.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        batch : boolean, int (optional)
            If True then return all elements currently waiting in the queue.
            If an integer than return that many elements from the queue
            If False (default) then return one item at a time
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._get, timeout=timeout, batch=batch, **kwargs)

    def qsize(self, **kwargs):
        """Current number of elements in the queue"""
        self._verify_running()
        return self.client.sync(self._qsize, **kwargs)

    async def _get(self, timeout=None, batch=False):
        resp = await self.client.scheduler.queue_get(
            timeout=timeout, name=self.name, batch=batch
        )

        def process(d):
            if d["type"] == "Future":
                value = Future(d["value"], self.client, state=d["state"])
                if d["state"] == "erred":
                    value._state.set_error(d["exception"], d["traceback"])
                self.client._send_to_scheduler(
                    {"op": "queue-future-release", "name": self.name, "key": d["value"]}
                )
            else:
                value = d["value"]

            return value

        if batch is False:
            result = process(resp)
        else:
            result = list(map(process, resp))

        return result

    async def _qsize(self):
        result = await self.client.scheduler.queue_qsize(name=self.name)
        return result

    def close(self):
        self._verify_running()
        if self.client.status == "running":  # TODO: can leave zombie futures
            self.client._send_to_scheduler({"op": "queue_release", "name": self.name})

    def __reduce__(self):
        return type(self), (self.name, None, self.maxsize)
