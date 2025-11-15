from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import MutableMapping

from dask.utils import stringify

from distributed.utils import log_errors


class PublishExtension:
    """An extension for the scheduler to manage collections

    *  publish_list
    *  publish_put
    *  publish_get
    *  publish_delete
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.datasets = dict()

        handlers = {
            "publish_list": self.list,
            "publish_put": self.put,
            "publish_get": self.get,
            "publish_delete": self.delete,
            "publish_wait_flush": self.flush_wait,
        }
        stream_handlers = {
            "publish_flush_batched_send": self.flush_receive,
        }

        self.scheduler.handlers.update(handlers)
        self.scheduler.stream_handlers.update(stream_handlers)
        self._flush_received = defaultdict(asyncio.Event)

    def flush_receive(self, uid, **kwargs):
        self._flush_received[uid].set()

    async def flush_wait(self, uid):
        await self._flush_received[uid].wait()

    @log_errors
    def put(self, keys=None, data=None, name=None, override=False, client=None):
        if not override and name in self.datasets:
            raise KeyError("Dataset %s already exists" % name)
        self.scheduler.client_desires_keys(keys, f"published-{stringify(name)}")
        self.datasets[name] = {"data": data, "keys": keys}
        return {"status": "OK", "name": name}

    @log_errors
    def delete(self, name=None):
        out = self.datasets.pop(name, {"keys": []})
        self.scheduler.client_releases_keys(out["keys"], f"published-{stringify(name)}")

    @log_errors
    def list(self, *args):
        return list(sorted(self.datasets.keys(), key=str))

    @log_errors
    def get(self, name=None, client=None):
        return self.datasets.get(name, None)


class Datasets(MutableMapping):
    """A dict-like wrapper around :class:`Client` dataset methods.

    Parameters
    ----------
    client : distributed.client.Client

    """

    __slots__ = ("_client",)

    def __init__(self, client):
        self._client = client

    def __getitem__(self, key):
        # When client is asynchronous, it returns a coroutine
        return self._client.get_dataset(key)

    def __setitem__(self, key, value):
        if self._client.asynchronous:
            # 'await obj[key] = value' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'client.datasets[name] = value' when client is "
                "asynchronous; please use 'client.publish_dataset(name=value)' instead"
            )
        self._client.publish_dataset(value, name=key)

    def __delitem__(self, key):
        if self._client.asynchronous:
            # 'await del obj[key]' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'del client.datasets[name]' when client is asynchronous; "
                "please use 'client.unpublish_dataset(name)' instead"
            )
        return self._client.unpublish_dataset(key)

    def __iter__(self):
        if self._client.asynchronous:
            raise TypeError(
                "Can't invoke iter() or 'for' on client.datasets when client is "
                "asynchronous; use 'async for' instead"
            )
        yield from self._client.list_datasets()

    def __aiter__(self):
        if not self._client.asynchronous:
            raise TypeError(
                "Can't invoke 'async for' on client.datasets when client is "
                "synchronous; use iter() or 'for' instead"
            )

        async def _():
            for key in await self._client.list_datasets():
                yield key

        return _()

    def __len__(self):
        if self._client.asynchronous:
            # 'await len(obj)' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'len(client.datasets)' when client is asynchronous; "
                "please use 'len(await client.list_datasets())' instead"
            )
        return len(self._client.list_datasets())
