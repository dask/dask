from collections.abc import MutableMapping

from .utils import log_errors, tokey


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
        }

        self.scheduler.handlers.update(handlers)
        self.scheduler.extensions["publish"] = self

    def put(
        self, comm=None, keys=None, data=None, name=None, override=False, client=None
    ):
        with log_errors():
            if not override and name in self.datasets:
                raise KeyError("Dataset %s already exists" % name)
            self.scheduler.client_desires_keys(keys, "published-%s" % tokey(name))
            self.datasets[name] = {"data": data, "keys": keys}
            return {"status": "OK", "name": name}

    def delete(self, comm=None, name=None):
        with log_errors():
            out = self.datasets.pop(name, {"keys": []})
            self.scheduler.client_releases_keys(
                out["keys"], "published-%s" % tokey(name)
            )

    def list(self, *args):
        with log_errors():
            return list(sorted(self.datasets.keys(), key=str))

    def get(self, stream, name=None, client=None):
        with log_errors():
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
        for key in self._client.list_datasets():
            yield key

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
