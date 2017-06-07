"""Experimental interface for asyncio, may disappear without warning"""

# flake8: noqa

import asyncio
from functools import wraps

from toolz import merge

from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.platform.asyncio import to_asyncio_future

from . import client
from .client import Client, Future
from .utils import ignoring


def to_asyncio(fn, **default_kwargs):
    """Converts Tornado gen.coroutines and futures to asyncio ones"""
    @wraps(fn)
    def convert(*args, **kwargs):
        if default_kwargs:
            kwargs = merge(default_kwargs, kwargs)
        return to_asyncio_future(fn(*args, **kwargs))
    return convert


class AioFuture(Future):
    """Provides awaitable syntax for a distributed Future"""

    def __await__(self):
        return self.result().__await__()

    result = to_asyncio(Future._result)
    exception = to_asyncio(Future._exception)
    traceback = to_asyncio(Future._traceback)


class AioClient(Client):
    """ Connect to and drive computation on a distributed Dask cluster

    This class provides an asyncio compatible async/await interface for
    dask.distributed.

    The Client connects users to a dask.distributed compute cluster. It
    provides an asynchronous user interface around functions and futures.
    This class resembles executors in ``concurrent.futures`` but also
    allows ``Future`` objects within ``submit/map`` calls.

    AioClient is an **experimental** interface for distributed and may
    disappear without warning!

    Parameters
    ----------
    address: string, or Cluster
        This can be the address of a ``Scheduler`` server like a string
        ``'127.0.0.1:8786'`` or a cluster object like ``LocalCluster()``

    Examples
    --------
    Provide cluster's scheduler address on initialization::

        client = AioClient('127.0.0.1:8786')

    Start the client::

        async def start_the_client():
            client = await AioClient()

            # Use the client....

            await client.shutdown()

    An ``async with`` statement is a more convenient way to start and shut down
    the client::

        async def start_the_client():
            async with AioClient() as client:
                # Use the client within this block.
                pass

    Use the ``submit`` method to send individual computations to the cluster,
    and await the returned future to retrieve the result::

        async def add_two_numbers():
            async with AioClient() as client:
                a = client.submit(add, 1, 2)
                result = await a

    Continue using submit or map on results to build up larger computations,
    and gather results with the ``gather`` method::

        async def gather_some_results():
            async with AioClient() as client:
                a = client.submit(add, 1, 2)
                b = client.submit(add, 10, 20)
                c = client.submit(add, a, b)
                result = await client.gather([c])

    See Also
    --------
    distributed.client.Client: Blocking Client
    distributed.scheduler.Scheduler: Internal scheduler
    """
    _Future = AioFuture

    def __init__(self, *args, **kwargs):
        if kwargs.get('set_as_default'):
            raise Exception("AioClient instance can't be set as default")

        loop = asyncio.get_event_loop()
        ioloop = BaseAsyncIOLoop(loop)
        super().__init__(*args, loop=ioloop, set_as_default=False, asynchronous=True, **kwargs)

    async def __aenter__(self):
        await to_asyncio_future(self._started)
        return self

    async def __aexit__(self, type, value, traceback):
        await self.shutdown()

    def __await__(self):
        return to_asyncio_future(self._started).__await__()

    async def shutdown(self, fast=False):
        if self.status == 'closed':
            return

        try:
            future = self._shutdown(fast=fast)
            await to_asyncio_future(future)

            with ignoring(AttributeError):
                future = self.cluster._close()
                await to_asyncio_future(future)
                self.cluster.status = 'closed'
        finally:
            BaseAsyncIOLoop.clear_current()

    def __del__(self):
        # Override Client.__del__ to avoid running self.shutdown()
        assert self.status != 'running'

    gather = to_asyncio(Client._gather)
    scatter = to_asyncio(Client._scatter)
    cancel = to_asyncio(Client._cancel)
    publish_dataset = to_asyncio(Client._publish_dataset)
    get_dataset = to_asyncio(Client._get_dataset)
    run_on_scheduler = to_asyncio(Client._run_on_scheduler)
    run = to_asyncio(Client._run)
    run_coroutine = to_asyncio(Client._run_coroutine)
    get = to_asyncio(Client.get, sync=False)
    upload_environment = to_asyncio(Client._upload_environment)
    restart = to_asyncio(Client._restart)
    upload_file = to_asyncio(Client._upload_file)
    upload_large_file = to_asyncio(Client._upload_large_file)
    rebalance = to_asyncio(Client._rebalance)
    replicate = to_asyncio(Client._replicate)
    start_ipython_workers = to_asyncio(Client._start_ipython_workers)

    def __enter__(self):
        raise RuntimeError("Use AioClient in an 'async with' block, not 'with'")


class as_completed(client.as_completed):
    __anext__ = to_asyncio(client.as_completed.__anext__)


wait = to_asyncio(client._wait)
