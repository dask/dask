from .client import Client


class TornadoClient(Client):
    """ An Asynchronous Dask Client that uses Tornado coroutines

    This copies the normal interface of the dask.distributed Client but
    operates asynchronously within a Tornado IOLoop.

    Examples
    --------
    >>> @tornado.gen.coroutine  # doctest: +SKIP
    ... def f():
    ...     c = yield TornadoClient('scheduler:8786')
    ...     futures = yield c.scatter([1, 2, 3])
    ...     future = c.submit(sum, futures)
    ...     result = yield c.gather(future)

    >>> IOLoop.current().add_callback(f)  # doctest: +SKIP

    See Also
    --------
    distributed.Client: Synchronous version
    distributed.AioClient: AsyncIO Client
    """
    def __init__(self, *args, **kwargs):
        kwargs['asynchronous'] = True
        super(TornadoClient, self).__init__(*args, **kwargs)

    gather = Client._gather
    get_dataset = Client._get_dataset
    publish_dataset = Client._publish_dataset
    replicate = Client._replicate
    rebalance = Client._rebalance
    restart = Client._restart
    run = Client._run
    run_coroutine = Client._run_coroutine
    run_on_scheduler = Client._run_on_scheduler
    scatter = Client._scatter
    shutdown = Client._shutdown
    start_ipython_workers = Client._start_ipython_workers
    upload_file = Client._upload_file
