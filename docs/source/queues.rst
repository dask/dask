Data Streams with Queues
========================

This feature is no longer supported.
Instead people may want to look at the following options:

1.  Use normal for loops with Client.submit/gather and as_completed
2.  Use :doc:`asynchronous async/await <asynchronous>` code and a few coroutines
3.  Try out the `Streamz <https://streamz.readthedocs.io>`_ project,
    which has Dask support
