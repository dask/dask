Frequently Asked Questions
==========================

1.  Q: How do I debug my program when using dask?

    If you want to inspect the dask graph itself see `inspect docs`_

    If you want to dive down with a Python debugger a common cause of
    frustration is the asynchronous schedulers which, because they run your
    code on different workers, are unable to provide access to the Python
    debugger.  Fortunately you can change to a synchronous scheduler like
    ``dask.get`` or ``dask.async.get_sync`` by providing a ``get=`` keyword
    to the ``compute`` method::

        my_array.compute(get=dask.async.get_sync)

    Both ``dask.async.get_sync`` and ``dask.get`` will provide traceback
    traversals.  ``dask.async.get_sync`` uses the same machinery of the async
    schedulers but with only one worker.  ``dask.get`` is dead-simple but does
    not cache data and so can be slow for some workloads.

2.  Q: What is ``chunks``

    Dask.array breaks your large array into lots of little pieces, each of
    which can fit in memory.  ``chunks`` determines the size of those pieces.

    Users most often interact with chunks when they create an array as in::

        >>> x = da.from_array(dataset, chunks=(1000, 1000))

    In this case chunks is a tuple defining the shape of each chunk of your
    array; e.g. "Please break ``dataset`` into 1000 by 1000 chunks."

    However internally dask uses a different representation (a tuple of tuples)
    to handle uneven chunk sizes that inevitably occur during computation.

3.  Q: How do I select a good value for ``chunks``?

    Choosing good values for ``chunks`` can strongly impact performance.
    Here are some general guidelines.  The strongest guide is memory:

    1.  The size of your blocks should fit in memory.
    2.  Actually, several blocks should fit in memory at once (assuming you
        want multi-core)
    3.  The size of the blocks should be large enough to hide scheduling
        overhead, which is a couple of milliseconds per task
    4.  Generally I shoot for 10MB-100MB sized chunks

    Additionally the computations you do may also inform your choice of
    ``chunks``.  Some operations like matrix multiply require anti-symmetric
    chunk shapes.  Others like ``svd`` and ``qr`` only work on tall-and-skinny
    matrices with only a single chunk along all of the columns.  Other
    operations might work but be faster or slower with different chunk shapes.

    Note that you can ``rechunk()`` an array if necessary.

.. _`inspect docs`: inspect.html
