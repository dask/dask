Frequently Asked Questions
==========================

We maintain most Q&A on `Stack Overflow under the #Dask tag`_.  You may find
the questions there useful to you.

.. _`Stack Overflow under the #Dask tag`: http://stackoverflow.com/questions/tagged/dask

1.  **Q: How do I debug my program when using dask?**

    If you want to inspect the dask graph itself see
    :doc:`inspect docs <inspect>`.

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


2.  **Q: In ``dask.array`` what is ``chunks``?**

    Dask.array breaks your large array into lots of little pieces, each of
    which can fit in memory.  ``chunks`` determines the size of those pieces.

    Users most often interact with chunks when they create an array as in::

        >>> x = da.from_array(dataset, chunks=(1000, 1000))

    In this case chunks is a tuple defining the shape of each chunk of your
    array; for example "Please break ``dataset`` into 1000 by 1000 chunks."

    However internally dask uses a different representation, a tuple of tuples,
    to handle uneven chunk sizes that inevitably occur during computation.


3.  **Q: How do I select a good value for ``chunks``?**

    Choosing good values for ``chunks`` can strongly impact performance.
    Here are some general guidelines.  The strongest guide is memory:

    1.  The size of your blocks should fit in memory.
    2.  Actually, several blocks should fit in memory at once, assuming you
        want multi-core
    3.  The size of the blocks should be large enough to hide scheduling
        overhead, which is a couple of milliseconds per task
    4.  Generally I shoot for 10MB-100MB sized chunks

    Additionally the computations you do may also inform your choice of
    ``chunks``.  Some operations like matrix multiply require anti-symmetric
    chunk shapes.  Others like ``svd`` and ``qr`` only work on tall-and-skinny
    matrices with only a single chunk along all of the columns.  Other
    operations might work but be faster or slower with different chunk shapes.

    Note that you can ``rechunk()`` an array if necessary.


4.  **Q: My computation fills memory, how do I spill to disk?**

    The schedulers endeavor not to use up all of your memory.  However for some
    algorithms filling up memory is unavoidable.  In these cases we can swap
    out the dictionary used to store intermediate results with a
    dictionary-like object that spills to disk.  The Chest_ project handles
    this nicely.

        >>> cache = Chest() # Uses temporary file. Deletes on garbage collection

    or

        >>> cache = Chest(path='/path/to/dir', available_memory=8e9)  # Use 8GB

    This chest object works just like a normal dictionary but, when available
    memory runs out (defaults to 1GB) it starts pickling data and sending it to
    disk, retrieving it as necessary.

    You can specify your cache when calling ``compute``

        >>> x.dot(x.T).compute(cache=cache)

    Alternatively you can set your cache as a global option.

        >>> with dask.set_options(cache=cache):  # sets state within with block
        ...     y = x.dot(x.T).compute()

    or

        >>> dask.set_options(cache=cache)  # sets global state
        >>> y = x.dot(x.T).compute()

    However, while using an on-disk cache is a great fallback performance, it's
    always best if we can keep from spilling to disk.  You could try one of the
    following

    1.  Use a smaller chunk/partition size
    2.  If you are convinced that a smaller chunk size will not help in your
        case you could also report your problem on our `issue tracker`_ and
        work with the dask development team to improve our scheduling policies.

5.  **How does Dask serialize functions?**

    When operating with the single threaded or multithreaded scheduler no
    function serialization is necessary.  When operating with the distributed
    memory or multiprocessing scheduler Dask uses cloudpickle_ to serialize
    functions to send to worker processes.  cloudpickle supports almost any
    kind of function, including lambdas, closures, partials and functions
    defined interactively.

    Cloudpickle can not serialize things like iterators, open files, locks, or
    other objects that are heavily tied to your current process.  Attempts to
    serialize these objects (or functions that implicitly rely on these
    objects) will result in scheduler errors.  You can verify that your objects
    are easily serializable by running them through the
    ``cloudpickle.dumps/loads`` functions

    .. code-block:: python

       from cloudpickle import dumps, loads
       obj2 = loads(dumps(obj))
       assert obj2 == obj

.. _cloudpickle: https://github.com/cloudpipe/cloudpickle
.. _`Chest`: https://github.com/blaze/chest
.. _`issue tracker`: https://github.com/dask/dask/issues/new
