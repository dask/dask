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


.. _`inspect docs`: inspect.html
