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

        my_array.compute(get=dask.get)

    ``dask.async.get_sync`` is still fairly fast though doesn't provide as deep
    of introspection.  ``dask.get`` provides full introspection with tools like
    ``pdb`` but does not cache data well.


.. _`inspect docs`: inspect.html
