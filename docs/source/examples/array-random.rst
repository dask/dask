Creating random arrays
======================

In a simple case, we can create arrays with random data using the ``da.random``
module.

.. code-block:: python

    >>> import dask.array as da
    >>> x = da.random.normal(0, 1, size=(100000,100000), chunks=(1000, 1000))
    >>> x.mean().compute()
    -0.0002280808453825202
