Creating random arrays
======================

We can create arrays with random variables using ``da.random.normal``.

.. code-block:: python

    >>> import dask.array as da
    >>> x = da.random.normal(0, 1, size=(100000,100000), chunks=(10000, 10000))
    >>> x.mean().compute()
    -0.0002280808453825202
