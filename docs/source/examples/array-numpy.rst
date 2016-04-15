Creating Dask arrays from NumPy arrays
======================================

We can create Dask arrays from NumPy arrays using ``da.from_array``.

.. code-block:: python

    >>> import numpy as np
    >>> import dask.array as da
    >>> x = np.arange(1000)
    >>> y = da.from_array(x, chunks=(100))
    >>> y.mean().compute()
    499.5
