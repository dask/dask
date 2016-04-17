Creating Dask arrays from NumPy arrays
======================================

We can create Dask arrays from any object that implements NumPy slicing, like a
``numpy.ndarray`` or on-disk formats like h5py or netCDF Dataset objects. This
is particularly useful with on disk arrays that don't fit in memory but, for
simplicity's sake, we show how this works on a NumPy array.

The following example uses ``da.from_array`` to create a Dask array from a NumPy
array, which isn't particularly valuable (the NumPy array already works in
memory just fine) but is easy to play with.

.. code-block:: python

    >>> import numpy as np
    >>> import dask.array as da
    >>> x = np.arange(1000)
    >>> y = da.from_array(x, chunks=(100))
    >>> y.mean().compute()
    499.5
