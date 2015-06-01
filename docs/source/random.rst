dask.array.random
=================

``dask.array`` copies the ``numpy.random`` module for all univariate
distributions.  The interface to each function is identical except for the
addition of a new ``chunks=`` keyword argument.

.. code-block:: python

   import numpy as np
   x = np.random.normal(10, 0.1, size=(10, 10))

   import dask.array as da
   x = da.random.normal(10, 0.1, size=(10, 10), chunks=(5, 5))
