LinearOperator
==============

Dask Array implements the SciPy LinearOperator_ interface and it can be used
with any SciPy algorithm depending on that interface.

Example
-------

.. code-block:: python

   import dask.array as da
   x = da.random.random(size=(10000, 10000), chunks=(1000, 1000))

   from scipy.sparse.linalg.interface import MatrixLinearOperator
   A = MatrixLinearOperator(x)

   import numpy as np
   b = np.random.random(10000)

   from scipy.sparse.linalg import gmres
   x = gmres(A, b)

*Disclaimer: This is just a toy example and not necessarily the best way to
solve this problem for this data.*


.. _LinearOperator: https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.linalg.LinearOperator.html
