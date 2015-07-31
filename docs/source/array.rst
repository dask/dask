Array
=====

Dask Array implements a subset of the NumPy ``ndarray`` interface using blocked
algorithms, cutting up the large array into many small arrays.  This lets us
compute on arrays larger than memory using all of our cores.  We coordinate
these blocked algorithms using ``dask`` graphs.

.. toctree::
   :maxdepth: 1

   array-overview.rst
   array-design.rst
   array-creation.rst
   array-api.rst
   slicing.rst
   stack.rst
   random.rst
   ghost.rst
   array-blaze.rst
   array-extend.rst
