Array
=====

.. toctree::
   :maxdepth: 1
   :hidden:

   array-api.rst
   array-creation.rst
   array-ghost.rst
   array-design.rst
   array-sparse.rst
   array-stats.rst
   array-linear-operator.rst
   array-slicing.rst
   array-stack.rst
   array-gufunc.rst

Dask Array implements a subset of the NumPy ndarray interface using blocked
algorithms, cutting up the large array into many small arrays. This lets us
compute on arrays larger than memory using all of our cores.  We coordinate
these blocked algorithms using dask graphs.

Design
------

.. image:: images/dask-array-black-text.svg
   :alt: Dask arrays coordinate many numpy arrays
   :align: right

Dask arrays coordinate many NumPy arrays arranged into a grid.  These
NumPy arrays may live on disk or on other machines.

Common Uses
-----------

Dask array used in fields like atmospheric and oceanographic science, large
scale imaging, genomics, numerical algorithms for optimization or statistics ,
and more.

Scope
-----

Dask arrays supports most of the Numpy interface like the following:

-  Arithmetic and scalar mathematics, ``+, *, exp, log, ...``
-  Reductions along axes, ``sum(), mean(), std(), sum(axis=0), ...``
-  Tensor contractions / dot products / matrix multiply, ``tensordot``
-  Axis reordering / transpose, ``transpose``
-  Slicing, ``x[:100, 500:100:-2]``
-  Fancy indexing along single axes with lists or numpy arrays, ``x[:, [10, 1, 5]]``
-  Array protocols like ``__array__``, and ``__array_ufunc__``
-  Some linear algebra ``svd, qr, solve, solve_triangular, lstsq``
-  ...


However Dask array does not implement the entire numpy interface.  Users expecting this
will be disappointed.  Notably, Dask array lacks the following features:

-   Much of ``np.linalg`` has not been implemented.
    This has been done by a number of excellent BLAS/LAPACK implementations,
    and is the focus of numerous ongoing academic research projects.
-   Arrays with unknown shapes do not support all operations
-   Operations like ``sort`` which are notoriously
    difficult to do in parallel, and are of somewhat diminished value on very
    large data (you rarely actually need a full sort).
    Often we include parallel-friendly alternatives like ``topk``.
-   Dask array doesn't implement operations like ``tolist`` that would be very
    inefficient for larger datasets. Likewise it is very inefficient to iterate
    over a Dask array with for loops.
-   Dask development is driven by immediate need, and so many lesser used
    functions have not been implemented. Community contributions are encouraged.

See :doc:`the dask.array API<array-api>` for a more extensive list of
functionality.

Execution
---------

By default Dask array uses the threaded scheduler in order to avoid data
transfer costs and because NumPy releases the GIL well.  It is also quite
effective on a cluster using the `dask.distributed`_ scheduler.

.. _`dask.distributed`: https://distributed.readthedocs.io/en/latest/
