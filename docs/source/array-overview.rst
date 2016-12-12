Overview
========

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

Today Dask array is commonly used in the sort of gridded data analysis that
arises in weather, climate modeling, or oceanography, especially when data
sizes become inconveniently large.  Dask array complements large on-disk array
stores like HDF5, NetCDF, and BColz.  Additionally Dask array is commonly used
to speed up expensive in-memory computations using multiple cores, such as you
might find in image analysis or statistical and machine learning applications.

Scope
-----

The ``dask.array`` library supports the following interface from ``numpy``:

*  Arithmetic and scalar mathematics, ``+, *, exp, log, ...``
*  Reductions along axes, ``sum(), mean(), std(), sum(axis=0), ...``
*  Tensor contractions / dot products / matrix multiply, ``tensordot``
*  Axis reordering / transpose, ``transpose``
*  Slicing, ``x[:100, 500:100:-2]``
*  Fancy indexing along single axes with lists or numpy arrays, ``x[:, [10, 1, 5]]``
*  The array protocol ``__array__``
*  Some linear algebra ``svd, qr, solve, solve_triangular, lstsq``

See :doc:`the dask.array API<array-api>` for a more extensive list of
functionality.

Execution
---------

By default Dask array uses the threaded scheduler in order to avoid data
transfer costs and because NumPy releases the GIL well.  It is also quite
effective on a cluster using the `dask.distributed`_ scheduler.

.. _`dask.distributed`: https://distributed.readthedocs.io/en/latest/

Limitations
-----------

Dask array does not implement the entire numpy interface.  Users expecting this
will be disappointed.  Notably, Dask array has the following limitations:

1.  Dask array does not implement all of ``np.linalg``.  This has been done by a
    number of excellent BLAS/LAPACK implementations, and is the focus of
    numerous ongoing academic research projects.
2.  Dask array with unknown shapes do not support all operations
3.  Dask array does not attempt operations like ``sort`` which are notoriously
    difficult to do in parallel, and are of somewhat diminished value on very
    large data (you rarely actually need a full sort).
    Often we include parallel-friendly alternatives like ``topk``.
4.  It is very inefficient to iterate over a Dask array with for loops.
5.  Dask development is driven by immediate need, and so many lesser used
    functions have not been implemented. Community contributions are encouraged.
