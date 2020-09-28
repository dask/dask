Array
=====

.. toctree::
   :maxdepth: 1
   :hidden:

   array-api.rst
   array-best-practices.rst
   array-chunks.rst
   array-creation.rst
   array-overlap.rst
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
these blocked algorithms using Dask graphs.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/9h_61hXCDuI"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>

Examples
--------

Visit https://examples.dask.org/array.html to see and run examples using
Dask Array.

Design
------

.. image:: images/dask-array-black-text.svg
   :alt: Dask arrays coordinate many numpy arrays
   :align: right

Dask arrays coordinate many NumPy arrays (or "duck arrays" that are
sufficiently NumPy-like in API such as CuPy or Spare arrays) arranged into a
grid. These arrays may live on disk or on other machines.

New duck array chunk types (types below Dask on
`NEP-13's type-casting heirarchy`_) can be registered via
:func:`~dask.array.register_chunk_type`. Any other duck array types that are
not registered will be deferred to in binary operations and NumPy
ufuncs/functions (that is, Dask will return ``NotImplemented``). Note, however,
that *any* ndarray-like type can be inserted into a Dask Array using
:func:`~dask.array.Array.from_array`.

Common Uses
-----------

Dask Array is used in fields like atmospheric and oceanographic science, large
scale imaging, genomics, numerical algorithms for optimization or statistics,
and more.

Scope
-----

Dask arrays support most of the NumPy interface like the following:

-  Arithmetic and scalar mathematics: ``+, *, exp, log, ...``
-  Reductions along axes: ``sum(), mean(), std(), sum(axis=0), ...``
-  Tensor contractions / dot products / matrix multiply: ``tensordot``
-  Axis reordering / transpose: ``transpose``
-  Slicing: ``x[:100, 500:100:-2]``
-  Fancy indexing along single axes with lists or NumPy arrays: ``x[:, [10, 1, 5]]``
-  Array protocols like ``__array__`` and ``__array_ufunc__``
-  Some linear algebra: ``svd, qr, solve, solve_triangular, lstsq``
-  ...


However, Dask Array does not implement the entire NumPy interface.  Users expecting this
will be disappointed.  Notably, Dask Array lacks the following features:

-   Much of ``np.linalg`` has not been implemented.
    This has been done by a number of excellent BLAS/LAPACK implementations,
    and is the focus of numerous ongoing academic research projects
-   Arrays with unknown shapes do not support all operations
-   Operations like ``sort`` which are notoriously
    difficult to do in parallel, and are of somewhat diminished value on very
    large data (you rarely actually need a full sort).
    Often we include parallel-friendly alternatives like ``topk``
-   Dask Array doesn't implement operations like ``tolist`` that would be very
    inefficient for larger datasets. Likewise, it is very inefficient to iterate
    over a Dask array with for loops
-   Dask development is driven by immediate need, hence many lesser used
    functions have not been implemented. Community contributions are encouraged

See :doc:`the dask.array API<array-api>` for a more extensive list of
functionality.

Execution
---------

By default, Dask Array uses the threaded scheduler in order to avoid data
transfer costs, and because NumPy releases the GIL well.  It is also quite
effective on a cluster using the `dask.distributed`_ scheduler.

.. _`dask.distributed`: https://distributed.dask.org/en/latest/
.. _`NEP-13's type-casting hierarchy`: https://numpy.org/neps/nep-0013-ufunc-overrides.html#type-casting-hierarchy
