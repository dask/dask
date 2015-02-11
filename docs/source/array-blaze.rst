Blaze
=====

Difference between Dask Arrays and Blaze
----------------------------------------

Blaze_ is a library to translate high-level syntax to various computational
backends; Blaze does no work on its own.  Dask is a computational backend; Dask
does work.

So you can drive dask arrays from Blaze, but you can also use Dask on its own,
just as you would use NumPy on its own.


Why use Blaze?
--------------

Blaze can reason about and optimize your expressions at a higher level.

Each operation on a dask array immediately produces a new task graph, so the
following Python code results in two operations on every block

.. code-block:: Python

   >>> ((x + 1) + 1)

Blaze thinks about this expression as an abstract syntax tree before it sends
it to Dask, which would immediately distribute these operations as lots of
little tasks in a dictionary.

Currently Blaze offers the following concrete benefits:

*  Numba integration for elementwise operations
*  DType and shape tracking
*  Integration with other Blaze projects like `Blaze Server`_

This comes at a cost of indirection which may confuse new users.

How to use Blaze with Dask
--------------------------

We can drive dask arrays with Blaze.

.. code-block:: Python

   >>> x = da.from_array(...)                   # Make a dask array

   >>> from blaze import Data, log, compute
   >>> d = Data(x)                              # Wrap with Blaze
   >>> y = log(d + 1)[:5].sum(axis=1)           # Do work as usual

   >>> result = compute(y)                      # Fall back to dask

This provides a smoother interactive experience, dtype tracking, numba
acceleration, etc. but does require an extra step.

If you're comfortable using Blaze and ``into`` you can jump directly from the
blaze expression to storage, leaving it to handle dataset creation.

.. code-block:: Python

   >>> from blaze import Data, log, into
   >>> d = Data(x)
   >>> y = log(d + 1)[:5].sum(axis=1)

   >>> into('myfile.hdf5::/data', y)

.. _`Blaze Server`: http://blaze.pydata.org/docs/dev/server.html
.. _Blaze: http://continuum.io/open-source/blaze/
