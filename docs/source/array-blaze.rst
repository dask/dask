Blaze
=====

Difference between Dask Arrays and Blaze
----------------------------------------

Blaze_ and Dask.array both provide array abstractions over biggish data, what
is the difference?

In short, Blaze is one level more abstract than Dask.  Blaze is an expression
system and thinks about syntax trees while Dask is a scheduling system and
thinks about blocked algorithms and directed acyclic graphs.

Blaze reasons about and optimizes the expressions that a user types in,
optimizing order of execution, operator fusion, checking type errors, and so on.
Blaze applies these optimizations and then translates to a variety of
computational systems, passing work off to them.  One such computational system
is dask.array.

Dask.arrays are fundamentally a way to create task schedules that execute
blocked matrix algorithms.  Dask.array does not think or optimize at the
expression level like Blaze.  Instead each operation on dask.arrays produces a new
dask.array with its own task directed acyclic graph.  Dask.arrays then optimize
*this* graph in ways very different from how Blaze might act on an expression
tree.

Finally, Blaze is general while Dask.array is specific to blocked NumPy
algorithms.


Example
-------

Consider the following scalar expression on the array ``x``.

.. code-block:: Python

    >>> (((x + 1) * 2) ** 3)

If ``x`` is a dask.array with 1000 blocks then each binary operation adds 1000
new tasks to the task graph.  Dask is unable to reason effectively about the
expression that the user has typed in.

However if ``x`` is a Blaze symbol then this graph only has a few nodes (``x``,
``1``, ``x + 1``, ...) and so Blaze is able to wrap this tree up into a fused
scalar operation.  If we then decide to execute the expression against
dask.array then Blaze can intelligently craft Numba_ ufuncs for dask.array to
use.


Why use Blaze?
--------------

Blaze and Dask have orthogonal sets of optimizations.  When we use them
together we can optimize first the expression tree and then translate to dask
and optimize the task dependency graph.

Currently Blaze offers the following concrete benefits:

*  Smoother immediate feedback for type and shape errors
*  dtype tracking
*  Numba integration for element-wise operations
*  Integration with other Blaze projects like `Blaze Server`_

However this comes at a cost of indirection and potential confusion.

These different projects (Blaze -> dask.array -> NumPy -> Numba) act as
different stages in a compiler.  They start at abstract syntax trees, move to
task DAGs, then to in-core computations, finally to LLVM and beyond.  For
simple problems you may only need to think about the middle of this chain
(NumPy, dask.array) but as you require more performance optimizations you
extend your interest to the outer edges (Blaze, Numba).


How to use Blaze with Dask
--------------------------

We can drive dask arrays with Blaze.

.. code-block:: Python

   >>> x = da.from_array(...)                   # Make a dask array

   >>> from blaze import Data, log, compute
   >>> d = Data(x)                              # Wrap with Blaze
   >>> y = log(d + 1)[:5].sum(axis=1)           # Do work as usual

   >>> result = compute(y)                      # Fall back to dask

If you're comfortable using Blaze and ``into`` you can jump directly from the
blaze expression to storage, leaving it to handle dataset creation.

.. code-block:: Python

   >>> from blaze import Data, log, into
   >>> d = Data(x)
   >>> y = log(d + 1)[:5].sum(axis=1)

   >>> into('myfile.hdf5::/data', y)

.. _`Blaze Server`: http://blaze.pydata.org/en/latest/server.html
.. _Blaze: http://continuum.io/open-source/blaze/
.. _Numba: http://numba.pydata.org/
