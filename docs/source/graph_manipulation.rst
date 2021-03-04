.. _graph_manipulation:

Advanced graph manipulation
===========================
There are some situations where computations with Dask collections will result in
suboptimal memory usage (e.g. an entire Dask DataFrame is loaded into memory).
This may happen when Dask’s scheduler doesn’t automatically delay the computation of
nodes in a task graph to avoid occupying memory with their output for prolonged periods
of time, or in scenarios where recalculating nodes is much cheaper than holding their
output in memory.

This page highlights a set of graph manipulation utilities which can be used to help
avoid these scenarios. In particular, the utilities described below rewrite the
underlying Dask graph for Dask collections, producing equivalent collections with
different sets of keys.

Consider the following example:

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.random.normal(size=500_000_000, chunks=100_000)
   >>> x_mean = x.mean()
   >>> y = (x - x_mean).max().compute()

The above example computes the largest value of a distribution after removing its bias.
This involves loading the chunks of ``x`` into memory in order to compute ``x_mean``.
However, since the ``x`` array is needed later in the computation to compute ``y``, the
entire ``x`` array is kept in memory. For large Dask Arrays this can be very
problematic.

To alleviate the need for the entire ``x`` array to be kept in memory, one could rewrite
the last line as follows:

.. code-block:: python

   >>> from dask.graph_manipulation import bind
   >>> xb = bind(x, x_mean)
   >>> y = (xb - x_mean).max().compute()

Here we use :func:`~dask.graph_manipulation.bind` to create a new Dask Array, ``xb``,
which produces exactly the same output as ``x``, but whose underlying Dask graph has
different keys than ``x``, and will only be computed after ``x_mean`` has been
calculated.

This results in the chunks of ``x`` being computed and immediately individually reduced
by ``mean``; then recomputed and again immediately pipelined into the subtraction
followed by reduction with ``max``. This results in a much smaller peak memory usage as
the full ``x`` array is no longer loaded into memory. However, the tradeoff is that the
compute time increases as ``x`` is computed twice.


API
---

.. currentmodule:: dask.graph_manipulation

.. autosummary::

   checkpoint
   wait_on
   bind
   clone


Definitions
~~~~~~~~~~~

.. autofunction:: checkpoint
.. autofunction:: wait_on
.. autofunction:: bind
.. autofunction:: clone
