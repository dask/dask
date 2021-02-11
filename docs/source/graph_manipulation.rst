.. _graph_manipulation:

Advanced graph manipulation
===========================
There are some situations where computations with Dask collections will result in
suboptimal memory usage (e.g. an entire Dask DataFrame is loaded into memory).
This may happen when Dask’s scheduler doesn’t automatically delay the computation of
nodes in a task graph to avoid occupying memory with their output for prolonged
periods of time, or in scenarios where recalculating nodes is much cheaper
than holding their output in memory.

This page highlights a set of graph manipulation utilities which can be used to
help avoid these scenarios. In particular, the utilities described below rewrite the
underlying Dask graph for Dask collections, producing equivalent collections with
different sets of keys.

Consider the following example:

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.random.normal(size=5e9, chunks=1e7)
   >>> x_avg = x.avg()
   >>> y = (x - x_avg).topk(50000)[-1]

The above example computes the largest value of a distribution after removing
its bias. This involves loading the chunks of ``x`` into memory in order to compute
``x_mean``. However, since the ``x`` array is needed later in the computation
to compute ``y``, the entire ``x`` array is kept in memory. For large Dask Arrays
this can be very problematic.

To alleviate the need for the entire ``x`` array to be kept in memory, one could
rewrite the last line as follows:

.. code-block:: python

   >>> from dask.graph_manipulation import bind
   >>> xb = bind(x, x_avg)
   >>> y = (xb - x_avg).topk(50000)[-1]

``xb`` produces exactly the same output as ``x``, but it is rebuilt from zero, and only
after ``x_avg`` has been calculated. The chunks of ``x`` are computed and immediately
individually reduced by ``avg``; then recomputed and again immediately pipelined into
the subtraction followed by reduction with :func:`~dask.array.topk`. The peak RAM usage
has dropped drastically (measured: 5.5 GiB); the tradeoff is that the CPU time required
to compute ``x`` has doubled.


API
---

.. currentmodule:: dask.graph_manipulation

.. autosummary::

   checkpoint
   block_until_done
   bind
   clone


Definitions
~~~~~~~~~~~

.. autofunction:: checkpoint
.. autofunction:: block_until_done
.. autofunction:: bind
.. autofunction:: clone
