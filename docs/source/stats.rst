Stats
=====

Dask implements a subset of the `scipy.stats`_ package.

Statistical Functions
---------------------

You can calculate various measures of an array including skewnes, kurtosis, and arbitrary moments.

.. code-block:: python

   >>> x = da.random.beta(1, 1, size=(1000,), chunks=10)
   >>> k, s, m = [stats.kurtosis(x), stats.skew(x), stats.moment(x, 5)]
   >>> dask.compute(k, s, m)
   (1.7612340817172787, -0.064073498030693302, -0.00054523780628304799)


Statistical Tests
-----------------

You can perform basic statistical tests on dask arrays.


.. code-block:: python

   >>> a = da.random.uniform(size=(50,), chunks=(25,))
   >>> b = a + da.random.uniform(high=.25, size=(50,), chunks=(25,))
   >>> result = ttest_rel(a, b)
   >>> dask.compute(*result)
   (-12.951840468566829, 1.934718600173124e-17)

Each test returns one of the namedtuples defined in ``scipy.stats.stats``.


.. toctree::
   :maxdepth: 1

   stats-api.rst

.. _scipy.stats: https://docs.scipy.org/doc/scipy-0.19.0/reference/stats.html
