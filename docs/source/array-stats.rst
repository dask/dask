Stats
=====

Dask Array implements a subset of the `scipy.stats`_ package.

Statistical Functions
---------------------

You can calculate various measures of an array including skewness, kurtosis, and arbitrary moments.

.. code-block:: python

   >>> from dask.array import stats
   >>> x = da.random.beta(1, 1, size=(1000,), chunks=10)
   >>> k, s, m = [stats.kurtosis(x), stats.skew(x), stats.moment(x, 5)]
   >>> dask.compute(k, s, m)
   (1.7612340817172787, -0.064073498030693302, -0.00054523780628304799)


Statistical Tests
-----------------

You can perform basic statistical tests on Dask arrays.
Each of these tests return a ``dask.delayed`` wrapping one of the scipy ``namedtuple``
results.


.. code-block:: python

   >>> a = da.random.uniform(size=(50,), chunks=(25,))
   >>> b = a + da.random.uniform(low=-0.15, high=0.15, size=(50,), chunks=(25,))
   >>> result = stats.ttest_rel(a, b)
   >>> result.compute()
   Ttest_relResult(statistic=-1.5102104380013242, pvalue=0.13741197274874514)

.. _scipy.stats: https://docs.scipy.org/doc/scipy-0.19.0/reference/stats.html
