Dask Expressions
================

Dask DataFrames with query optimization.

This is a proof-of-concept rewrite of Dask DataFrame that includes query
optimization and generally improved organization.

More in our blog posts:
- [Dask Expressions overview](https://blog.dask.org/2023/08/25/dask-expr-introduction)
- [TPC-H benchmark results vs. Dask DataFrame](https://blog.coiled.io/blog/dask-expr-tpch-dask.html)

Example
-------

```python
import dask_expr as dx

df = dx.datasets.timeseries()
df.head()

df.groupby("name").x.mean().compute()
```

Query Representation
--------------------

Dask-expr encodes user code in an expression tree:

```python
>>> df.x.mean().pprint()

Mean:
  Projection: columns='x'
    Timeseries: seed=1896674884
```

This expression tree will be optimized and modified before execution:

```python
>>> df.x.mean().optimize().pprint()

Div:
  Sum:
    Fused(375f9):
    | Projection: columns='x'
    |   Timeseries: dtypes={'x': <class 'float'>} seed=1896674884
  Count:
    Fused(375f9):
    | Projection: columns='x'
    |   Timeseries: dtypes={'x': <class 'float'>} seed=1896674884
```

Stability
---------

This project is a work in progress and will be changed without notice or
deprecation warning.  Please provide feedback, but it's best to avoid use in
production settings.


API Coverage
------------

Dask-Expr covers almost everything of the Dask DataFrame API. The only missing features are:

- ``melt``
- named GroupBy Aggregations
