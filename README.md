Dask Expressions
================

Dask Dataframes with query optimization.

This is a proof-of-concept rewrite of Dask dataframe that includes query
optimization and generally improved organization.

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
