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

**`dask_expr.DataFrame`**

- `abs`
- `add`
- `add_prefix`
- `add_sufix`
- `align`
- `all`
- `any`
- `apply`
- `assign`
- `astype`
- `bfill`
- `clip`
- `combine_first`
- `copy`
- `count`
- `cummax`
- `cummin`
- `cumprod`
- `cumsum`
- `dask`
- `div`
- `divide`
- `drop`
- `drop_duplicates`
- `dropna`
- `dtypes`
- `eval`
- `explode`
- `ffill`
- `fillna`
- `floordiv`
- `groupby`
- `head`
- `idxmax`
- `idxmin`
- `ìloc`
- `index`
- `isin`
- `isna`
- `join`
- `map`
- `map_overlap`
- `map_partitions`
- `mask`
- `max`
- `mean`
- `memory_usage`
- `memory_usage_per_partition`
- `merge`
- `min`
- `min`
- `mod`
- `mode`
- `mul`
- `nlargest`
- `nsmallest`
- `nunique_approx`
- `partitions`
- `pivot_table`
- `pow`
- `prod`
- `query`
- `radd`
- `rdiv`
- `rename`
- `rename_axis`
- `repartition`
- `replace`
- `reset_index`
- `rfloordiv`
- `rmod`
- `rmul`
- `round`
- `rpow`
- `rsub`
- `rtruediv`
- `sample`
- `select_dtypes`
- `set_index`
- `shift`
- `shuffle`
- `sort_values`
- `std`
- `sub`
- `sum`
- `tail`
- `to_parquet`
- `to_timestamp`
- `truediv`
- `var`
- `visualize`
- `where`


**`dask_expr.Series`**

- `abs`
- `add`
- `align`
- `all`
- `any`
- `apply`
- `astype`
- `between`
- `bfill`
- `clip`
- `combine_first`
- `copy`
- `count`
- `cummax`
- `cummin`
- `cumprod`
- `cumsum`
- `dask`
- `div`
- `divide`
- `drop_duplicates`
- `dropna`
- `dtype`
- `explode`
- `ffill`
- `fillna`
- `floordiv`
- `groupby`
- `head`
- `idxmax`
- `idxmin`
- `index`
- `isin`
- `isna`
- `map`
- `map_partitions`
- `mask`
- `max`
- `mean`
- `memory_usage`
- `memory_usage_per_partition`
- `min`
- `min`
- `mod`
- `mode`
- `mul`
- `nlargest`
- `nsmallest`
- `nunique_approx`
- `partitions`
- `pow`
- `prod`
- `product`
- `radd`
- `rdiv`
- `rename`
- `rename_axis`
- `repartition`
- `replace`
- `reset_index`
- `rfloordiv`
- `rmod`
- `rmul`
- `round`
- `rpow`
- `rsub`
- `rtruediv`
- `shift`
- `shuffle`
- `std`
- `sub`
- `sum`
- `tail`
- `to_frame`
- `to_timestamp`
- `truediv`
- `unique`
- `value_counts`
- `var`
- `visualize`
- `where`


**`dask_expr.Index`**

- `abs`
- `align`
- `all`
- `any`
- `apply`
- `astype`
- `clip`
- `combine_first`
- `copy`
- `count`
- `dask`
- `dtype`
- `fillna`
- `groupby`
- `head`
- `idxmax`
- `idxmin`
- `index`
- `isin`
- `isna`
- `map_partitions`
- `max`
- `memory_usage`
- `min`
- `min`
- `mode`
- `nunique_approx`
- `partitions`
- `prod`
- `rename`
- `rename_axis`
- `repartition`
- `replace`
- `reset_index`
- `round`
- `shuffle`
- `std`
- `sum`
- `tail`
- `to_frame`
- `to_timestamp`
- `var`
- `visualize`


**`dask_expr._groupby.GroupBy`**

- `agg`
- `aggregate`
- `apply`
- `bfill
- `count`
- `ffill`
- `first`
- `last`
- `max`
- `mean`
- `median`
- `min`
- `nunique`
- `prod`
- `shift`
- `size`
- `std`
- `sum`
- `transform`
- `value_counts`
- `var`

Support for ``SeriesGroupBy`` and ``DataFrameGroupBy``.

**`dask_expr._resample.Resampler`**

- `agg`
- `count`
- `first`
- `last`
- `max`
- `mean`
- `median`
- `min`
- `nunique`
- `ohlc`
- `prod`
- `quantile`
- `sem`
- `size`
- `std`
- `sum`
- `var`


**`dask_expr._rolling.Rolling`**

- `agg`
- `apply`
- `count`
- `max`
- `mean`
- `median`
- `min`
- `quantile`
- `std`
- `sum`
- `var`
- `skew`
- `kurt`


**Binary operators (`DataFrame`, `Series`, and `Index`)**:

- `__add__`
- `__radd__`
- `__sub__`
- `__rsub__`
- `__mul__`
- `__pow__`
- `__rmul__`
- `__truediv__`
- `__rtruediv__`
- `__lt__`
- `__rlt__`
- `__gt__`
- `__rgt__`
- `__le__`
- `__rle__`
- `__ge__`
- `__rge__`
- `__eq__`
- `__ne__`
- `__and__`
- `__rand__`
- `__or__`
- `__ror__`
- `__xor__`
- `__rxor__`


**Unary operators (`DataFrame`, `Series`, and `Index`)**:

- `__invert__`
- `__neg__`
- `__pos__`

**Accessors**:

- `CategoricalAccessor`
- `DatetimeAccessor`
- `StringAccessor`

**Function**

- `concat`
- `from_pandas`
- `merge`
- `pivot_table`
- `read_csv`
- `read_parquet`
- `repartition`
- `to_datetime`
- `to_numeric`
- `to_timedelta`
- `to_parquet`
