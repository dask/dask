API
---

.. currentmodule:: dask.dataframe

Dataframe
~~~~~~~~~

.. autosummary::

    DataFrame
    DataFrame.add
    DataFrame.append
    DataFrame.apply
    DataFrame.assign
    DataFrame.astype
    DataFrame.categorize
    DataFrame.columns
    DataFrame.compute
    DataFrame.corr
    DataFrame.count
    DataFrame.cov
    DataFrame.cummax
    DataFrame.cummin
    DataFrame.cumprod
    DataFrame.cumsum
    DataFrame.describe
    DataFrame.div
    DataFrame.drop
    DataFrame.drop_duplicates
    DataFrame.dropna
    DataFrame.dtypes
    DataFrame.explode
    DataFrame.fillna
    DataFrame.floordiv
    DataFrame.get_partition
    DataFrame.groupby
    DataFrame.head
    DataFrame.iloc
    DataFrame.index
    DataFrame.isna
    DataFrame.isnull
    DataFrame.iterrows
    DataFrame.itertuples
    DataFrame.join
    DataFrame.known_divisions
    DataFrame.loc
    DataFrame.map_partitions
    DataFrame.mask
    DataFrame.max
    DataFrame.mean
    DataFrame.memory_usage
    DataFrame.memory_usage_per_partition
    DataFrame.merge
    DataFrame.min
    DataFrame.mod
    DataFrame.mul
    DataFrame.ndim
    DataFrame.nlargest
    DataFrame.npartitions
    DataFrame.partitions
    DataFrame.pop
    DataFrame.pow
    DataFrame.prod
    DataFrame.quantile
    DataFrame.query
    DataFrame.radd
    DataFrame.random_split
    DataFrame.rdiv
    DataFrame.rename
    DataFrame.repartition
    DataFrame.replace
    DataFrame.reset_index
    DataFrame.rfloordiv
    DataFrame.rmod
    DataFrame.rmul
    DataFrame.rpow
    DataFrame.rsub
    DataFrame.rtruediv
    DataFrame.sample
    DataFrame.set_index
    DataFrame.shape
    DataFrame.std
    DataFrame.sub
    DataFrame.sum
    DataFrame.tail
    DataFrame.to_bag
    DataFrame.to_csv
    DataFrame.to_dask_array
    DataFrame.to_delayed
    DataFrame.to_hdf
    DataFrame.to_json
    DataFrame.to_parquet
    DataFrame.to_records
    DataFrame.to_sql
    DataFrame.truediv
    DataFrame.values
    DataFrame.var
    DataFrame.visualize
    DataFrame.where

Series
~~~~~~

.. autosummary::

   Series
   Series.add
   Series.align
   Series.all
   Series.any
   Series.append
   Series.apply
   Series.astype
   Series.autocorr
   Series.between
   Series.bfill
   Series.cat
   Series.clear_divisions
   Series.clip
   Series.clip_lower
   Series.clip_upper
   Series.compute
   Series.copy
   Series.corr
   Series.count
   Series.cov
   Series.cummax
   Series.cummin
   Series.cumprod
   Series.cumsum
   Series.describe
   Series.diff
   Series.div
   Series.drop_duplicates
   Series.dropna
   Series.dt
   Series.dtype
   Series.eq
   Series.explode
   Series.ffill
   Series.fillna
   Series.first
   Series.floordiv
   Series.ge
   Series.get_partition
   Series.groupby
   Series.gt
   Series.head
   Series.idxmax
   Series.idxmin
   Series.isin
   Series.isna
   Series.isnull
   Series.iteritems
   Series.known_divisions
   Series.last
   Series.le
   Series.loc
   Series.lt
   Series.map
   Series.map_overlap
   Series.map_partitions
   Series.mask
   Series.max
   Series.mean
   Series.memory_usage
   Series.memory_usage_per_partition
   Series.min
   Series.mod
   Series.mul
   Series.nbytes
   Series.ndim
   Series.ne
   Series.nlargest
   Series.notnull
   Series.nsmallest
   Series.nunique
   Series.nunique_approx
   Series.persist
   Series.pipe
   Series.pow
   Series.prod
   Series.quantile
   Series.radd
   Series.random_split
   Series.rdiv
   Series.reduction
   Series.repartition
   Series.replace
   Series.rename
   Series.resample
   Series.reset_index
   Series.rolling
   Series.round
   Series.sample
   Series.sem
   Series.shape
   Series.shift
   Series.size
   Series.std
   Series.str
   Series.sub
   Series.sum
   Series.to_bag
   Series.to_csv
   Series.to_dask_array
   Series.to_delayed
   Series.to_frame
   Series.to_hdf
   Series.to_string
   Series.to_timestamp
   Series.truediv
   Series.unique
   Series.value_counts
   Series.values
   Series.var
   Series.visualize
   Series.where


Groupby Operations
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.groupby

.. autosummary::
   DataFrameGroupBy.aggregate
   DataFrameGroupBy.apply
   DataFrameGroupBy.count
   DataFrameGroupBy.cumcount
   DataFrameGroupBy.cumprod
   DataFrameGroupBy.cumsum
   DataFrameGroupBy.get_group
   DataFrameGroupBy.max
   DataFrameGroupBy.mean
   DataFrameGroupBy.min
   DataFrameGroupBy.size
   DataFrameGroupBy.std
   DataFrameGroupBy.sum
   DataFrameGroupBy.var
   DataFrameGroupBy.cov
   DataFrameGroupBy.corr
   DataFrameGroupBy.first
   DataFrameGroupBy.last
   DataFrameGroupBy.idxmin
   DataFrameGroupBy.idxmax

.. autosummary::
   SeriesGroupBy.aggregate
   SeriesGroupBy.apply
   SeriesGroupBy.count
   SeriesGroupBy.cumcount
   SeriesGroupBy.cumprod
   SeriesGroupBy.cumsum
   SeriesGroupBy.get_group
   SeriesGroupBy.max
   SeriesGroupBy.mean
   SeriesGroupBy.min
   SeriesGroupBy.nunique
   SeriesGroupBy.size
   SeriesGroupBy.std
   SeriesGroupBy.sum
   SeriesGroupBy.var
   SeriesGroupBy.first
   SeriesGroupBy.last
   SeriesGroupBy.idxmin
   SeriesGroupBy.idxmax

.. autosummary::
   Aggregation

Rolling Operations
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   rolling.map_overlap
   Series.rolling
   DataFrame.rolling

.. currentmodule:: dask.dataframe.rolling

.. autosummary::
   Rolling.apply
   Rolling.count
   Rolling.kurt
   Rolling.max
   Rolling.mean
   Rolling.median
   Rolling.min
   Rolling.quantile
   Rolling.skew
   Rolling.std
   Rolling.sum
   Rolling.var


Create DataFrames
~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   read_csv
   read_table
   read_fwf
   read_parquet
   read_hdf
   read_json
   read_orc
   read_sql_table
   from_array
   from_bcolz
   from_dask_array
   from_delayed
   from_pandas
   dask.bag.core.Bag.to_dataframe

Store DataFrames
~~~~~~~~~~~~~~~~

.. autosummary::

   to_csv
   to_parquet
   to_hdf
   to_records
   to_sql
   to_bag
   to_json

Convert DataFrames
~~~~~~~~~~~~~~~~~~

.. autosummary::

   DataFrame.to_dask_array
   DataFrame.to_delayed

Reshape DataFrames
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.reshape

.. autosummary::

   get_dummies
   pivot_table
   melt

DataFrame Methods
~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autoclass:: DataFrame
   :members:
   :inherited-members:

Series Methods
~~~~~~~~~~~~~~

.. autoclass:: Series
   :members:
   :inherited-members:

.. currentmodule:: dask.dataframe.groupby

DataFrameGroupBy
~~~~~~~~~~~~~~~~

.. autoclass:: DataFrameGroupBy
   :members:
   :inherited-members:

SeriesGroupBy
~~~~~~~~~~~~~

.. autoclass:: SeriesGroupBy
   :members:
   :inherited-members:

Custom Aggregation
~~~~~~~~~~~~~~~~~~
.. autoclass:: Aggregation


Storage and Conversion
~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autofunction:: read_csv
.. autofunction:: read_table
.. autofunction:: read_fwf
.. autofunction:: read_parquet
.. autofunction:: read_orc
.. autofunction:: read_hdf
.. autofunction:: read_json
.. autofunction:: read_sql_table
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz
.. autofunction:: from_dask_array
.. autofunction:: from_delayed
.. autofunction:: to_records
.. autofunction:: to_csv
.. autofunction:: to_bag
.. autofunction:: to_hdf
.. autofunction:: to_parquet
.. autofunction:: to_json
.. autofunction:: to_sql

Rolling
~~~~~~~

.. currentmodule:: dask.dataframe.rolling

.. autofunction:: map_overlap

Resampling
~~~~~~~~~~

.. currentmodule:: dask.dataframe.tseries.resample

.. autoclass:: Resampler
   :members:
   :inherited-members:

Dask Metadata
~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.utils

.. autofunction:: make_meta

Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autofunction:: compute
.. autofunction:: map_partitions
.. autofunction:: to_datetime
.. autofunction:: to_numeric

.. currentmodule:: dask.dataframe.multi

.. autofunction:: concat
.. autofunction:: merge         # doctest: +ELLIPSIS
.. autofunction:: merge_asof    # doctest: +ELLIPSIS

.. currentmodule:: dask.dataframe.reshape

.. autofunction:: get_dummies
.. autofunction:: pivot_table
.. autofunction:: melt
