Dask DataFrame API with Logical Query Planning
==============================================

.. currentmodule:: dask.dataframe

DataFrame
~~~~~~~~~

.. autosummary::
    :toctree: generated/

    DataFrame
    DataFrame.abs
    DataFrame.add
    DataFrame.align
    DataFrame.all
    DataFrame.any
    DataFrame.apply
    DataFrame.assign
    DataFrame.astype
    DataFrame.bfill
    DataFrame.categorize
    DataFrame.columns
    DataFrame.compute
    DataFrame.copy
    DataFrame.corr
    DataFrame.count
    DataFrame.cov
    DataFrame.cummax
    DataFrame.cummin
    DataFrame.cumprod
    DataFrame.cumsum
    DataFrame.describe
    DataFrame.diff
    DataFrame.div
    DataFrame.divide
    DataFrame.divisions
    DataFrame.drop
    DataFrame.drop_duplicates
    DataFrame.dropna
    DataFrame.dtypes
    DataFrame.eq
    DataFrame.eval
    DataFrame.explode
    DataFrame.ffill
    DataFrame.fillna
    DataFrame.floordiv
    DataFrame.ge
    DataFrame.get_partition
    DataFrame.groupby
    DataFrame.gt
    DataFrame.head
    DataFrame.idxmax
    DataFrame.idxmin
    DataFrame.iloc
    DataFrame.index
    DataFrame.info
    DataFrame.isin
    DataFrame.isna
    DataFrame.isnull
    DataFrame.items
    DataFrame.iterrows
    DataFrame.itertuples
    DataFrame.join
    DataFrame.known_divisions
    DataFrame.le
    DataFrame.loc
    DataFrame.lt
    DataFrame.map_partitions
    DataFrame.mask
    DataFrame.max
    DataFrame.mean
    DataFrame.median
    DataFrame.median_approximate
    DataFrame.melt
    DataFrame.memory_usage
    DataFrame.memory_usage_per_partition
    DataFrame.merge
    DataFrame.min
    DataFrame.mod
    DataFrame.mode
    DataFrame.mul
    DataFrame.ndim
    DataFrame.ne
    DataFrame.nlargest
    DataFrame.npartitions
    DataFrame.nsmallest
    DataFrame.partitions
    DataFrame.persist
    DataFrame.pivot_table
    DataFrame.pop
    DataFrame.pow
    DataFrame.prod
    DataFrame.quantile
    DataFrame.query
    DataFrame.radd
    DataFrame.random_split
    DataFrame.rdiv
    DataFrame.rename
    DataFrame.rename_axis
    DataFrame.repartition
    DataFrame.replace
    DataFrame.resample
    DataFrame.reset_index
    DataFrame.rfloordiv
    DataFrame.rmod
    DataFrame.rmul
    DataFrame.round
    DataFrame.rpow
    DataFrame.rsub
    DataFrame.rtruediv
    DataFrame.sample
    DataFrame.select_dtypes
    DataFrame.sem
    DataFrame.set_index
    DataFrame.shape
    DataFrame.shuffle
    DataFrame.size
    DataFrame.sort_values
    DataFrame.squeeze
    DataFrame.std
    DataFrame.sub
    DataFrame.sum
    DataFrame.tail
    DataFrame.to_backend
    DataFrame.to_bag
    DataFrame.to_csv
    DataFrame.to_dask_array
    DataFrame.to_delayed
    DataFrame.to_hdf
    DataFrame.to_html
    DataFrame.to_json
    DataFrame.to_orc
    DataFrame.to_parquet
    DataFrame.to_records
    DataFrame.to_string
    DataFrame.to_sql
    DataFrame.to_timestamp
    DataFrame.truediv
    DataFrame.values
    DataFrame.var
    DataFrame.visualize
    DataFrame.where

Series
~~~~~~

.. autosummary::
   :toctree: generated/

   Series
   Series.add
   Series.align
   Series.all
   Series.any
   Series.apply
   Series.astype
   Series.autocorr
   Series.between
   Series.bfill
   Series.clear_divisions
   Series.clip
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
   Series.dtype
   Series.eq
   Series.explode
   Series.ffill
   Series.fillna
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
   Series.known_divisions
   Series.le
   Series.loc
   Series.lt
   Series.map
   Series.map_overlap
   Series.map_partitions
   Series.mask
   Series.max
   Series.mean
   Series.median
   Series.median_approximate
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
   Series.sub
   Series.sum
   Series.to_backend
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

Index
~~~~~~

.. autosummary::
   :toctree: generated/

   Index
   Index.add
   Index.align
   Index.all
   Index.any
   Index.apply
   Index.astype
   Index.autocorr
   Index.between
   Index.bfill
   Index.clear_divisions
   Index.clip
   Index.compute
   Index.copy
   Index.corr
   Index.count
   Index.cov
   Index.cummax
   Index.cummin
   Index.cumprod
   Index.cumsum
   Index.describe
   Index.diff
   Index.div
   Index.drop_duplicates
   Index.dropna
   Index.dtype
   Index.eq
   Index.explode
   Index.ffill
   Index.fillna
   Index.floordiv
   Index.ge
   Index.get_partition
   Index.groupby
   Index.gt
   Index.head
   Index.is_monotonic_decreasing
   Index.is_monotonic_increasing
   Index.isin
   Index.isna
   Index.isnull
   Index.known_divisions
   Index.le
   Index.loc
   Index.lt
   Index.map
   Index.map_overlap
   Index.map_partitions
   Index.mask
   Index.max
   Index.median
   Index.median_approximate
   Index.memory_usage
   Index.memory_usage_per_partition
   Index.min
   Index.mod
   Index.mul
   Index.nbytes
   Index.ndim
   Index.ne
   Index.nlargest
   Index.notnull
   Index.nsmallest
   Index.nunique
   Index.nunique_approx
   Index.persist
   Index.pipe
   Index.pow
   Index.quantile
   Index.radd
   Index.random_split
   Index.rdiv
   Index.rename
   Index.repartition
   Index.replace
   Index.resample
   Index.reset_index
   Index.rolling
   Index.round
   Index.sample
   Index.sem
   Index.shape
   Index.shift
   Index.size
   Index.sub
   Index.to_backend
   Index.to_bag
   Index.to_csv
   Index.to_dask_array
   Index.to_delayed
   Index.to_frame
   Index.to_hdf
   Index.to_series
   Index.to_string
   Index.to_timestamp
   Index.truediv
   Index.unique
   Index.value_counts
   Index.values
   Index.visualize
   Index.where
   Index.to_frame

Accessors
~~~~~~~~~
Similar to pandas, Dask provides dtype-specific methods under various accessors.
These are separate namespaces within :class:`Series` that only apply to specific data types.

Datetime Accessor
*****************

**Methods**

.. autosummary::
   :toctree: generated/
   :template: autosummary/accessor_method.rst

   Series.dt.ceil
   Series.dt.floor
   Series.dt.isocalendar
   Series.dt.normalize
   Series.dt.round
   Series.dt.strftime

**Attributes**

.. autosummary::
   :toctree: generated/
   :template: autosummary/accessor_attribute.rst

   Series.dt.date
   Series.dt.day
   Series.dt.dayofweek
   Series.dt.dayofyear
   Series.dt.daysinmonth
   Series.dt.freq
   Series.dt.hour
   Series.dt.microsecond
   Series.dt.minute
   Series.dt.month
   Series.dt.nanosecond
   Series.dt.quarter
   Series.dt.second
   Series.dt.time
   Series.dt.timetz
   Series.dt.tz
   Series.dt.week
   Series.dt.weekday
   Series.dt.weekofyear
   Series.dt.year

String Accessor
***************

**Methods**

.. autosummary::
   :toctree: generated/
   :template: autosummary/accessor_method.rst

   Series.str.capitalize
   Series.str.casefold
   Series.str.cat
   Series.str.center
   Series.str.contains
   Series.str.count
   Series.str.decode
   Series.str.encode
   Series.str.endswith
   Series.str.extract
   Series.str.extractall
   Series.str.find
   Series.str.findall
   Series.str.fullmatch
   Series.str.get
   Series.str.index
   Series.str.isalnum
   Series.str.isalpha
   Series.str.isdecimal
   Series.str.isdigit
   Series.str.islower
   Series.str.isnumeric
   Series.str.isspace
   Series.str.istitle
   Series.str.isupper
   Series.str.join
   Series.str.len
   Series.str.ljust
   Series.str.lower
   Series.str.lstrip
   Series.str.match
   Series.str.normalize
   Series.str.pad
   Series.str.partition
   Series.str.repeat
   Series.str.replace
   Series.str.rfind
   Series.str.rindex
   Series.str.rjust
   Series.str.rpartition
   Series.str.rsplit
   Series.str.rstrip
   Series.str.slice
   Series.str.split
   Series.str.startswith
   Series.str.strip
   Series.str.swapcase
   Series.str.title
   Series.str.translate
   Series.str.upper
   Series.str.wrap
   Series.str.zfill

Categorical Accessor
********************

**Methods**

.. autosummary::
   :toctree: generated/
   :template: autosummary/accessor_method.rst

   Series.cat.add_categories
   Series.cat.as_known
   Series.cat.as_ordered
   Series.cat.as_unknown
   Series.cat.as_unordered
   Series.cat.remove_categories
   Series.cat.remove_unused_categories
   Series.cat.rename_categories
   Series.cat.reorder_categories
   Series.cat.set_categories

**Attributes**

.. autosummary::
   :toctree: generated/
   :template: autosummary/accessor_attribute.rst

   Series.cat.categories
   Series.cat.codes
   Series.cat.known
   Series.cat.ordered

Groupby Operations
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.api

DataFrame Groupby
*****************

.. autosummary::
   :toctree: generated/

   GroupBy.aggregate
   GroupBy.apply
   GroupBy.bfill
   GroupBy.count
   GroupBy.cumcount
   GroupBy.cumprod
   GroupBy.cumsum
   GroupBy.ffill
   GroupBy.get_group
   GroupBy.max
   GroupBy.mean
   GroupBy.min
   GroupBy.size
   GroupBy.std
   GroupBy.sum
   GroupBy.var
   GroupBy.cov
   GroupBy.corr
   GroupBy.first
   GroupBy.last
   GroupBy.idxmin
   GroupBy.idxmax
   GroupBy.rolling
   GroupBy.transform


Series Groupby
**************

.. autosummary::
   :toctree: generated/

   SeriesGroupBy.aggregate
   SeriesGroupBy.apply
   SeriesGroupBy.bfill
   SeriesGroupBy.count
   SeriesGroupBy.cumcount
   SeriesGroupBy.cumprod
   SeriesGroupBy.cumsum
   SeriesGroupBy.ffill
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
   SeriesGroupBy.rolling
   SeriesGroupBy.transform

Custom Aggregation
******************

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   Aggregation

Rolling Operations
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   Series.rolling
   DataFrame.rolling

.. currentmodule:: dask.dataframe.api

.. autosummary::
   :toctree: generated/

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
   :toctree: generated/

   read_csv
   read_table
   read_fwf
   read_parquet
   read_hdf
   read_json
   read_orc
   read_sql_table
   read_sql_query
   read_sql
   from_array
   from_dask_array
   from_delayed
   from_map
   from_pandas
   DataFrame.from_dict

Store DataFrames
~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   to_csv
   to_parquet
   to_hdf
   to_records
   to_sql
   to_json
   to_orc

Convert DataFrames
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   DataFrame.to_bag
   DataFrame.to_dask_array
   DataFrame.to_delayed

Reshape DataFrames
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   get_dummies
   pivot_table
   melt

Concatenate DataFrames
~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   DataFrame.merge
   concat
   merge
   merge_asof


Resampling
~~~~~~~~~~

.. currentmodule:: dask.dataframe.tseries.resample

.. autosummary::
   :toctree: generated/

   Resampler
   Resampler.agg
   Resampler.count
   Resampler.first
   Resampler.last
   Resampler.max
   Resampler.mean
   Resampler.median
   Resampler.min
   Resampler.nunique
   Resampler.ohlc
   Resampler.prod
   Resampler.quantile
   Resampler.sem
   Resampler.size
   Resampler.std
   Resampler.sum
   Resampler.var


Dask Metadata
~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.utils

.. autosummary::
   :toctree: generated/

   make_meta


Query Planning and Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   DataFrame.explain
   DataFrame.visualize
   DataFrame.analyze

Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   compute
   map_partitions
   map_overlap

   to_datetime
   to_numeric
   to_timedelta
