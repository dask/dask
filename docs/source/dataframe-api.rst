API
---

.. currentmodule:: dask.dataframe

Dataframe
~~~~~~~~~

.. autosummary::
    :toctree: generated/

    DataFrame
    DataFrame.abs
    DataFrame.add
    DataFrame.align
    DataFrame.all
    DataFrame.any
    DataFrame.append
    DataFrame.apply
    DataFrame.applymap
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
    DataFrame.drop
    DataFrame.drop_duplicates
    DataFrame.dropna
    DataFrame.dtypes
    DataFrame.eq
    DataFrame.eval
    DataFrame.explode
    DataFrame.ffill
    DataFrame.fillna
    DataFrame.first
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
    DataFrame.last
    DataFrame.le
    DataFrame.loc
    DataFrame.lt
    DataFrame.map_partitions
    DataFrame.mask
    DataFrame.max
    DataFrame.mean
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
    DataFrame.to_bag
    DataFrame.to_csv
    DataFrame.to_dask_array
    DataFrame.to_delayed
    DataFrame.to_hdf
    DataFrame.to_html
    DataFrame.to_json
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
   Series.append
   Series.apply
   Series.astype
   Series.autocorr
   Series.between
   Series.bfill
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

.. currentmodule:: dask.dataframe.groupby

DataFrame Groupby
*****************

.. autosummary::
   :toctree: generated/

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
   DataFrameGroupBy.rolling


Series Groupby
**************

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   dask.dataframe