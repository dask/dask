API
---

.. currentmodule:: dask.dataframe

Top level user functions:

.. autosummary::

    DataFrame
    DataFrame.add
    DataFrame.append
    DataFrame.apply
    DataFrame.assign
    DataFrame.astype
    DataFrame.cache
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
    DataFrame.fillna
    DataFrame.floordiv
    DataFrame.get_partition
    DataFrame.groupby
    DataFrame.head
    DataFrame.index
    DataFrame.iterrows
    DataFrame.itertuples
    DataFrame.join
    DataFrame.known_divisions
    DataFrame.loc
    DataFrame.map_partitions
    DataFrame.mask
    DataFrame.max
    DataFrame.mean
    DataFrame.merge
    DataFrame.min
    DataFrame.mod
    DataFrame.mul
    DataFrame.ndim
    DataFrame.nlargest
    DataFrame.npartitions
    DataFrame.pow
    DataFrame.quantile
    DataFrame.query
    DataFrame.radd
    DataFrame.random_split
    DataFrame.rdiv
    DataFrame.rename
    DataFrame.repartition
    DataFrame.reset_index
    DataFrame.rfloordiv
    DataFrame.rmod
    DataFrame.rmul
    DataFrame.rpow
    DataFrame.rsub
    DataFrame.rtruediv
    DataFrame.sample
    DataFrame.set_index
    DataFrame.set_partition
    DataFrame.std
    DataFrame.sub
    DataFrame.sum
    DataFrame.tail
    DataFrame.to_bag
    DataFrame.to_csv
    DataFrame.to_delayed
    DataFrame.to_hdf
    DataFrame.to_records
    DataFrame.truediv
    DataFrame.values
    DataFrame.var
    DataFrame.visualize
    DataFrame.where

Rolling Operations
~~~~~~~~~~~~~~~~~~

.. autosummary::
   rolling.map_overlap
   rolling.rolling_apply
   rolling.rolling_count
   rolling.rolling_kurt
   rolling.rolling_max
   rolling.rolling_mean
   rolling.rolling_median
   rolling.rolling_min
   rolling.rolling_quantile
   rolling.rolling_skew
   rolling.rolling_std
   rolling.rolling_sum
   rolling.rolling_var
   rolling.rolling_window

Create DataFrames
~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   read_csv
   read_table
   read_parquet
   read_hdf
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
    to_bag
    to_delayed

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

Storage and Conversion
~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autofunction:: read_csv
.. autofunction:: read_table
.. autofunction:: read_parquet
.. autofunction:: read_hdf
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz
.. autofunction:: from_dask_array
.. autofunction:: from_delayed
.. autofunction:: to_delayed
.. autofunction:: to_records
.. autofunction:: to_csv
.. autofunction:: to_bag
.. autofunction:: to_hdf
.. autofunction:: to_parquet

Rolling
~~~~~~~

.. currentmodule:: dask.dataframe.rolling

.. autofunction:: rolling_apply
.. autofunction:: map_overlap
.. autofunction:: rolling_count
.. autofunction:: rolling_kurt
.. autofunction:: rolling_max
.. autofunction:: rolling_mean
.. autofunction:: rolling_median
.. autofunction:: rolling_min
.. autofunction:: rolling_quantile
.. autofunction:: rolling_skew
.. autofunction:: rolling_std
.. autofunction:: rolling_sum
.. autofunction:: rolling_var
.. autofunction:: rolling_window


Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autofunction:: compute
.. autofunction:: map_partitions

.. currentmodule:: dask.dataframe.multi

.. autofunction:: concat
.. autofunction:: merge
