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
    DataFrame.column_info
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
    DataFrame.get_division
    DataFrame.groupby
    DataFrame.head
    DataFrame.iloc
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
    DataFrame.to_castra
    DataFrame.to_csv
    DataFrame.to_hdf
    DataFrame.to_delayed
    DataFrame.truediv
    DataFrame.var
    DataFrame.visualize
    DataFrame.where

Rolling Operations
~~~~~~~~~~~~~~~~~~

.. autosummary::
   rolling.rolling_apply
   rolling.rolling_chunk
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
   from_array
   from_bcolz
   from_castra
   read_csv
   from_dask_array
   from_delayed
   from_pandas

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

Other functions
~~~~~~~~~~~~~~~

.. autofunction:: compute
.. autofunction:: map_partitions
.. autofunction:: quantile

.. currentmodule:: dask.dataframe.multi

.. autofunction:: concat
.. autofunction:: merge

.. currentmodule:: dask.dataframe

.. autofunction:: read_csv
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz


.. autofunction:: rolling.rolling_apply
.. autofunction:: rolling.rolling_chunk
.. autofunction:: rolling.rolling_count
.. autofunction:: rolling.rolling_kurt
.. autofunction:: rolling.rolling_max
.. autofunction:: rolling.rolling_mean
.. autofunction:: rolling.rolling_median
.. autofunction:: rolling.rolling_min
.. autofunction:: rolling.rolling_quantile
.. autofunction:: rolling.rolling_skew
.. autofunction:: rolling.rolling_std
.. autofunction:: rolling.rolling_sum
.. autofunction:: rolling.rolling_var
.. autofunction:: rolling.rolling_window
