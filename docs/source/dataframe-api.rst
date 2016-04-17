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
    DataFrame.to_imperative
    DataFrame.truediv
    DataFrame.var
    DataFrame.visualize
    DataFrame.where

Create DataFrames
~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.io

.. autosummary::
   read_csv
   from_array
   from_pandas
   from_bcolz

.. currentmodule:: dask.dataframe.core

DataFrame Methods
~~~~~~~~~~~~~~~~~

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

.. currentmodule:: dask.dataframe.io

.. autofunction:: read_csv
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz
