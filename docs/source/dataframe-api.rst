API
---

Create DataFrames
~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.io

.. autosummary::
   read_csv
   read_hdf
   from_array
   from_pandas
   from_bcolz
   from_castra
   from_dask_array

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

GroupBy Methods
~~~~~~~~~~~~~~~

.. autoclass:: GroupBy
   :members:
   :inherited-members:

.. autoclass:: SeriesGroupBy
   :members:
   :inherited-members:

Other functions
~~~~~~~~~~~~~~~

.. autofunction:: compute
.. autofunction:: map_partitions
.. autofunction:: categorize
.. autofunction:: quantiles
.. autofunction:: set_index
.. autofunction:: shuffle

.. currentmodule:: dask.dataframe.multi

.. autofunction:: concat
.. autofunction:: merge

.. currentmodule:: dask.dataframe.io

.. autofunction:: read_csv
.. autofunction:: read_hdf
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz
.. autofunction:: from_castra
.. autofunction:: from_dask_array
