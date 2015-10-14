API
---

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
.. autofunction:: quantile

.. currentmodule:: dask.dataframe.multi

.. autofunction:: concat
.. autofunction:: merge

.. currentmodule:: dask.dataframe.io

.. autofunction:: read_csv
.. autofunction:: from_array
.. autofunction:: from_pandas
.. autofunction:: from_bcolz
