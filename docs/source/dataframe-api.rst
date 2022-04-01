API
---

.. currentmodule:: dask.dataframe.core

Dataframe
~~~~~~~~~

.. autosummary::
    :toctree: generated
    :recursive:
    :template: custom-class-template.rst

    DataFrame

Series
~~~~~~

.. autosummary::
   :toctree: generated
   :recursive:
   :template: custom-class-template.rst

   Series


Accessors
~~~~~~~~~

.. currentmodule:: dask.dataframe

Similar to pandas, Dask provides dtype-specific methods under various accessors.
These are separate namespaces within :class:`Series` that only apply to specific data types.

:doc:`Datetime Accessor<dataframe-api-dt>`

:doc:`String Accessor<dataframe-api-str>`

:doc:`Categorical Accessor<dataframe-api-cat>`

Groupby Operations
~~~~~~~~~~~~~~~~~~
.. currentmodule:: dask.dataframe.groupby

.. autosummary::
   :toctree: generated
   :recursive:
   :template: custom-class-template.rst

   Aggregation
   DataFrameGroupBy
   SeriesGroupBy

Rolling Operations
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   rolling.map_overlap
   Series.rolling
   DataFrame.rolling

.. currentmodule:: dask.dataframe.rolling

.. autosummary::
   :toctree: generated/
   :recursive:
   :template: custom-class-template.rst

   Rolling

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
   from_bcolz
   from_dask_array
   from_delayed
   from_pandas

.. currentmodule:: dask.bag

.. autosummary::
   :toctree: generated/

   Bag.to_dataframe


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

Convert DataFrames
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   DataFrame.to_bag
   DataFrame.to_dask_array
   DataFrame.to_delayed

Reshape DataFrames
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.reshape

.. autosummary::
   :toctree: generated/

   get_dummies
   pivot_table
   melt

Concatenate DataFrames
~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.multi

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
   :recursive:
   :template: custom-class-template.rst

   Resampler

HyperLogLog
~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/
   :recursive:
   :template: custom-module-template.rst

   hyperloglog


Dask Metadata
~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe.utils

.. autosummary::
   :toctree: generated/

   make_meta

Other functions
~~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   compute
   map_partitions

   to_datetime
   to_numeric