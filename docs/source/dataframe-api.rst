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
These are separate namespaces within ``Series`` that only apply to specific data types.

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