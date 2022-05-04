Create and Store Dask DataFrames
================================

Dask can create DataFrames from various data storage formats like CSV, HDF,
Apache Parquet, and others.  For most formats, this data can live on various
storage systems including local disk, network file systems (NFS), the Hadoop
File System (HDFS), Google Cloud Storage, and Amazon's S3
(excepting HDF, which is only available on POSIX like file systems).

See the :doc:`DataFrame overview page <dataframe>` for more on
``dask.dataframe`` scope, use, and limitations.

API
---

The following functions provide access to convert between Dask DataFrames,
file formats, and other Dask or Python collections.

.. currentmodule:: dask.dataframe

File Formats:

.. autosummary::
    read_csv
    read_parquet
    read_hdf
    read_orc
    read_json
    read_sql_table
    read_sql_query
    read_sql
    read_table
    read_fwf
    from_bcolz
    from_array
    to_csv
    to_parquet
    to_hdf
    to_sql

Dask Collections:

.. autosummary::
    from_delayed
    from_dask_array
    from_map
    dask.bag.core.Bag.to_dataframe
    DataFrame.to_delayed
    to_records
    to_bag

Pandas:

.. autosummary::
    from_pandas

Creating
--------

Read from CSV
~~~~~~~~~~~~~

You can use :func:`read_csv` to read one or more CSV files into a Dask DataFrame.
It supports loading multiple files at once:
  
.. code-block:: python
   >>> df = dd.read_csv('myfiles.*.csv')

Or you can break up large files with the ``blocksize`` parameter:

.. code-block:: python
   >>> df = dd.read_csv('largefile.csv', blocksize=25e6)  # 25MB chunks  

Changing the ``blocksize`` parameter will change the number of partitions (see the explanation on
:doc:`partitions <dataframe-design-partitions>`). A good rule of thumb when working with
Dask DataFrames is to keep your partitions under 100MB in size.

Read from Parquet
~~~~~~~~~~~~~~~~~

Similarly, you can use :func:`read_parquet` to reading one or more Parquet files.
You can read in a single Parquet file:

.. code-block:: python
   >>> df = dd.read_parquet("path/to/mydata.parquet")

Or multiple Parquet files:

.. code-block:: python
   >>> df = dd.read_parquet("path/to/my/parquet/")

For more details on working with Parquet files, including tips and best practices, see the documentation
on :doc:`dataframe-parquet`.

Read from cloud storage
~~~~~~~~~~~~~~~~~~~~~~~

Dask can read data from a variety of data stores including network file systems, cloud object stores, and Hadoop.
You can usually do this by prepending a protocol to the paths:

.. code-block:: python

   >>> df = dd.read_csv('s3://bucket/path/to/data-*.csv')
   >>> df = dd.read_parquet('gcs://bucket/path/to/data-*.parq')

For remote systems like Amazon S3 or Google Cloud Storage, you may need to provide credentials.
These are usually handled by a configuration file.
In some cases you may want to pass storage-specific options through to the storage backend. You can do this with the ``storage_options=`` keyword:

.. code-block:: python

   >>> df = dd.read_csv('s3://bucket-name/my-data-*.csv',
   ...                  storage_options={'anon': True})
   >>> df = dd.read_parquet('gs://dask-nyc-taxi/yellowtrip.parquet',
   ...                      storage_options={'token': 'anon'})

See the documentation on connecting to :doc:`Amazon S3 <how-to/connect-to-remote-data-s3>` or
:doc:`Google Cloud Storage <how-to/connect-to-remote-data-gc>`.

Mapping from a function
~~~~~~~~~~~~~~~~~~~~~~~

For cases that are not covered by the functions above, but *can* be
captured by a simple ``map`` operation, :func:`from_map` is likely to be
the most convenient means for DataFrame creation. For example, this
API can be used to convert an arbitrary PyArrow ``Dataset`` object into a
DataFrame collection by mapping fragments to DataFrame partitions:

.. code-block:: python

   >>> import pyarrow.dataset as ds
   >>> dataset = ds.dataset("hive_data_path", format="orc", partitioning="hive")
   >>> fragments = dataset.get_fragments()
   >>> func = lambda frag: frag.to_table().to_pandas()
   >>> df = dd.from_map(func, fragments)


Dask Delayed
~~~~~~~~~~~~

:doc:`Dask delayed<delayed>` is particularly useful when simple ``map`` operations aren’t sufficient to capture the complexity of your data layout. It lets you construct Dask DataFrames out of arbitrary Python function calls, which can be
helpful to handle custom data formats or bake in particular logic around loading data.
See the :doc:`documentation on using dask.delayed with collections<delayed-collections>`.

Storing
-------

Writing files locally
~~~~~~~~~~~~~~~~~~~~~

You can save files locally, assuming each worker can access the same file system. Either the workers are located on the same machine, or a network file system is mounted and referenced at the same path location for every worker node.

Writing to remote locations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask can write to a variety of data stores including cloud object stores.
For example, you can write a ``dask.dataframe`` to an Azure storage blob as:

.. code-block:: python

   >>> d = {'col1': [1, 2, 3, 4], 'col2': [5, 6, 7, 8]}
   >>> df = dd.from_pandas(pd.DataFrame(data=d), npartitions=2)
   >>> dd.to_parquet(df=df,
   ...               path='abfs://CONTAINER/FILE.parquet'
   ...               storage_options={'account_name': 'ACCOUNT_NAME',
   ...                                'account_key': 'ACCOUNT_KEY'}

See the :doc:`how to connect to remote data <how-to/connect-to-remote-data>`
for more information.

Next Steps
----------

See the :doc:`DataFrame Best Practices <dataframe-best-practices>` for more tips
on how to use Dask DataFrame and solutions to common problems.
