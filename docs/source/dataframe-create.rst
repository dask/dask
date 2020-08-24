Create and Store Dask DataFrames
================================

Dask can create DataFrames from various data storage formats like CSV, HDF,
Apache Parquet, and others.  For most formats, this data can live on various
storage systems including local disk, network file systems (NFS), the Hadoop
File System (HDFS), and Amazon's S3 (excepting HDF, which is only available on
POSIX like file systems).

See the :doc:`DataFrame overview page <dataframe>` for an in depth
discussion of ``dask.dataframe`` scope, use, and limitations.

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
    dask.bag.core.Bag.to_dataframe
    DataFrame.to_delayed
    to_records
    to_bag

Pandas:

.. autosummary::
    from_pandas

Creating
--------

Reading from various locations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For text, CSV, and Apache Parquet formats, data can come from local disk,
the Hadoop File System, S3FS, or other sources, by prepending the filenames with
a protocol:

.. code-block:: python

   >>> df = dd.read_csv('my-data-*.csv')
   >>> df = dd.read_csv('hdfs:///path/to/my-data-*.csv')
   >>> df = dd.read_csv('s3://bucket-name/my-data-*.csv')

For remote systems like HDFS, S3 or GS credentials may be an issue.  Usually, these
are handled by configuration files on disk (such as a ``.boto`` file for S3),
but in some cases you may want to pass storage-specific options through to the
storage backend.  You can do this with the ``storage_options=`` keyword:

.. code-block:: python

   >>> df = dd.read_csv('s3://bucket-name/my-data-*.csv',
   ...                  storage_options={'anon': True})
   >>> df = dd.read_parquet('gs://dask-nyc-taxi/yellowtrip.parquet',
   ...                      storage_options={'token': 'anon'})

Dask Delayed
~~~~~~~~~~~~

For more complex situations not covered by the functions above, you may want to
use :doc:`dask.delayed<delayed>`, which lets you construct Dask DataFrames out
of arbitrary Python function calls that load DataFrames.  This can allow you to
handle new formats easily or bake in particular logic around loading data if,
for example, your data is stored with some special format.

See :doc:`documentation on using dask.delayed with
collections<delayed-collections>` or an `example notebook
<https://gist.github.com/mrocklin/e7b7b3a65f2835cda813096332ec73ca>`_ showing
how to create a Dask DataFrame from a nested directory structure of Feather
files (as a stand in for any custom file format).

Dask delayed is particularly useful when simple ``map`` operations aren't
sufficient to capture the complexity of your data layout.

From Raw Dask Graphs
~~~~~~~~~~~~~~~~~~~~

This section is mainly for developers wishing to extend ``dask.dataframe``.  It
discusses internal API not normally needed by users.  Everything below can be
done just as effectively with :doc:`dask.delayed<delayed>`  described
just above.  You should never need to create a DataFrame object by hand.

To construct a DataFrame manually from a dask graph you need the following
information:

1.  Dask: a Dask graph with keys like ``{(name, 0): ..., (name, 1): ...}`` as
    well as any other tasks on which those tasks depend.  The tasks
    corresponding to ``(name, i)`` should produce ``pandas.DataFrame`` objects
    that correspond to the columns and divisions information discussed below
2.  Name: the special name used above
3.  Columns: a list of column names
4.  Divisions: a list of index values that separate the different partitions.
    Alternatively, if you don't know the divisions (this is common), you can
    provide a list of ``[None, None, None, ...]`` with as many partitions as
    you have plus one.  For more information, see the Partitions section in the
    :doc:`DataFrame documentation <dataframe>`

As an example, we build a DataFrame manually that reads several CSV files that
have a datetime index separated by day.  Note that you should **never** do this.
The ``dd.read_csv`` function does this for you:

.. code-block:: Python

   dsk = {('mydf', 0): (pd.read_csv, 'data/2000-01-01.csv'),
          ('mydf', 1): (pd.read_csv, 'data/2000-01-02.csv'),
          ('mydf', 2): (pd.read_csv, 'data/2000-01-03.csv')}
   name = 'mydf'
   columns = ['price', 'name', 'id']
   divisions = [Timestamp('2000-01-01 00:00:00'),
                Timestamp('2000-01-02 00:00:00'),
                Timestamp('2000-01-03 00:00:00'),
                Timestamp('2000-01-03 23:59:59')]

   df = dd.DataFrame(dsk, name, columns, divisions)

Storing
-------

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

See the :doc:`remote data services documentation<remote-data-services>`
for more information.
