Create and Store Dask DataFrames
================================

Dask can create dataframes from various data storage formats like CSV, HDF,
Parquet, and others.  For most formats this data can live on various storage
systems including local disk, network file systems (NFS), the Hadoop File
System (HDFS), and Amazon's S3 (excepting HDF, which is only available on POSIX
like file systems).

See the `Overview section
<http://dask.pydata.org/en/latest/dataframe-overview.html>`_ for an in depth
discussion of ``dask.dataframe`` scope, use, limitations.

API
---

The following functions provide access to convert between Dask Dataframes,
file formats, and other Dask or Python collections.

.. currentmodule:: dask.dataframe

File Formats:

.. autosummary::

    read_csv
    read_parquet
    read_hdf
    from_bcolz
    from_array
    to_csv
    to_parquet
    to_hdf

Dask Collections:

.. autosummary::
    from_delayed
    from_dask_array
    dask.bag.core.Bag.to_dataframe
    to_delayed
    to_records
    to_bag

Pandas:

.. autosummary::
    from_pandas

Locations
---------

For text, CSV, and Parquet formats data can come from local disk, from the
Hadoop File System, from S3FS, or others, by prepending the filenames with a
protocol.

.. code-block:: python

   >>> df = dd.read_csv('my-data-*.csv')
   >>> df = dd.read_csv('hdfs:///path/to/my-data-*.csv')
   >>> df = dd.read_csv('s3://bucket-name/my-data-*.csv')

For remote systems like HDFS or S3 credentials may be an issue.  Usually these
are handled by configuration files on disk (such as a ``.boto`` file for S3)
but in some cases you may want to pass storage-specific options through to the
storage backend.  You can do this with the ``storage_options=`` keyword.

.. code-block:: python

   >>> df = dd.read_csv('s3://bucket-name/my-data-*.csv',
   ...                  storage_options={'anon': True})


Dask Delayed
------------

For more complex situations not covered by the functions above you may want to
use :doc:`dask.delayed<delayed-overview>` , which lets you construct
Dask.dataframes out of arbitrary Python function calls that load dataframes.
This can allow you to handle new formats easily, or bake in particular logic
around loading data if, for example, your data is stored with some special

See :doc:`documentation on using dask.delayed with
collections<delayed-collections>` or an `example notebook
<https://gist.github.com/mrocklin/e7b7b3a65f2835cda813096332ec73ca>`_ showing
how to create a Dask DataFrame from a nested directory structure of Feather
files (as a stand in for any custom file format).

Dask.delayed is particularly useful when simple ``map`` operations aren't
sufficient to capture the complexity of your data layout.


From Raw Dask Graphs
--------------------

This section is mainly for developers wishing to extend dask.dataframe.  It
discusses internal API not normally needed by users.  Everything below can be
done just as effectively with :doc:`dask.delayed<delayed-overview>`  described
just above.  You should never need to create a dataframe object by han

To construct a DataFrame manually from a dask graph you need the following
information:

1.  dask: a dask graph with keys like ``{(name, 0): ..., (name, 1): ...}`` as
    well as any other tasks on which those tasks depend.  The tasks
    corresponding to ``(name, i)`` should produce ``pandas.DataFrame`` objects
    that correspond to the columns and divisions information discussed below.
2.  name: The special name used above
3.  columns: A list of column names
4.  divisions: A list of index values that separate the different partitions.
    Alternatively, if you don't know the divisions (this is common) you can
    provide a list of ``[None, None, None, ...]`` with as many partitions as
    you have plus one.  For more information see the Partitions section in the
    :doc:`dataframe documentation <dataframe>`.

As an example, we build a DataFrame manually that reads several CSV files that
have a datetime index separated by day.  Note, you should never do this.  The
``dd.read_csv`` function does this for you.

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
