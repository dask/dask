Create Dask DataFrames
======================

Dask can create dataframes objects from various data storage formats.  Dask dataframes are commonly used to quickly inspect and analyze large volumes of tabular data stored in CSV, HDF5, or other tabular formats.  

See the `Overview section <http://dask.pydata.org/en/latest/dataframe-overview.html>`_ for an in depth discussion of ``dask.dataframe`` scope, use, limitations.    

From CSV files
--------------

``dask.dataframe.read_csv`` uses ``pandas.read_csv`` and so inherits most of
that function's options.  Additionally it gains two new functionalities

1.  You can provide a globstring

.. code-block:: python

   >>> df = dd.read_csv('data.*.csv') 

2.  You can specify the size of each block of data in bytes of uncompressed
    data.  Note that, especially for text data the size on disk may be much
    less than the number of bytes in memory.

.. code-block:: python

   >>> df = dd.read_csv('data.csv', chunkbytes=1000000)  # 1MB chunks 

For a detailed example of the ``dask.dataframe.read_csv`` method, :doc:`visit the CSV examples section</examples/dataframe-csv>`

From HDF5
----------

`HDF5 <https://www.hdfgroup.org/HDF5/doc/H5.intro.html>`_ is a Hierarchical Data Format (HDF) designed to store and organize large amounts of data.  Similar to the `pandas I\/O API <http://pandas.pydata.org/pandas-docs/stable/io.html>`_,  ``dask.dataframe`` can create a DataFrame directly from `HDF5 <https://www.hdfgroup.org/HDF5/doc/H5.intro.html>`_ datasets. For more detailed examples, :doc:`visit the HDF5 examples section</examples/dataframe-hdf5>`.

``dask.dataframe`` can read a single HDF5 ('myfile.hdf5') file by referencing a group key ('/x'):

.. code-block:: Python

   >>> import dask.dataframe as dd
   >>> dd.read_hdf('myfile1.hdf5', '/x', chunksize=1000000) 

It is also possible to create a DataFrame object from multiple HDF5 files in a directory with similar group keys by using a wildcard character (\*).  The ``dask.dataframe`` syntax for this task is:

.. code-block:: Python

   >>> import dask.dataframe as dd
   >>> dd.read_hdf('myfile*.hdf5', '/x', chunksize=1000000) 
   
Finally, ``dask.dataframe`` can load multiple datasets from a single HDF5 file using this syntax:

.. code-block:: Python
   
   >>> import dask.dataframe as dd 
   >>> dd.read_hdf('myfile1.hdf5', '/*', chunksize=1000000) 
   
From an Array
-------------

You can create a DataFrame from any sliceable array like object including both
NumPy arrays and HDF5 datasets. For a discussion of ``dask.array`` capabilities and
instruction on creating dask arrays, see the `Array Overview <http://dask.pydata.org/en/latest/array-overview.html>`_ and `Create Dask Arrays <http://dask.pydata.org/en/latest/array-creation.html>`_ sections.

.. code-block:: Python

   >>> dd.from_array(x, chunksize=1000000) 

From BColz
----------

BColz_ is an on-disk, chunked, compressed, column-store.  These attributes make
it very attractive for dask.dataframe which can operate particularly well on
it.  There is a special ``from_bcolz`` function.

.. code-block:: Python

   >>> df = dd.from_bcolz('myfile.bcolz', chunksize=1000000) 

In particular column access on a dask.dataframe backed by a ``bcolz.ctable``
will only read the necessary columns from disk.  This can provide dramatic
performance improvements.

.. _BColz: http://bcolz.blosc.org/

From Castra
-----------

Castra_ is a tiny, experimental partitioned on-disk data structure designed to
fit the ``dask.dataframe`` model.  It provides columnstore access and range
queries along the index.  It is also a very small project (roughly 400 lines)
and not actively maintained; use at your own risk.

.. code-block:: Python

   >>> from castra import Castra 
   >>> c = Castra(path='/my/castra/file') 
   >>> df = c.to_dask() 

.. _Castra: http://github.com/blaze/castra


From Bags
---------

You can create a dask.dataframe from a dask bag.

.. autosummary:: dask.bag.core.Bag.to_dataframe

Using ``dask.delayed``
----------------------

You can create a plan to arrange many Pandas frames into a sequence with normal
for loops using :doc:`dask.delayed<delayed-overview>` and then convert these
into a dask dataframe later.  See :doc:`documentation on using dask.delayed with
collections<delayed-collections>` or an `example notebook <https://gist.github.com/mrocklin/e7b7b3a65f2835cda813096332ec73ca>`_.
Dask.delayed is more useful when simple ``map`` operations aren't sufficient to
capture the complexity of your data layout.


From Raw Dask Graphs
--------------------

This section is for developer information and discusses internal API.  You
should never need to create a dataframe object by hand.

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
