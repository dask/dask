Create Dask DataFrames
----------------------

From CSV files
~~~~~~~~~~~~~~

``dask.dataframe.read_csv`` uses ``pandas.read_csv`` and so inherits all of
that functions options.  Additionally it gains two new functionalities

1.  You can provide a globstring

.. code-block:: python

   >>> df = dd.read_csv('data.*.csv.gz', compression='gzip')

2.  You can specify the size of each block of data in bytes of uncompressed
    data.  Note that, especially for text data the size on disk may be much
    less than the number of bytes in memory.

.. code-block:: python

   >>> df = dd.read_csv('data.csv', chunkbytes=10000000)  # 1MB chunks

3.  You can ask to categorize your result.  This is slightly faster at read_csv
    time because we can selectively read the object dtype columns first.  This
    requires a full read of the dataset and may take some time

.. code-block:: python

   >>> df = dd.read_csv('data.csv', categorize=True)


so needs a docstring. Maybe we should have ``iris.csv`` somewhere in
the project.

From an Array
~~~~~~~~~~~~~

You can create a DataFrame from any sliceable array like object including both
NumPy arrays and HDF5 datasets.

.. code-block:: Python

   >>> dd.from_array(x, chunksize=1000000)

From BColz
~~~~~~~~~~

BColz_ is an on-disk, chunked, compressed, column-store.  These attributes make
it very attractive for dask.dataframe which can operate particularly well on
it.  There is a special ``from_bcolz`` function.

.. code-block:: Python

   >>> df = dd.from_bcolz('myfile.bcolz', chunksize=1000000)

In particular column access on a dask.dataframe backed by a ``bcolz.ctable``
will only read the necessary columns from disk.  This can provide dramatic
performance improvements.

Castra
~~~~~~

Castra_ is a tiny, experimental partitioned on-disk data structure designed to
fit the ``dask.dataframe`` model.  It provides columnstore access and range
queries along the index.  It is also a very small project (roughly 400 lines).

.. code-block:: Python

   >>> from castra import Castra
   >>> c = Castra(path='/my/castra/file')
   >>> df = c.to_dask()


.. _BColz: http://bcolz.blosc.org/
.. _Castra: http://github.com/Blosc/castra
