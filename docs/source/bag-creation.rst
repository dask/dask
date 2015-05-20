Create Bags
===========

There are several ways to create dask.bags around your data


``db.from_sequence``
--------------------

You can create a bag from an existing Python sequence.

.. code-block:: python

   >>> import dask.bag as db
   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6])

You can control the number of partitions into which this data is binned.

.. code-block:: python

   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6], npartitions=2)

This controls the granularity of the parallelism that you expose.  By default
dask will try to partition your data into about 100 partitions.

Warning: you should not load your data into Python and then load that data into
dask.bag.  Instead, you should use dask.bag to load your data.  This
parallelizes the loading step and reduces inter-worker communication.

.. code-block:: python

   >>> b = db.from_sequence(['1.dat', '2.dat', ...]).map(load_from_filename)


``db.from_filenames``
---------------------

Dask.bag can load data from textfiles directly.
You can pass either a single filename, a list of filenames, or a globstring.
The resulting bag will have one item per line, one file per partition.

.. code-block:: python

   >>> b = db.from_filenames('myfile.json')
   >>> b = db.from_filenames(['myfile.1.json', 'myfile.2.json', ...])
   >>> b = db.from_filenames('myfile.*.json')

Dask.bag handles standard compression libraries, notably ``gzip`` and ``bz2``,
based on the filename extension.

.. code-block:: python

   >>> b = db.from_filenames('myfile.*.json.gz')

The resulting items in the bag are strings.  You may want to parse them using
functions like ``json.loads``

.. code-block:: python

   >>> import json
   >>> b = db.from_filenames('myfile.*.json.gz').map(json.loads)

Or do string munging tasks.  For convenience there is a string namespace
attached directly to bags with ``.str.methodname``.

.. code-block:: python

   >>> b = db.from_filenames('myfile.*.csv.gz').str.strip().str.split(',')


``db.from_hdfs``
----------------

Dask.bag can use WebHDFS to load text data from HDFS

.. code-block:: python

   >>> from pywebhdfs.webhdfs import PyWebHdfsClient
   >>> hdfs = PyWebHdfsClient(host='hostname', user_name='hdfs')

   >>> b = db.from_hdfs('/user/username/data/2015/06/', hdfs=hdfs)

If the input is a directory then we return all data underneath that directory
and all subdirectories.

This uses WebHDFS to pull data from HDFS and so only works if that is enabled.
It does not require your computer to actually be on HDFS, merely that you have
network access.  Data will be downloaded to memory, decompressed, used, and
cleaned up as necessary.

Notably, this function does not tightly integrate dask.bag with a Hadoop
cluster.  Computation is not guaranteed (or likely) to be local to the node
that has the data.  This functionality is not the same as what you would get
with Hadoop or Spark.  *No dask scheduler currently integrates nicely with
data-local file systems like HDFS*.
