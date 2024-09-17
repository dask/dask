Create Dask Bags
================

There are several ways to create Dask bags around your data:

``db.from_sequence``
--------------------

You can create a bag from an existing Python iterable:

.. code-block:: python

   >>> import dask.bag as db
   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6])

You can control the number of partitions into which this data is binned:

.. code-block:: python

   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6], npartitions=2)

This controls the granularity of the parallelism that you expose.  By default,
Dask will try to partition your data into about 100 partitions.

IMPORTANT: do not load your data into Python and then load that data into a
Dask bag.  Instead, use Dask Bag to load your data.  This
parallelizes the loading step and reduces inter-worker communication:

.. code-block:: python

   >>> b = db.from_sequence(['1.dat', '2.dat', ...]).map(load_from_filename)


``db.read_text``
----------------

Dask Bag can load data directly from text files.  You can pass either a 
single file name, a list of file names, or a globstring.  The resulting 
bag will have one item per line and one file per partition:

.. code-block:: python

   >>> b = db.read_text('myfile.txt')
   >>> b = db.read_text(['myfile.1.txt', 'myfile.2.txt', ...])
   >>> b = db.read_text('myfile.*.txt')

This handles standard compression libraries like ``gzip``, ``bz2``, ``xz``, or
any easily installed compression library that has a file-like object.
Compression will be inferred by the file name extension, or by using the
``compression='gzip'`` keyword:

.. code-block:: python

   >>> b = db.read_text('myfile.*.txt.gz')

The resulting items in the bag are strings. If you have encoded data like
line-delimited JSON, then you may want to map a decoding or load function across
the bag:

.. code-block:: python

   >>> import json
   >>> b = db.read_text('myfile.*.json').map(json.loads)

Or do string munging tasks.  For convenience, there is a string namespace
attached directly to bags with ``.str.methodname``:

.. code-block:: python

   >>> b = db.read_text('myfile.*.csv').str.strip().str.split(',')


``db.read_avro``
----------------

Dask Bag can read binary files in the `Avro`_ format if `fastavro`_ is installed.
A bag can be made from one or more files, with optional chunking within files. 
The resulting bag will have one item per Avro record, which will be a dictionary 
of the form given by the Avro schema.  There will be at least one partition per 
input file:

.. code-block:: python

   >>> b = db.read_avro('datafile.avro')
   >>> b = db.read_avro('data.*.avro')

.. _Avro: https://avro.apache.org/docs/1.8.2/
.. _fastavro: https://fastavro.readthedocs.io

By default, Dask will split data files into chunks of approximately ``blocksize`` 
bytes in size.  The actual blocks you would get depend on the internal blocking 
of the file.

For files that are compressed after creation (this is not the same as the internal 
"codec" used by Avro), no chunking should be used, and there will be exactly one 
partition per file:

.. code-block:: python

   > b = bd.read_avro('compressed.*.avro.gz', blocksize=None, compression='gzip')


``db.from_delayed``
-------------------

You can construct a Dask bag from :doc:`dask.delayed <delayed>` values using the 
``db.from_delayed`` function.  For more information, see
:doc:`documentation on using dask.delayed with collections <delayed-collections>`.


Store Dask Bags
===============

In Memory
---------

You can convert a Dask bag to a list or Python iterable by calling ``compute()`` 
or by converting the object into a list:

.. code-block:: python

   >>> result = b.compute()
   or
   >>> result = list(b)

To Text Files
-------------

You can convert a Dask bag into a sequence of files on disk by calling the
``.to_textfiles()`` method:

.. autofunction:: dask.bag.core.to_textfiles

To Avro
-------

Dask bags can be written directly to Avro binary format using `fastavro`_. One file
will be written per bag partition. This requires the user to provide a fully-specified
schema dictionary (see the docstring of the ``.to_avro()`` method).

.. autofunction:: dask.bag.avro.to_avro

To DataFrames
-------------

You can convert a Dask bag into a :doc:`Dask DataFrame<dataframe>` and use
those storage solutions.

.. automethod:: dask.bag.core.Bag.to_dataframe


To Delayed Values
-----------------

You can convert a Dask bag into a list of :doc:`Dask delayed values<delayed>`
and custom storage solutions from there.

.. automethod:: dask.bag.core.Bag.to_delayed
