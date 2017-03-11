Create Dask Bags
================

There are several ways to create Dask.bags around your data:

``db.from_sequence``
--------------------

You can create a bag from an existing Python iterable:

.. code-block:: python

   >>> import dask.bag as db
   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6])

You can control the number of partitions into which this data is binned:

.. code-block:: python

   >>> b = db.from_sequence([1, 2, 3, 4, 5, 6], npartitions=2)

This controls the granularity of the parallelism that you expose.  By default
dask will try to partition your data into about 100 partitions.

IMPORTANT: do not load your data into Python and then load that data into
dask.bag.  Instead, use dask.bag to load your data.  This
parallelizes the loading step and reduces inter-worker communication:

.. code-block:: python

   >>> b = db.from_sequence(['1.dat', '2.dat', ...]).map(load_from_filename)


``db.read_text``
----------------

Dask.bag can load data directly from textfiles.
You can pass either a single filename, a list of filenames, or a globstring.
The resulting bag will have one item per line, one file per partition:

.. code-block:: python

   >>> b = db.read_text('myfile.txt')
   >>> b = db.read_text(['myfile.1.txt', 'myfile.2.txt', ...])
   >>> b = db.read_text('myfile.*.txt')

This handles standard compression libraries like ``gzip``, ``bz2``, ``xz``, or
any easily installed compression library that has a File-like object.
Compression will be inferred by filename extension, or by using the
``compression='gzip'`` keyword:

.. code-block:: python

   >>> b = db.read_text('myfile.*.txt.gz')

The resulting items in the bag are strings.  If you have encoded data like JSON
then you may want to map a decoding or load function across the bag:

.. code-block:: python

   >>> import json
   >>> b = db.read_text('myfile.*.json').map(json.loads)

Or do string munging tasks.  For convenience there is a string namespace
attached directly to bags with ``.str.methodname``:

.. code-block:: python

   >>> b = db.read_text('myfile.*.csv').str.strip().str.split(',')


``db.from_delayed``
-------------------

You can construct a dask bag from :doc:`dask.delayed <delayed>` values
using the ``db.from_delayed`` function.  See
:doc:`documentation on using dask.delayed with collections <delayed-collections>`
for more information.


Store Dask Bags
===============

In Memory
---------

You can convert a dask bag to a list or Python iterable by calling ``compute()`` or by converting the object into a list

.. code-block:: python

   >>> result = b.compute()
   or
   >>> result = list(b)

To Textfiles
------------

You can convert a dask bag into a sequence of files on disk by calling the
``.to_textfiles()`` method

.. autofunction:: dask.bag.core.to_textfiles


To DataFrames
-------------

You can convert a dask bag into a :doc:`dask dataframe<dataframe>` and use
those storage solutions.

.. automethod:: dask.bag.core.Bag.to_dataframe


To Delayed Values
-----------------

You can convert a dask bag into a list of :doc:`dask delayed values<delayed>`
and custom storage solutions from there.

.. automethod:: dask.bag.core.Bag.to_delayed
