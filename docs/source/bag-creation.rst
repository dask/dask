Create Dask Bags
================

There are several ways to create dask.bags around your data:

``db.from_sequence``
--------------------

You can create a bag from an existing Python sequence:

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


``db.from_filenames``
---------------------

Dask.bag can load data from textfiles directly.
You can pass either a single filename, a list of filenames, or a globstring.
The resulting bag will have one item per line, one file per partition:

.. code-block:: python

   >>> b = db.from_filenames('myfile.json')
   >>> b = db.from_filenames(['myfile.1.json', 'myfile.2.json', ...])
   >>> b = db.from_filenames('myfile.*.json')

Dask.bag handles standard compression libraries, notably ``gzip`` and ``bz2``,
based on the filename extension:

.. code-block:: python

   >>> b = db.from_filenames('myfile.*.json.gz')

The resulting items in the bag are strings.  You may want to parse them using
functions like ``json.loads``:

.. code-block:: python

   >>> import json
   >>> b = db.from_filenames('myfile.*.json.gz').map(json.loads)

Or do string munging tasks.  For convenience there is a string namespace
attached directly to bags with ``.str.methodname``:

.. code-block:: python

   >>> b = db.from_filenames('myfile.*.csv.gz').str.strip().str.split(',')


``db.from_delayed``
----------------------

You can construct a dask bag from :doc:`dask.delayed <imperative>` values
using the ``db.from_delayed`` function.  See
:doc:`documentation on using dask.delayed with collections <imperative-collections>`
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

.. autofunction:: dask.bag.core.Bag.to_dataframe


To Delayed Values
-----------------

You can convert a dask bag into a list of :doc:`dask delayed values<imperative>`
and custom storage solutions from there.

.. autofunction:: dask.bag.core.Bag.to_delayed
