Bag Quickstart
==============

A Dask Bag is a multiset, a set that allows repetition of its elements but
doesn't guarantee ordering, particular elements can't be accessed. The common
use for ``dask.bag`` is to process unstructured or semi-structured data.

Installation Testing
--------------------
To test that the ``dask.array`` Python library is working, after you
:doc:`install </install>` ``dask.bag`` run ``python``, then run a brief set of
commands to display a sequence of numbers:

.. code::

   python

.. code-block:: Python

   import dask.array
   b = dask.array.from_sequence(range(1, 21))
   b.compute()

If dask is installed correctly, this will print out a list of numbers:

.. code::

   [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

Basics
------

Dask.bag can load data from text files directly and handles standard
compression libraries based on the filename extension. After you create a
``dask-bag`` you can use ``map()`` to map a function across all the elements in
the collection, then call ``compute()`` to trigger the computation, the result
will be a list.  Even tough the data involved in the computations can be larger
than memory, you have to ensure that your result will fit in memory.

For example if you need to read several files `data-1.json`, `data-2.json` and 
so on, each one containing simple information like the following:

.. code::

   {"location": {"city": "LA", "state": "CA"}, "name": "John"}

you can use ``from_filenames`` and map ``json.loads`` to read the data into a
``dask.bag``.

.. code-block:: Python

   import dask.bag as db
   import json
   b = db.from_filenames('data-*.json').map(json.loads)
   b.take(1)

   # name frequencies, result is another bag
   result = b.pluck('name').frequencies()

   # use compute() to create a list of tuples
   result.compute()

   # coerce to a dictionary
   dict(result)

   # count elements
   b.count().compute()

   # create a list of all distinct cities
   cities = b.pluck('location').pluck('city')
   cities.distinct.compute()

For a more comprehensive list of features please see the 
:ref:`dask bag API<dask-bag-api>`.
