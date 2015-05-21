Bag
===

Dask.Bag parallelizes computations across a large collection of generic Python
objects.  It is particularly useful when dealing with large quantities of
semi-structured data like JSON blobs or log files.


Name
----

*Bag* is an abstract collection, like list and set

* ``list``: *ordered* collection *with repeats*, ``[1, 2, 3, 2]``
* ``set``: *unordered* collection *without repeats*,  ``{1, 2, 3}``
* ``bag``: *unordered* collection *with repeats*, ``{1, 2, 2, 3}``

So a bag is like a list but doesn't guarantee an ordering among elements.
There can be repeated elements but you can't ask for a particular element.


Example
-------

We commonly use ``dask.bag`` to process unstructured or semi-structured data.

.. code-block:: python

   >>> import dask.bag as db
   >>> import json
   >>> js = db.from_filenames('logs/2015-*.json.gz').map(json.loads)
   >>> js.take(2)
   ({'name': 'Alice', 'location': {'city': 'LA', 'state': 'CA'}},
    {'name': 'Bob', 'location': {'city': 'NYC', 'state': 'NY'})

   >>> result = js.pluck('name').frequencies()  # just another Bag
   >>> dict(result)                             # Evaluate Result
   {'Alice': 10000, 'Bob': 5555, 'Charlie': ...}

Contents
--------

.. toctree::
   :maxdepth: 1

   bag-creation.rst
   bag-execution.rst
