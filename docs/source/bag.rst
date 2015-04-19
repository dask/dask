Bag
===

Dask.Bag parallelizes computations across a list of Python objects.  It uses
``multiprocessing`` by default and is particularly useful when dealing with
large quantities of semi-structured data like JSON blobs.

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


Create Bags
-----------

We often create bags with the following functions::

   db.from_filenames -- filename, globstring, or list of filenames
   db.from_sequence -- Python Iterable

In the case of ``from_filenames`` we get one partition per file.  In the case
of ``from_sequence`` we get one partition per element if there are few
elements, or 100 partitions if there are many.  Note that we want our input to
this function to be very small.  All inputs passed in will have to be
serialized and sent to worker processes.


Evaluate Results
----------------

Bags have a ``.compute()`` method to trigger computation.  You must ensure
that your result will fit in memory.  Bags also support the ``__iter__``
protocol and so work well with pythonic collections like ``list, tuple, set,
dict``.  Converting your object into a list or dict can look more Pythonic
than calling ``.compute()``::

   >>> list(b.map(lambda x: x + 1))


Multiprocessing
---------------

Dask.bag executes tasks in parallel using multiprocessing.  This leverages
multiple cores on Pure Python code and so avoids the GIL, but means that
inter-worker communication is very expensive.  For common operations this is
rarely an issue as most ``dask.bag`` workflows are embarrassingly parallel or
result in reductions with little data moving between workers.

Exceptions to this include full ``groupby`` and bag-to-bag ``join`` operations.
These require inter-process communication and so will be very slow.  It is best
to use ``dask.bag`` to clean and process data into a form more suitable for
communication before doing serious analytics.


Function Serialization and Error Handling
-----------------------------------------

Dask.bag uses dill_ to serialize functions to send to worker processes.  Dill
supports almost any kind of function, including lambdas, closures, partials
and functions defined interactively.

When an error occurs in a remote process dask records the Exception and the
traceback and delivers them to the main process.  These tracebacks can not be
navigated (i.e. you can't use ``pdb``) but still contain valuable contextual
information.

These two features are arguably the most important when comparing ``dask.bag``
to direct use of ``multiprocessing``.

.. _dill: http://trac.mystic.cacr.caltech.edu/project/pathos/wiki/dill
