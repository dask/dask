Overview
========

Dask Bag implements a operations like ``map, filter, fold, frequencies`` and
``groupby`` on lists of Python objects.  It does this in parallel using
multiple processes and in small memory using Python iterators building off of
libraries like PyToolz_.

.. _PyToolz: http://toolz.readthedocs.io/en/latest/

Design
------

Dask bags coordinate many Python lists or Iterators, each of which forms a
partition of a larger linear collection.

Common Uses
-----------

Dask bags are often used to parallelize simple computations on unstructured or
semi-structured data like text data, log files, JSON records, or user defined
Python objects.

Execution
---------

Execution on bags provide two benefits:

1.  Streaming: data processes lazily, allowing smooth execution of
    larger-than-memory data
2.  Parallel: data is split up, allowing multiple cores to execute in parallel.


Default scheduler
~~~~~~~~~~~~~~~~~

By default ``dask.bag`` uses ``dask.multiprocessing`` for computation.  As a
benefit dask bypasses the GIL and uses multiple cores on Pure Python objects.
As a drawback dask.bag doesn't perform well on computations that include a
great deal of inter-worker communication.  For common operations this is
rarely an issue as most ``dask.bag`` workflows are embarrassingly parallel or
result in reductions with little data moving between workers.

Shuffle
~~~~~~~

Some operations, like full ``groupby`` and bag-to-bag ``join`` do require
substantial inter-worker communication.  These are handled specially by shuffle
operations that use disk and a central memory server as a central point of
communication.

Shuffle operations are expensive and better handled by projects like
``dask.dataframe``.  It is best to use ``dask.bag`` to clean and process data,
then transform it into an array or dataframe before embarking on the more
complex operations that require shuffle steps.

Dask uses partd_ to perform efficient, parallel, spill-to-disk shuffles.

.. _partd: https://github.com/mrocklin/partd


Known Limitations
-----------------

Bags provide very general computation (any Python function.)  This generality
comes at cost.  Bags have the following known limitations:

1.  By default they rely on the multiprocessing scheduler, which has its own
    set of known limitations (see :doc:`shared`)
2.  Bag operations tend to be slower than array/dataframe computations in the
    same way that Python tends to be slower than NumPy/pandas
3.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
    Using ``Bag.foldby`` requires more thought.
4.  The implementation backing ``Bag.groupby`` is under heavy churn.


Name
----

*Bag* is the mathematical name for an unordered collection allowing repeats. It
is a friendly synonym to multiset_. A bag or a multiset is a generalization of
the concept of a set that, unlike a set, allows multiple instances of the
multiset's elements.

* ``list``: *ordered* collection *with repeats*, ``[1, 2, 3, 2]``
* ``set``: *unordered* collection *without repeats*,  ``{1, 2, 3}``
* ``bag``: *unordered* collection *with repeats*, ``{1, 2, 2, 3}``

So a bag is like a list, but it doesn't guarantee an ordering among elements.
There can be repeated elements but you can't ask for the ith element.

.. _multiset: http://en.wikipedia.org/wiki/Bag_(mathematics)
