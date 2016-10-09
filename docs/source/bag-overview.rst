Overview
========

Dask.Bag implements a operations like ``map``, ``filter``, ``fold``, and
``groupby`` on collections of Python objects.  It does this in parallel and in
small memory using Python iterators.  It is similar to a parallel version of
PyToolz_ or a Pythonic version of the `PySpark RDD`_.

.. _PyToolz: https://toolz.readthedocs.io/en/latest/
.. _`PySpark RDD`: http://spark.apache.org/docs/latest/api/python/pyspark.html

Design
------

Dask bags coordinate many Python lists or Iterators, each of which forms a
partition of a larger collection.

Common Uses
-----------

Dask bags are often used to parallelize simple computations on unstructured or
semi-structured data like text data, log files, JSON records, or user defined
Python objects.

Execution
---------

Execution on bags provide two benefits:

1.  Parallel: data is split up, allowing multiple cores or machines to execute
    in parallel.
2.  Iterating: data processes lazily, allowing smooth execution of
    larger-than-memory data, even on a single machine within a single partition


Default scheduler
~~~~~~~~~~~~~~~~~

By default ``dask.bag`` uses ``dask.multiprocessing`` for computation.  As a
benefit Dask bypasses the GIL_ and uses multiple cores on Pure Python objects.
As a drawback Dask.bag doesn't perform well on computations that include a
great deal of inter-worker communication.  For common operations this is rarely
an issue as most Dask.bag workflows are embarrassingly parallel or result in
reductions with little data moving between workers.

.. _GIL: https://docs.python.org/3/glossary.html#term-gil


Shuffle
~~~~~~~

Some operations, like ``groupby``, require substantial inter-worker
communication. On a single machine, dask uses partd_ to perform efficient,
parallel, spill-to-disk shuffles. When working in a cluster, dask uses a task
based shuffle.

These shuffle operations are expensive and better handled by projects like
``dask.dataframe``. It is best to use ``dask.bag`` to clean and process data,
then transform it into an array or dataframe before embarking on the more
complex operations that require shuffle steps.

.. _partd: https://github.com/mrocklin/partd


Known Limitations
-----------------

Bags provide very general computation (any Python function.)  This generality
comes at cost.  Bags have the following known limitations:

1.  By default they rely on the multiprocessing scheduler, which has its own
    set of known limitations (see :doc:`shared`)
2.  Bags are immutable and so you can not change individual elements
3.  Bag operations tend to be slower than array/dataframe computations in the
    same way that standard Python containers tend to be slower than NumPy
    arrays and Pandas dataframes.
4.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
    Using ``Bag.foldby`` requires more thought.


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
