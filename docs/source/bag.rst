Bag
===

Dask.Bag parallelizes computations across a large collection of generic Python
objects.  It is particularly useful when dealing with large quantities of
semi-structured data like JSON blobs or log files.


Name
----

*Bag* is an abstract collection, like *list* or *set*.  It is a friendly
synonym to multiset_. A bag or a multiset is a generalization of the concept 
of a set that, unlike a set, allows multiple instances of the multiset's 
elements. 

* ``list``: *ordered* collection *with repeats*, ``[1, 2, 3, 2]``
* ``set``: *unordered* collection *without repeats*,  ``{1, 2, 3}``
* ``bag``: *unordered* collection *with repeats*, ``{1, 2, 2, 3}``

So a bag is like a list, but it doesn't guarantee an ordering among elements.
There can be repeated elements but you can't ask for a particular element.

.. _multiset: http://en.wikipedia.org/wiki/Bag_(mathematics)


Example
-------

We commonly use ``dask.bag`` to process unstructured or semi-structured data:

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

There are several ways to create dask.bags around your data:

``db.from_sequence``
~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

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


``db.from_hdfs``
~~~~~~~~~~~~~~~~

Dask.bag can use WebHDFS to load text data from HDFS:

.. code-block:: python

   >>> from pywebhdfs.webhdfs import PyWebHdfsClient
   >>> hdfs = PyWebHdfsClient(host='hostname', user_name='hdfs')

   >>> b = db.from_hdfs('/user/username/data/2015/06/', hdfs=hdfs)

If the input is a directory, then we return all data underneath that directory
and all subdirectories.

This uses WebHDFS to pull data from HDFS, and so only works if that is enabled.
It does not require your computer to actually be on HDFS, merely that you have
network access.  Data will be downloaded to memory, decompressed, used, and
cleaned up as necessary.

Notably, this function does not tightly integrate dask.bag with a Hadoop
cluster.  Computation is not guaranteed (or likely) to be local to the node
that has the data.  This functionality is not the same as what you would get
with Hadoop or Spark.  *No dask scheduler currently integrates nicely with
data-local file systems like HDFS*.

Execution
---------

Execution on bags provide two benefits:

1.  Streaming: data processes lazily, allowing smooth execution of
    larger-than-memory data
2.  Parallel: data is split up, allowing multiple cores to execute in parallel.


Trigger Evaluation
~~~~~~~~~~~~~~~~~~

Bags have a ``.compute()`` method to trigger computation:

.. code-block:: python

   >>> c = b.map(func)
   >>> c.compute()
   [1, 2, 3, 4, ...]

You must ensure that your result will fit in memory:

Bags also support the ``__iter__``
protocol and so work well with pythonic collections like ``list, tuple, set,
dict``.  Converting your object into a list or dict can look more Pythonic
than calling ``.compute()``:

.. code-block:: python

   >>> list(b.map(lambda x: x + 1))
   [1, 2, 3, 4, ...]

   >>> dict(b.frequencies())
   {'Alice': 100, 'Bob': 200, ...}


Default scheduler
~~~~~~~~~~~~~~~~~

By default ``dask.bag`` uses ``dask.multiprocessing`` for computation.  As a
benefit dask bypasses the GIL and uses multiple cores on Pure Python objects.
As a drawback dask.bag doesn't perform well on computations that include a
great deal of inter-worker communication.  For common operations this is
rarely an issue as most ``dask.bag`` workflows are embarrassingly parallel or
result in reductions with little data moving between workers.

Additionally, using multiprocessing opens up potential problems with function
serialization (see below).

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

Dask.bag uses partd_ to perform efficient, parallel, spill-to-disk shuffles.

.. _partd: https://github.com/mrocklin/partd


Function Serialization and Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask.bag uses cloudpickle_ to serialize functions to send to worker processes.
cloudpickle supports almost any kind of function, including lambdas, closures,
partials and functions defined interactively.

When an error occurs in a remote process the dask schedulers record the
Exception and the traceback and delivers these to the main process.  These
tracebacks can not be navigated (i.e. you can't use ``pdb``) but still contain
valuable contextual information.

These two features are arguably the most important when comparing ``dask.bag``
to direct use of ``multiprocessing``.

If you would like to turn off multiprocessing you can do so by setting the
default get function to the synchronous single-core scheduler:

.. code-block:: python

   >>> from dask.async import get_sync
   >>> b.compute(get=get_sync)

   or

   >>> import dask
   >>> dask.set_options(get=get_sync)  # set global
   >>> list(b)  # uses synchronous scheduler

.. _cloudpickle: https://github.com/cloudpipe/cloudpickle


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

.. _dask-bag-api:

API
---

.. currentmodule:: dask.bag.core

Create Bags
~~~~~~~~~~~

.. autosummary::
   from_sequence
   from_filenames
   from_hdfs
   concat

Turn Bags into other things
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   Bag.to_textfiles
   Bag.to_dataframe

Bag Methods
~~~~~~~~~~~

.. autoclass:: Bag
   :members:

Other functions
~~~~~~~~~~~~~~~

.. autofunction:: from_sequence
.. autofunction:: from_filenames
.. autofunction:: from_hdfs
.. autofunction:: concat
