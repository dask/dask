Dask
====

|Build Status| |Coverage| |Doc Status| |Gitter| |Version Status| |Downloads|
|Dask Examples|

Dask provides multi-core execution on larger-than-memory datasets using blocked
algorithms and task scheduling.  It maps high-level NumPy, Pandas, and list
operations on large datasets on to many operations on small in-memory
datasets.  It then executes these graphs in parallel on a single machine with the 
multiprocessing and multithreaded scheduler, and on many machines with the distributed 
scheduler.  New schedulers can be written or adapted in a straightforward manner
as well.  Dask lets us use traditional NumPy, Pandas, and list programming while operating on
inconveniently large data in a small amount of space.

*  ``dask`` is a specification to describe task dependency graphs.
*  ``dask.array`` is a drop-in NumPy replacement (for a subset of NumPy) that encodes blocked algorithms in ``dask`` dependency graphs.
*  ``dask.bag`` encodes blocked algorithms on Python lists of arbitrary Python objects.
*  ``dask.dataframe`` encodes blocked algorithms on Pandas DataFrames.
*  ``dask.async`` is a shared-memory asynchronous scheduler efficiently execute ``dask`` dependency graphs on multiple cores.

See full documentation at http://dask.pydata.org. Read developer-focused
blogposts_ about dask's development. Or try dask in your browser with example notebooks on Binder_.


Use ``dask.array``
------------------

Dask.array implements a numpy clone on larger-than-memory datasets using
multiple cores.

.. code-block:: python

    >>> import dask.array as da

    >>> x = da.random.normal(10, 0.1, size=(100000, 100000), chunks=(1000, 1000))

    >>> x.mean(axis=0)[:3].compute()
    array([ 10.00026926,  10.0000592 ,  10.00038236])


Use ``dask.dataframe``
----------------------

Dask.dataframe implements a Pandas clone on larger-than-memory datasets using
multiple cores.

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> df = dd.read_csv('nyc-taxi-*.csv.gz')

   >>> g = df.groupby('medallion')
   >>> g.trip_time_in_secs.mean().head(5)
   medallion
   0531373C01FD1416769E34F5525B54C8     795.875026
   867D18559D9D2941173AD7A0F3B33E77     924.187954
   BD34A40EDD5DC5368B0501F704E952E7     717.966875
   5A47679B2C90EA16E47F772B9823CE51     763.005149
   89CE71B8514E7674F1C662296809DDF6     869.274052
   Name: trip_time_in_secs, dtype: float64

Use ``dask.bag``
----------------

Dask.bag implements a large collection of Python objects and mimicking the
toolz_ interface

.. code-block:: python

   >>> import dask.bag as db
   >>> import json
   >>> b = db.from_filenames('2014-*.json.gz')
   ...       .map(json.loads)

   >>> alices = b.filter(lambda d: d['name'] == 'Alice')
   >>> alices.take(3)
   ({'name': 'Alice', 'city': 'LA',  'balance': 100},
    {'name': 'Alice', 'city': 'LA',  'balance': 200},
    {'name': 'Alice', 'city': 'NYC', 'balance': 300},

   >>> dict(alices.pluck('city').frequencies())
   {'LA': 10000, 'NYC': 20000, ...}


Use Dask Graphs
---------------

Dask.array, dask.dataframe, and dask.bag are thin layers on top of dask graphs,
which represent computational task graphs of regular Python functions on
regular Python objects.

As an example consider the following simple program:

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this computation as a dask graph in the following way:

.. code-block:: python

   d = {'x': 1,
        'y': (inc, 'x'),
        'z': (add, 'y', 10)}

A dask graph is just a dictionary of tuples where the first element of the
tuple is a function and the rest are the arguments for that function.  While
this representation of the computation above may be less aesthetically
pleasing, it may now be analyzed, optimized, and computed by other Python code,
not just the Python interpreter.

.. image:: docs/source/_static/dask-simple.png
   :height: 400px
   :alt: A simple dask dictionary
   :align: right


Install
-------

Dask is easily installable through your favorite Python package manager::

    conda install dask

    or

    pip install dask[array]
    or
    pip install dask[bag]
    or
    pip install dask[dataframe]
    or
    pip install dask[complete]


Dependencies
------------

``dask.core`` supports Python 2.6+ and Python 3.3+ with a common codebase.  It
is pure Python and requires no dependencies beyond the standard library. It is
a light weight dependency.

``dask.array`` depends on ``numpy``.

``dask.bag`` depends on ``toolz`` and ``cloudpickle``.


Examples
--------

Dask examples are available in the following repository: https://github.com/dask/dask-examples.

You can also find them in Anaconda.org: https://notebooks.anaconda.org/dask/.


LICENSE
-------

New BSD. See `License File <https://github.com/dask/dask/blob/master/LICENSE.txt>`__.


Related Work
------------

Task Scheduling
```````````````

One might ask why we didn't use one of these other fine libraries:

* Luigi_
* Joblib_
* mrjob_
* Any of the fine schedulers in numeric analysis (Plasma_, PaRSEC_, ...)
* Any of the fine high-throughput schedulers (Condor_, Pegasus_, Swiftlang_, ...)

The answer is because we wanted all of the following:

* Fine-ish grained parallelism (latencies around 1ms)
* In-memory communication of intermediate results
* Dependency structures more complex than ``map``
* Good support for numeric data
* First class Python support
* Trivial installation

Most task schedulers in the Python ecosystem target long-running batch jobs,
often for processing large amounts of text and aren't appropriate for executing
multi-core numerics.


Arrays
``````

There are many "Big NumPy Array" or general distributed array solutions all
with fine characteristics.  Some projects in the Python ecosystem include the
following:

*  Spartan_
*  Distarray_
*  Biggus_
*  Thunder_

There is a rich history of distributed array computing.  An incomplete sampling
includes the following projects:

* Elemental_
* Plasma_
* Arrays in MLlib_

.. _Spartan: https://github.com/spartan-array/spartan
.. _Distarray: http://docs.enthought.com/distarray/
.. _Biggus: https://github.com/SciTools/biggus
.. _Thunder: https://github.com/thunder-project/thunder/

.. _MLlib: http://spark.apache.org/docs/1.1.0/mllib-data-types.html
.. _Elemental: http://libelemental.org/
.. _Plasma: http://icl.cs.utk.edu/plasma/

.. _Luigi: http://luigi.readthedocs.org
.. _Joblib: https://pythonhosted.org/joblib/index.html
.. _mrjob: https://pythonhosted.org/mrjob/
.. _toolz: https://toolz.readthedocs.org/en/latest/
.. _Condor: http://research.cs.wisc.edu/htcondor/
.. _Pegasus: http://pegasus.isi.edu/
.. _Swiftlang: http://swift-lang.org/main/
.. _PaRSEC: http://icl.eecs.utk.edu/parsec/index.html
.. _blogposts: http://matthewrocklin.com/blog/tags.html#dask-ref
.. _Binder: http://mybinder.org/repo/dask/dask-examples
.. |Build Status| image:: https://travis-ci.org/dask/dask.svg
   :target: https://travis-ci.org/dask/dask
.. |Version Status| image:: https://img.shields.io/pypi/v/dask.svg
   :target: https://pypi.python.org/pypi/dask/
.. |Doc Status| image:: https://readthedocs.org/projects/dask/badge/?version=latest
   :target: https://readthedocs.org/projects/dask/?badge=latest
   :alt: Documentation Status
.. |Coverage| image:: https://coveralls.io/repos/mrocklin/dask/badge.svg
   :target: https://coveralls.io/r/mrocklin/dask
   :alt: Coverage status
.. |Gitter| image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/dask/dask
   :target: https://gitter.im/dask/dask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. |Downloads| image:: https://img.shields.io/pypi/dm/dask.svg
   :target: https://pypi.python.org/pypi/dask/
.. |Dask Examples| image:: http://mybinder.org/badge.svg
   :alt: Dask Example Notebooks
   :target: http://mybinder.org/repo/dask/dask-examples
