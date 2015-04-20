Dask
====

|Build Status| |Coverage| |Doc Status|

Dask provides multi-core execution on larger-than-memory datasets using blocked
algorithms and task scheduling.  It maps high-level NumPy and list operations
on large datasets on to graphs of many operations on small in-memory datasets.
It then executes these graphs in parallel on a single machine.  Dask lets us
use traditional NumPy and list programming while operating on inconveniently
large data in a small amount of space.


*  ``dask`` is a specification to describe task dependency graphs.
*  ``dask.array`` is a drop-in NumPy replacement (for a subset of NumPy) that encodes blocked algorithms in ``dask`` dependency graphs.
*  ``dask.bag`` encodes blocked algorithms on Python lists of arbitrary Python objects.
*  ``dask.async`` is a shared-memory asynchronous scheduler efficiently execute ``dask`` dependency graphs on multiple cores.

Dask does not currently have a distributed memory scheduler.

See full documentation at http://dask.pydata.org or read developer-focused
blogposts_ about dask's development.


Install
-------

Dask is easily installable through your favorite Python package manager::

    conda install dask

    or

    pip install dask[array]
    or
    pip install dask[bag]
    or
    pip install dask[complete]


Dask Graphs
-----------

Consider the following simple program:

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this as a dictionary in the following way:

.. code-block:: python

   d = {'x': 1,
        'y': (inc, 'x'),
        'z': (add, 'y', 10)}

While less aesthetically pleasing this dictionary may now be analyzed,
optimized, and computed on by other Python code, not just the Python
interpreter.

.. image:: docs/source/_static/dask-simple.png
   :height: 400px
   :alt: A simple dask dictionary
   :align: right


Dask Arrays
-----------

The ``dask.array`` module creates these graphs from NumPy-like operations

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.random.random((4, 4), blockshape=(2, 2))
   >>> x.T[0, 3].dask
   {('x', 0, 0): (np.random.random, (2, 2)),
    ('x', 0, 1): (np.random.random, (2, 2)),
    ('x', 1, 0): (np.random.random, (2, 2)),
    ('x', 1, 1): (np.random.random, (2, 2)),
    ('y', 0, 0): (np.transpose, ('x', 0, 0)),
    ('y', 0, 1): (np.transpose, ('x', 1, 0)),
    ('y', 1, 0): (np.transpose, ('x', 0, 1)),
    ('y', 1, 1): (np.transpose, ('x', 1, 1)),
    ('z',): (getitem, ('y', 0, 1), (0, 1))}

Finally, a scheduler executes these graphs to achieve the intended result.  The
``dask.async`` module contains a shared memory scheduler that efficiently
leverages multiple cores.


Dependencies
------------

``dask.core`` supports Python 2.6+ and Python 3.3+ with a common codebase.  It
is pure Python and requires no dependencies beyond the standard library. It is
a light weight dependency.

``dask.array`` depends on ``numpy``.

``dask.bag`` depends on ``toolz`` and ``dill``.


LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/dask/blob/master/LICENSE.txt>`__.


Related Work
------------

Task Scheduling
```````````````

One might ask why we didn't use one of these other fine libraries:

* Luigi_
* Joblib_
* mrjob_
* Any of the fine schedulers in numeric analysis (DAGue_, ...)
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

There is a rich history of distributed array computing.  An incomplete sampling
includes the following projects:

* Elemental_
* Plasma_
* Arrays in MLlib_


.. _Spartan: https://github.com/spartan-array/spartan
.. _Distarray: http://docs.enthought.com/distarray/
.. _Biggus: https://github.com/SciTools/biggus

.. _MLlib: http://spark.apache.org/docs/1.1.0/mllib-data-types.html
.. _Elemental: http://libelemental.org/
.. _Plasma: http://icl.cs.utk.edu/plasma/

.. _Luigi: http://luigi.readthedocs.org
.. _Joblib: https://pythonhosted.org/joblib/index.html
.. _mrjob: https://pythonhosted.org/mrjob/
.. _Condor: http://research.cs.wisc.edu/htcondor/
.. _Pegasus: http://pegasus.isi.edu/
.. _Swiftlang: http://swift-lang.org/main/
.. _DAGue: http://icl.eecs.utk.edu/dague/
.. _blogposts: http://matthewrocklin.com/blog/tags.html#dask-ref
.. |Build Status| image:: https://travis-ci.org/ContinuumIO/dask.png
   :target: https://travis-ci.org/ContinuumIO/dask
.. |Version Status| image:: https://pypip.in/v/dask.png
   :target: https://pypi.python.org/pypi/dask/
.. |Doc Status| image:: https://readthedocs.org/projects/dask/badge/?version=latest
   :target: https://readthedocs.org/projects/dask/?badge=latest
   :alt: Documentation Status
.. |Coverage| image:: https://coveralls.io/repos/mrocklin/dask/badge.svg
   :target: https://coveralls.io/r/mrocklin/dask
   :alt: Coverage status
