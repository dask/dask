Dask
====

|Build Status| |Doc Status|

A minimal task scheduling abstraction.

See Dask documentation at http://dask.readthedocs.org

LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/dask/blob/master/LICENSE.txt>`__.

Install
-------

``dask`` is not yet on any package index.  It is still experimental.

::

    python setup.py install

Example
-------

Consider the following simple program

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this as a dictionary in the following way

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


Dependencies
------------

``dask.core`` supports Python 2.6+ and Python 3.2+ with a common codebase.  It
is pure Python and requires no dependencies beyond the standard library.

It is, in short, a light weight dependency.

The threaded implementation depends on networkx.  The ``Array`` dataset depends
on ``numpy`` and the ``blaze`` family of projects.


Related Work
------------

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


.. _Luigi: http://luigi.readthedocs.org
.. _Joblib: https://pythonhosted.org/joblib/index.html
.. _mrjob: https://pythonhosted.org/mrjob/
.. _Condor: http://research.cs.wisc.edu/htcondor/
.. _Pegasus: http://pegasus.isi.edu/
.. _Swiftlang: http://swift-lang.org/main/
.. _DAGue: http://icl.eecs.utk.edu/dague/
.. |Build Status| image:: https://travis-ci.org/ContinuumIO/dask.png
   :target: https://travis-ci.org/ContinuumIO/dask
.. |Version Status| image:: https://pypip.in/v/dask.png
   :target: https://pypi.python.org/pypi/dask/
.. |Doc Status| image:: https://readthedocs.org/projects/dask/badge/?version=latest
   :target: https://readthedocs.org/projects/dask/?badge=latest
   :alt: Documentation Status
