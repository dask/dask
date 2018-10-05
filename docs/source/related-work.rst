Related Work
============

Writing the "related work" for a project called "distributed", is a Sisyphean
task.  We'll list a few notable projects that you've probably already heard of
down below.

You may also find the `dask comparison with spark`_ of interest.

.. _`dask comparison with spark`: http://docs.dask.org/en/latest/spark.html


Big Data World
--------------

*   The venerable Hadoop_ provides batch processing with the MapReduce
    programming paradigm.  Python users typically use `Hadoop Streaming`_ or
    MRJob_.
*   Spark builds on top of HDFS systems with a nicer API and in-memory
    processing.  Python users typically use PySpark_.
*   Storm_ provides streaming computation.  Python users typically use
    streamparse_.

This is a woefully inadequate representation of the excellent work blossoming
in this space.  A variety of projects have come into this space and rival or
complement the projects above.  Still, most "Big Data" processing hype probably
centers around the three projects above, or their derivatives.

.. _Hadoop: https://hadoop.apache.org/
.. _MRJob: https://pythonhosted.org/mrjob/
.. _`Hadoop Streaming`: https://hadoop.apache.org/docs/r1.2.1/streaming.html
.. _Spark: http://spark.apache.org/
.. _PySpark: http://spark.apache.org/docs/latest/api/python/
.. _storm: http://storm.apache.org/
.. _streamparse: https://streamparse.readthedocs.io/en/latest/index.html
.. _Disco: http://discoproject.org/

Python Projects
---------------

There are dozens of Python projects for distributed computing.  Here we list a
few of the more prominent projects that we see in active use today.

Task scheduling
~~~~~~~~~~~~~~~

*   Celery_: An asynchronous task scheduler, focusing on real-time processing.
*   Luigi_: A bulk big-data/batch task scheduler, with hooks to a variety of
    interesting data sources.

Ad hoc computation
~~~~~~~~~~~~~~~~~~

*   `IPython Parallel`_: Allows for stateful remote control of several running
    ipython sessions.
*   Scoop_: Implements the `concurrent.futures`_ API on distributed workers.
    Notably allows tasks to spawn more tasks.

Direct Communication
~~~~~~~~~~~~~~~~~~~~

*   MPI4Py_: Wraps the Message Passing Interface popular in high performance
    computing.
*   PyZMQ_: Wraps ZeroMQ, the high-performance asynchronous messaging library.

Venerable
~~~~~~~~~

There are a couple of older projects that often get mentioned

*   Dispy_: Embarrassingly parallel function evaluation
*   Pyro_:  Remote objects / RPC

.. _Luigi: https://luigi.readthedocs.io/en/latest/
.. _MPI4Py: http://mpi4py.readthedocs.io/en/stable/
.. _PyZMQ: https://github.com/zeromq/pyzmq
.. _Celery: http://www.celeryproject.org/
.. _`IPython Parallel`: https://ipyparallel.readthedocs.io/en/latest/
.. _Scoop: https://github.com/soravux/scoop/
.. _`concurrent.futures`: https://docs.python.org/3/library/concurrent.futures.html
.. _Dispy: http://dispy.sourceforge.net/
.. _Pyro: https://pythonhosted.org/Pyro4/

Relationship
------------

In relation to these projects ``distributed``...

*  Supports data-local computation like Hadoop and Spark
*  Uses a task graph with data dependencies abstraction like Luigi
*  In support of ad-hoc applications, like IPython Parallel and Scoop


In depth comparison to particular projects
------------------------------------------

IPython Parallel
~~~~~~~~~~~~~~~~

**Short Description**

`IPython Parallel`_ is a distributed computing framework from the IPython
project.  It uses a centralized hub to farm out jobs to several ``ipengine``
processes running on remote workers.  It communicates over ZeroMQ sockets and
centralizes communication through the central hub.

IPython parallel has been around for a while and, while not particularly fancy,
is quite stable and robust.

IPython Parallel offers parallel ``map`` and remote ``apply`` functions that
route computations to remote workers

.. code-block:: python

   >>> view = Client(...)[:]
   >>> results = view.map(func, sequence)
   >>> result = view.apply(func, *args, **kwargs)
   >>> future = view.apply_async(func, *args, **kwargs)

It also provides direct execution of code in the remote process and collection
of data from the remote namespace.

.. code-block:: python

   >>> view.execute('x = 1 + 2')
   >>> view['x']
   [3, 3, 3, 3, 3, 3]

**Brief Comparison**

Distributed and IPython Parallel are similar in that they provide ``map`` and
``apply/submit`` abstractions over distributed worker processes running Python.
Both manage the remote namespaces of those worker processes.

They are dissimilar in terms of their maturity, how worker nodes communicate to
each other, and in the complexity of algorithms that they enable.

**Distributed Advantages**

The primary advantages of ``distributed`` over IPython Parallel include

1.  Peer-to-peer communication between workers
2.  Dynamic task scheduling

``Distributed`` workers share data in a peer-to-peer fashion, without having to
send intermediate results through a central bottleneck.  This allows
``distributed`` to be more effective for more complex algorithms and to manage
larger datasets in a more natural manner.  IPython parallel does not provide a
mechanism for workers to communicate with each other, except by using the
central node as an intermediary for data transfer or by relying on some other
medium, like a shared file system.  Data transfer through the central node can
easily become a bottleneck and so IPython parallel has been mostly helpful in
embarrassingly parallel work (the bulk of applications) but has not been used
extensively for more sophisticated algorithms that require non-trivial
communication patterns.

The distributed client includes a dynamic task scheduler capable of managing
deep data dependencies between tasks.  The IPython parallel docs include `a
recipe`_ for executing task graphs with data dependencies.  This same idea is
core to all of ``distributed``, which uses a dynamic task scheduler for all
operations.  Notably, ``distributed.Future`` objects can be used within
``submit/map/get`` calls before they have completed.

.. code-block:: python

   >>> x = client.submit(f, 1)  # returns a future
   >>> y = client.submit(f, 2)  # returns a future
   >>> z = client.submit(add, x, y)  # consumes futures

The ability to use futures cheaply within ``submit`` and ``map`` methods
enables the construction of very sophisticated data pipelines with simple code.
Additionally, distributed can serve as a full dask task scheduler, enabling
support for distributed arrays, dataframes, machine learning pipelines, and any
other application build on dask graphs.  The dynamic task schedulers within
``distributed`` are adapted from the dask_ task schedulers and so are fairly
sophisticated/efficient.

**IPython Parallel Advantages**

IPython Parallel has the following advantages over ``distributed``

1.  Maturity:  IPython Parallel has been around for a while.
2.  Explicit control over the worker processes:  IPython parallel
    allows you to execute arbitrary statements on the workers, allowing it to
    serve in system administration tasks.
3.  Deployment help:  IPython Parallel has mechanisms built-in to aid
    deployment on SGE, MPI, etc..  Distributed does not have any such sugar,
    though is fairly simple to :doc:`set up <setup>` by hand.
4.  Various other advantages:  Over the years IPython parallel has accrued a
    variety of helpful features like IPython interaction magics, ``@parallel``
    decorators, etc..

.. _`a recipe`: https://ipython.org/ipython-doc/3/parallel/dag_dependencies.html#dag-dependencies
.. _dask: https://dask.org/


concurrent.futures
~~~~~~~~~~~~~~~~~~

The :class:`distributed.Client` API is modeled after :mod:`concurrent.futures`
and :pep:`3184`.  It has a few notable differences:

*  ``distributed`` accepts :class:`~distributed.client.Future` objects within
   calls to ``submit/map``. When chaining computations, it is preferable to
   submit Future objects directly rather than wait on them before submission.
*  The :meth:`~distributed.client.Client.map` method returns
   :class:`~distributed.client.Future` objects, not concrete results.
   The :meth:`~distributed.client.Client.map` method returns immediately.
*  Despite sharing a similar API, ``distributed`` :class:`~distributed.client.Future`
   objects cannot always be substituted for :class:`concurrent.futures.Future`
   objects, especially when using ``wait()`` or ``as_completed()``.
*  Distributed generally does not support callbacks.

If you need full compatibility with the :class:`concurrent.futures.Executor`
API, use the object returned by the
:meth:`~distributed.client.Client.get_executor` method.


.. _PEP-3184: https://www.python.org/dev/peps/pep-3148/
