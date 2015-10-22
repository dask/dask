Related Work
============

Writing the "related work" for a project called "distributed", is a Sisyphean
task.  We'll list a few notable projects that you've probably already heard of
down below.

You may also find the `dask comparison with spark`_ of interest.

.. _`dask comparison with spark`: http://dask.pydata.org/en/latest/spark.html


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
.. _streamparse: https://streamparse.readthedocs.org/en/latest/index.html
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
*   PyZMQ_: Wraps ZeroMQ, the gentleman's socket.

Venerable
~~~~~~~~~

There are a couple of older projects that often get mentioned

*   Dispy_: Embarrassingly parallel function evaluation
*   Pyro_:  Remote objects / RPC

.. _Luigi: http://luigi.readthedocs.org/en/latest/
.. _MPI4Py: http://pythonhosted.org/mpi4py/
.. _PyZMQ: https://github.com/zeromq/pyzmq
.. _Celery: http://www.celeryproject.org/
.. _`IPython Parallel`: https://ipyparallel.readthedocs.org/en/latest/
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
