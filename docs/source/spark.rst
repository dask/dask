Comparison to PySpark
=====================

Spark_ is a popular distributed computing tool with a decent Python API
PySpark_.  Spark is growing to become a dominant name today in Big Data
analysis alongside Hadoop, for which MRJob_ is possibly the dominant
Python layer.

Dask has several elements that appear to intersect this space and we are often
asked, "How does Dask compare with Spark?"

Answering such comparison questions in an unbiased and informed way is hard,
particularly when the differences can be somewhat technical.  This document
tries to do this; we welcome any corrections.

Summary
-------

Apache Spark is more mature and better integrates with HDFS.  It handles
resiliency and was originally built to scale up to thousands of workers.

Dask is trivially installable, doesn't use the Java Virtual Machine (JVM),
and was originally built to handle numeric workloads in a large single
workstation very efficiently.


User-Facing Differences
-----------------------

Scale
~~~~~

Spark began its life aimed at the thousand node cluster case.  As
such it thinks well about worker failures and integration with data-local
file systems like the Hadoop FileSystem (HDFS).  That being said, Spark can
run in standalone mode on a single machine.

Dask began its life building out parallel algorithms for numerical array
computations on a single computer.  As such it thinks well about low-latency
scheduling, low memory footprints, shared memory, and efficient use of local
disk.  That being said dask can run on a distributed_ cluster.

.. _distributed: http://distributed.readthedocs.io/


Java Python Performance
~~~~~~~~~~~~~~~~~~~~~~~

Spark is written in Scala, a multi-paradigm language built on top of the Java
Virtual Machine (JVM).  Since the rise of Hadoop, Java based languages have
steadily gained traction on data warehousing tasks and are good at managing
large amounts of heterogeneous data such as you might find in JSON blobs.  The
Spark development team is now focusing more on binary and native data formats
with their new effort, Tungsten.

Dask is written in Python, a multi-paradigm language built on top of the
C/Fortran native stack.  This stack benefits from decades of scientific research
optimizing very fast computation on numeric data.  As such, dask is already
very good on analytic computations on data such as you might find in HDF5 files
or analytic databases.  It can also handle JSON blob type data using Python
data structures (which are `surprisingly fast`_) using the cytoolz_ library in
parallel.


Java Python Disconnect
~~~~~~~~~~~~~~~~~~~~~~

Python users on Spark sometimes express frustration by how far separated they
are from computations.  Some of this is inevitable; distributed debugging is a
hard problem.  Some of it however is due to having to hop over the JVM.  Spark
workers spin up JVMs which in turn spin up Python processes.  Data moving back
and forth makes extra trips both through a distributed cluster and also through
extra serialization layers (see py4j_) and computation layers.  Limitations
like the Java heap size and large Java stack traces come as a surprise to users
accustomed to native code execution.

Dask has an advantage for Python users because it is itself a Python library,
so serialization and debugging when things go wrong happens more smoothly.

However, dask only benefits Python users while Spark is useful in a
variety of JVM languages (Scala, Java, Clojure) and also has limited support in
Python and R.  New Spark projects like the DataFrame skip serialization and
boxed execution issues by forgoing the Python process entirely and instead have
Python code drive native Scala code.  APIs for these libraries tend to lag a
bit behind their Scala counterparts.


Scope
~~~~~

Spark was originally built around the RDD, an unordered collection allowing
repeats.  Most spark add-ons were built on top of this construct, inheriting
both its abilities and limitations.

Dask is built on a lower-level and more general construct of a generic task
graph with arbitrary data dependencies.  This allows more general computations
to be built by users within the dask framework.  This is probably the largest
fundamental difference between the two projects.  Dask gives up high-level
understanding to allow users to express more complex parallel algorithms.  This
ended up being essential when writing complex projects like ``dask.array``.


Developer-Facing Differences
----------------------------

Graph Granularity
~~~~~~~~~~~~~~~~~

Both Spark and Dask represent computations with directed acyclic graphs.  These
graphs however represent computations at very different granularities.

One operation on a Spark RDD might add a node like ``Map`` and ``Filter`` to
the graph.  These are high-level operations that convey meaning and will
eventually be turned into many little tasks to execute on individual workers.
This many-little-tasks state is only available internally to the Spark
scheduler.

Dask graphs skip this high-level representation and go directly to the
many-little-tasks stage.  As such one ``map`` operation on a dask collection
will immediately generate and add possibly thousands of tiny tasks to the dask
graph.

This difference in the scale of the underlying graph has implications on the
kinds of analysis and optimizations one can do and also on the generality that
one exposes to users.  Dask is unable to perform some optimizations that Spark
can because Dask schedulers do not have a top-down picture of the computation
they were asked to perform.  However, dask is able to easily represent far more
`complex algorithms`_ and expose the creation of these algorithms to normal users.

Dask.bag, the equivalent of the Spark.RDD, is just one abstraction built on top
of dask.  Others exist.  Alternatively power-users can forego high-level
collections entirely and jump straight to direct low-level task scheduling.


Coding Styles
~~~~~~~~~~~~~

Both Spark and Dask are written in a functional style.  Spark will probably be
more familiar to those who enjoy algebraic types while dask will probably be
more familiar to those who enjoy Lisp and "code as data structures".


Conclusion
----------

If you have petabytes of JSON files, a simple workflow,  and a thousand node
cluster then you should probably use Spark.  If you have 10s-1000s of gigabytes
of binary or numeric data, complex algorithms, and a large multi-core
workstation then you should probably use dask.

If you have a terabyte or less of CSV or JSON data then you should forget both
Spark and Dask and use Postgres_ or MongoDB_.


.. _Spark: https://spark.apache.org/
.. _PySpark: https://spark.apache.org/docs/latest/api/python/
.. _Hadoop: https://hadoop.apache.org/
.. _MRJob: https://mrjob.readthedocs.io
.. _`surprisingly fast`: https://www.youtube.com/watch?v=PpBK4zIaFLE
.. _cytoolz: https://toolz.readthedocs.io
.. _py4j: http://py4j.sourceforge.net/
.. _Postgres: http://www.postgresql.org/
.. _MongoDB: https://www.mongodb.org/
.. _`complex algorithms`: http://matthewrocklin.com/blog/work/2015/06/26/Complex-Graphs/
