Comparison to PySpark
=====================

Spark_ is a popular distributed computing tool written in Scala but with with
PySpark_ a decent Python API.  It is growing to become a dominant name today
in Big Data analysis in Python alongside Hadoop, for which MRJob_ is possibly
the dominant Python layer.

Dask has several elements that appear to intersect this space and as such we
often receive the following question:

*How does dask compare with Spark?*

Answering such comparison questions in an unbiased and perfectly informed way
is hard, particularly when the differences can be somewhat technical.  This
document tries to do this; we welcome any corrections.

Brief Answer
------------

Apache Spark is more mature and better integrates with HDFS.  It handles
resiliency and was originally built to scale up to thousands of nodes.

Dask is pip installable, doesn't use the Java Virtual Machine (JVM), and was
originally built to handle a large single workstation very efficiently.


User-Facing Differences
-----------------------

Scale
~~~~~

Spark began its life aimed at the thousand node cluster case.  As such it
thinks well about worker failures and tight integration with data-local file
systems like the Hadoop FileSystem (HDFS).  That being said, Spark can run in
standalone mode on a single machine.

Dask began its life building out parallel algorithms for numerical array
computations on a single computer.  As such it thinks well about low-latency
scheduling, low memory footprints, shared memory, and efficient use of local
disk.  That being said dask can run on a distributed cluster.


Java Python Performance
~~~~~~~~~~~~~~~~~~~~~~~

Spark is written in Scala, a multi-paradigm language built on top of the Java
Virtual Machine (JVM).  Since the rise of Hadoop, Java based languages have
steadily gained traction on data warehousing tasks and are good at managing
large amounts of heterogeneous data such as you mind find in JSON blobs.  The
Spark development team is now focusing more on binary and native data formats
with their new effort, Tungsten.

Dask is written in Python, a multi-paradigm language built on top of the
C/Fortan native stack.  It benefits from decades of scientific research
optimizing very fast computation on numeric data.  As such, dask is very good
on analytic computations on data such as you might find in HDF5 files or
analytic databases.  It can also handle JSON blob type data using Python data
structures (which are `surprisingly fast`_) using the cytoolz_ library in
parallel.


Java Python Disconnect
~~~~~~~~~~~~~~~~~~~~~~

Python users or Spark sometimes express frustration by how far separated they
are from computations.  Spark workers spin up JVMs which in turn spin up Python
processes.  Data moving back and forth makes extra trips both through a
distributed cluster and also through extra serialization layers (see py4j_) and
computation layers.  Limitations like the Java heap size come as a surprise to
users accustomed to native code execution.

Dask has an advantage for Python users because it is itself a Python library,
so serialization and debugging when things go wrong happens more smoothly.

However, dask has no benefit to non-Python users while Spark is useful in a
variety of JVM languages (Scala, Java, Clojure) as well as limited support in
Python and R.  New Spark projects like the DataFrame skip serialization and
boxed execution issues by forgoing the Python process entirely and instead have
Python code drive native Scala code.

Scope
~~~~~

Spark was originally built around the RDD, an unordered collection allowing
repeats.  All spark add-ons were originally built on top of this construct.

Dask is built on a slightly lower and more general construct of a generic task
graph with data dependencies.  This allows more general computations to be
built by users within the dask framework.  This is probably the largest
fundamental difference between the two projects.


Developer-Facing Differences
----------------------------

Graph Granularity
~~~~~~~~~~~~~~~~~

Both Spark and Dask represent computations with directed acyclic graphs.  These
graphs however represent computations at very different granularities.

One operation on a Spark RDD might add a node like ``Map`` and ``Filter`` to
the graph.  These are high-level operations that convey meaning and will
eventually be turned into many little tasks to execute on individual nodes.

Dask graphs skip this high-level representation and go directly to the "many
little tasks" stage.  As such one ``map`` operation on a dask collection will
immediately generate and add possibly thousands of tiny tasks to the dask
graph.

This difference in the scale of the underlying graph has implications on the
kinds of analysis and optimizations one can do and also on the generality that
one exposes to users.  Dask is unable to perform some optimizations that Spark
can because Dask does not have a top-down picture of the computation it was
asked to perform.  However, dask is able to easily represent far more complex
algorithms and expose the creation of these algorithms to normal users.


Coding Styles
~~~~~~~~~~~~~

Both Spark and Dask are written in a functional style.  Spark will probably be
more familiar to those who enjoy algebraic types while dask will probably be
more familiar to those who enjoy LISP and homoiconicity.


Conclusion
----------

If you have petabytes of JSON files, a simple workflow,  and a thousand node
cluster then you should probably use Spark.  If you have terabytes of binary
data, a complex workflow, and a multi-core workstation then you should probably
use dask.

If you have a terabyte of CSV or JSON data then you should forget both Spark
and Dask and go use Postgres_ or MongoDB_.


.. _Spark: https://spark.apache.org/
.. _PySpark: https://spark.apache.org/docs/latest/api/python/
.. _Hadoop: https://hadoop.apache.org/
.. _MRJob: https://mrjob.readthedocs.org
.. _`distributed computing`: distributed.html
.. _`surprisingly fast`: https://www.youtube.com/watch?v=PpBK4zIaFLE
.. _cytoolz: https://toolz.readthedocs.org
.. _py4j: http://py4j.sourceforge.net/
.. _Postgres: http://www.postgresql.org/
.. _MongoDB: https://www.mongodb.org/
