Comparison to Spark
===================

`Apache Spark <https://spark.apache.org/>`_ is a popular distributed computing
tool for tabular datasets that is growing to become a dominant name in Big Data
analysis today.  Dask has several elements that appear to intersect this space
and we are often asked, "How does Dask compare with Spark?"

Answering such comparison questions in an unbiased and informed way is hard,
particularly when the differences can be somewhat technical.  This document
tries to do this; we welcome any corrections.

Summary
-------

Generally Dask is smaller and lighter weight than Spark.  This means that it
has fewer features and, instead, is used in conjunction with other libraries,
particularly those in the numeric Python ecosystem.  It couples with libraries
like Pandas or Scikit-Learn to achieve high-level functionality.

Language
~~~~~~~~

-   Spark is written in Scala with some support for Python and R.  It
    interoperates well with other JVM code.

-   Dask is written in Python and only really supports Python.  It
    interoperates well with C/C++/Fortran/LLVM or other natively compiled
    code linked through Python.

Ecosystem
~~~~~~~~~

-   Spark is an all-in-one project that has inspired its own ecosystem.  It
    integrates well with many other Apache projects.

-   Dask is a component of the larger Python ecosystem.  It couples with and
    enhances other libraries like NumPy, Pandas, and Scikit-Learn.


Age and Trust
~~~~~~~~~~~~~

-   Spark is older (since 2010) and has become a dominant and
    well-trusted tool in the Big Data enterprise world.

-   Dask is younger (since 2014) and is an extension of the
    well trusted NumPy/Pandas/Scikit-learn/Jupyter stack.

Scope
~~~~~

-   Spark is more focused on traditional business intelligence
    operations like SQL and lightweight machine learning.

-   Dask is applied more generally both to business intelligence
    applications, as well as a number of scientific and custom situations.

Internal Design
~~~~~~~~~~~~~~~

-   Spark's internal model is higher level, providing good high level
    optimizations on uniformly applied computations, but lacking flexibility
    for more complex algorithms or ad-hoc systems.  It is fundamentally an
    extension of the Map-Shuffle-Reduce paradigm.

-   Dask's internal model is lower level, and so lacks high level
    optimizations, but is able to implement more sophisticated algorithms and
    build more complex bespoke systems.  It is fundamentally based on generic
    task scheduling.

Scale
~~~~~

-  Spark scales from a single node to thousand-node clusters.
-  Dask scales from a single node to thousand-node clusters.

APIs
~~~~

DataFrames
``````````

-   Spark DataFrame has its own API and memory model.  It also
    implements a large subset of the SQL language.  Spark includes a
    high-level query optimizer for complex queries.

-   Dask DataFrame reuses the Pandas API and memory model.  It implements
    neither SQL nor a query optimizer.  It is able to do random access,
    efficient time series operations, and other Pandas-style indexed
    operations.

Machine Learning
````````````````

-   Spark MLLib is a cohesive project with support for common operations
    that are easy to implement with Spark's Map-Shuffle-Reduce style
    system.  People considering MLLib might also want to consider *other*
    JVM-based machine learning libraries like H2O, which may have better
    performance.

-   Dask relies on and interoperates with existing libraries like
    Scikit-Learn and XGBoost.  These can be more familiar or higher
    performance, but generally results in a less-cohesive whole.  See the
    `dask-ml`_ project for integrations.

Arrays
``````

-   Spark does not include support for multi-dimensional arrays natively
    (this would be challenging given their computation model), although
    some support for two-dimensional matrices may be found in MLLib.
    People may also want to look at the
    `Thunder <https://github.com/thunder-project/thunder>`_ project, which
    combines Apache Spark with NumPy arrays.

-   Dask fully supports the NumPy model for
    :doc:`scalable multi-dimensional arrays <array>`.

Streaming
`````````

-   Spark's support for streaming data is first-class and integrates well
    into their other APIs.  It follows a mini-batch approach.  This
    provides decent performance on large uniform streaming operations.

-   Dask provides a :doc:`real-time futures interface <futures>` that is
    lower-level than Spark streaming.  This enables more creative and
    complex use-cases, but requires more work than Spark streaming.

Graphs / complex networks
`````````````````````````

-  Spark provides GraphX, a library for graph processing.

-  Dask provides no such library.

Custom parallelism
``````````````````

-   Spark generally expects users to compose computations out of their
    high-level primitives (map, reduce, groupby, join, ...).  It is also
    possible to extend Spark through subclassing RDDs, although this is
    rarely done.

-   Dask allows you to specify arbitrary task graphs for more complex and
    custom systems that are not part of the standard set of collections.

.. _dask-ml: https://ml.dask.org


Reasons you might choose Spark
------------------------------

-  You prefer Scala or the SQL language
-  You have mostly JVM infrastructure and legacy systems
-  You want an established and trusted solution for business
-  You are mostly doing business analytics with some lightweight machine learning
-  You want an all-in-one solution


Reasons you might choose Dask
-----------------------------

-  You prefer Python or native code, or have large legacy code bases that you
   do not want to entirely rewrite
-  Your use case is complex or does not cleanly fit the Spark computing model
-  You want a lighter-weight transition from local computing to cluster
   computing
-  You want to interoperate with other technologies and don't mind installing
   multiple packages


Reasons to choose both
----------------------

It is easy to use both Dask and Spark on the same data and on the same cluster.

They can both read and write common formats, like CSV, JSON, ORC, and Parquet,
making it easy to hand results off between Dask and Spark workflows.

They can both deploy on the same clusters.
Most clusters are designed to support many different distributed systems at the
same time, using resource managers like Kubernetes and YARN.  If you already
have a cluster on which you run Spark workloads, it's likely easy to also run
Dask workloads on your current infrastructure and vice versa.

In particular, for users coming from traditional Hadoop/Spark clusters (such as
those sold by Cloudera/Hortonworks) you are using the Yarn resource
manager.  You can deploy Dask on these systems using the `Dask Yarn
<https://yarn.dask.org>`_ project, as well as other projects, like `JupyterHub
on Hadoop <https://jupyterhub-on-hadoop.readthedocs.io/en/latest/>`_.


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
many-little-tasks stage.  As such, one ``map`` operation on a Dask collection
will immediately generate and add possibly thousands of tiny tasks to the Dask
graph.

This difference in the scale of the underlying graph has implications on the
kinds of analysis and optimizations one can do and also on the generality that
one exposes to users.  Dask is unable to perform some optimizations that Spark
can because Dask schedulers do not have a top-down picture of the computation
they were asked to perform.  However, Dask is able to easily represent far more
`complex algorithms`_ and expose the creation of these algorithms to normal users.


Conclusion
----------

-   Spark is mature and all-inclusive.  If you want a single project that does
    everything and you're already on Big Data hardware, then Spark is a safe bet,
    especially if your use cases are typical ETL + SQL and you're already using
    Scala.

-   Dask is lighter weight and is easier to integrate into existing code and hardware.
    If your problems vary beyond typical ETL + SQL and you want to add flexible
    parallelism to existing solutions, then Dask may be a good fit, especially if
    you are already using Python and associated libraries like NumPy and Pandas.

If you are looking to manage a terabyte or less of tabular CSV or JSON data,
then you should forget both Spark and Dask and use Postgres_ or MongoDB_.


.. _Spark: https://spark.apache.org/
.. _PySpark: https://spark.apache.org/docs/latest/api/python/
.. _Postgres: https://www.postgresql.org/
.. _MongoDB: https://www.mongodb.org/
.. _`complex algorithms`: http://matthewrocklin.com/blog/work/2015/06/26/Complex-Graphs
