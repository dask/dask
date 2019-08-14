Institutional FAQ
=================

**Question**: *Is appropriate for adoption within a larger institutional context?*

**Answer**: *Yes.* Dask is used within the world's largest banks, national labs,
retailers, technology companies, and government agencies.  It is used in highly
secure environments.  It is used in conservative institutions as well as fast
moving ones.

This page contains Frequently Asked Questions and concerns from institutions
when they first investigate Dask.

.. contents:: :local:

For Management
--------------

Briefly, what problem does Dask solve for us?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask is a general purpose parallel programming solution.
As such it is used in *many* different ways.

However, the most common problem that Dask solves is connecting Python analysts
to distributed hardware, particularly for data science and machine learning
workloads.  The institutions for whom Dask has the greatest
impact are those who have a large body of Python users who are accustomed to
libraries like NumPy, Pandas, Jupyter, Scikit-Learn and others, but want to
scale those workloads across a cluster.  Often they also have distributed
computing resources that are going underused.

Dask removes both technological and cultural barriers to connect Python users
to computing resources in a way that is native to both the users and IT.

"*Help me scale my notebook onto the cluster*" is a common pain point for
institutions today, and it is a common entry point for Dask usage.


Is Dask Mature?  Why should we trust it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes.  While Dask itself is relatively new (it began in 2015) it is built by the
NumPy, Pandas, Jupyter, Scikit-Learn developer community, which is well trusted.
Dask is a relatively thin wrapper on top of these libraries and,
as a result, the project can be relatively small and simple.
It doesn't reinvent a whole new system.

Additionally, this tight integration with the broader technology stack
gives substantial benefits long term.  For example:

-   Because Pandas maintainers also maintain Dask,
    when Pandas issues a new releases Dask issues a release at the same time to
    ensure continuity and compatibility.

-   Because Scikit-Learn maintainers maintain and use Dask when they train on large clusters,
    you can be assured that Dask-ML focuses on pragmatic and important
    solutions like XGBoost integration, and hyper-parameter selection,
    and that the integration between the two feels natural for novice and
    expert users alike.

-   Because Jupyter maintainers also maintain Dask,
    powerful Jupyter technologies like JupyterHub and JupyterLab are designed
    with Dask's needs in mind, and new features are pushed quickly to provide a
    first class and modern user experience.

Additionally, Dask is maintained both by a broad community of maintainers,
as well as substantial institutional support (several full-time employees each)
by both Anaconda, the company behind the leading data science distribution, and
NVIDIA, the leading hardware manufacturer of GPUs.  Despite large corporate
support, Dask remains a community governed project, and is fiscally sponsored by
NumFOCUS, the same 501c3 that fiscally sponsors NumPy, Pandas, Jupyter, and many others.


Who else uses Dask?
~~~~~~~~~~~~~~~~~~~

Dask is used by individual researchers in practically every field today.  It
has millions of downloads per month, and is integrated into many PyData
software packages today.

On an *institutional* level Dask is used by analytics and research groups in a
similarly broad set of domains across both energetic startups as well as large
conservative household names.  A web search shows articles by Capital One,
Barclays, Walmart, NASA, Los Alamos National Laboratories, and hundreds of
other similar institutions.


How does Dask compare with Apache Spark?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*This question has longer and more technical coverage* :doc:`here <spark>`

Dask and Apache Spark are similar in that they both ...

-  Promise easy parallelism for data science Python users
-  Provide Dataframe and ML APIs for ETL, data science, and machine learning
-  Scale out to similar scales, around 1-1000 machines

Dask differs from Apache Spark in a few ways:

-  Dask is more Python native, Spark is Scala/JVM native with Python bindings.

   Python users may find Dask more comfortable,
   but Dask is only useful for Python users,
   while Spark can also be used from JVM languages.

-  Dask is one component in the broader Python ecosystem alongside libraries
   like Numpy, Pandas, and Scikit-Learn,
   while Spark is an all-in-one system that re-invents much of the Python world
   in a single package.

   This means that it's often easier to compose Dask with new problem domains,
   but also that you need to install multiple things (like Dask and Pandas or
   Dask and Numpy) rather than just having everything in an all-in-one solution.

-  Apache Spark focuses strongly on traditional business intelligence workloads,
   like ETL, SQL queries, and then some lightweight machine learning,
   while Dask is more general purpose.

   This means that Dask is much more flexible and can handle other problem
   domains like multi-dimensional arrays, GIS, advanced machine learning, and
   custom systems, but that it is less focused and less tuned on typical SQL
   style computations.

   If you mostly want to focus on SQL queries then Spark is probably a better
   bet.  If you want to support a wide variety of custom workloads then Dask
   might be more natural.


For IT
------


How would I set up Dask on institutional hardware?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You already have cluster resources.
Dask can run on them today without significant change.

Most institutional clusters today have a resource manager.
This is typically managed by IT, with some mild permissions given to users to
launch jobs.  Dask works with all major resource managers today, including
those on Hadoop, HPC, Kubernetes, and Cloud clusters.

1.  **Hadoop/Spark**: If you have a Hadoop/Spark cluster, such as one purchased
    through Cloudera/Hortonworks/MapR then you will likely want to deploy Dask
    with YARN, the resource manager that deploys services like Hadoop, Spark,
    Hive, and others.

    To help with this, you'll likely want to use `Dask-Yarn <https://yarn.dask.org>`_.

2.  **HPC**: If you have an HPC machine that runs resource managers like SGE,
    SLLURM, PBS, LSF, Torque, Condor, or other job batch queuing systems, then
    users can launch Dask on these systems today using either:

    -  `Dask Jobqueue <https://jobqueue.dask.org>`_ , which uses typical
      ``qsub``, ``sbatch``, ``bsub`` or other submission tools in interactive
      settings.
    -  `Dask MPI <https://mpi.dask.org>`_ which uses MPI for deployment in
      batch settings

    For more information see :doc:`setup/hpc`

3.  **Kubernetes/Cloud**: Newer clusters may employ Kubernetes for deployment.
    This is particularly commonly used today on major cloud providers,
    all of which provide hosted Kubernetes as a service.  People today use Dask
    on Kubernetes using either of the following:

    -  **Helm**: an easy way to stand up a long-running Dask cluster and
      Jupyter notebook

    -  **Dask-Kubernetes**: for native Kubernetes integration for fast moving
      or ephemeral deployments.

    For more information see :doc:`setup/kubernetes`


Is Dask Secure?
~~~~~~~~~~~~~~~

Dask is deployed today within highly secure institutions,
including major financial, healthcare, and government agencies.

That being said it's worth noting that, by it's very nature, Dask enables the
execution of arbitrary user code on a large set of machines. Care should be
taken to isolate, authenticate, and govern access to these machines.  Fortunately,
your institution likely already does this and uses standard technologies like
SSL/TLS, Kerberos, and other systems with which Dask can integrate.


Do I need to purchase a new Cluster?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No.  It is easy to run Dask today on most clusters.
If you have a pre-existing HPC or Spark/Hadoop cluster then that will be fine
to start running Dask.

You can start using Dask without any capital expenditure.


How do I manage users?
~~~~~~~~~~~~~~~~~~~~~~

Dask doesn't manage users, you likely have existing systems that do this well.
In a large institutional setting we assume that you already have a resource
manager like Yarn (Hadoop), Kubernetes, or PBS/SLURM/SGE/LSF/..., each of which
have excellent user management capabilities, which are likely preferred by your
IT department anyway.

Dask is designed to operate with user-level permissions, which means that
your data science users should be able to ask those systems mentioned above for
resources, and have their processes tracked accordingly.

However, there are institutions where analyst-level users aren't given direct access to
the cluster.  This is particularly common in Cloudera/Hortonworks Hadoop/Spark deployments.
In these cases some level of explicit indirection may be required.  For this, we
recommend the `Dask Gateway project <https://gateway.dask.org>`_, which uses IT-level
permissions to properly route authenticated users into secure resources.


How do I manage software environments?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This depends on your cluster resource manager:

-  Most HPC users use their network file system
-  Hadoop/Spark/Yarn users package their environment into a tarball and ship it
   around with HDFS (Dask-Yarn integrates with `Conda Pack
   <https://conda.github.io/conda-pack/>`_ for this capability)
-  Kubernetes or Cloud users use Docker images

In each case Dask integrates with existing processes and technologies
that are well understood and familiar to the institution.


How Does Dask communicate data between machines?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask usually communicates over TCP, using msgpack for small administrative
messages, and its own protocol for efficiently passing around large data.
The scheduler and each worker host their own TCP server, making Dask a
distributed peer-to-peer network that uses point-to-point communication.
We do not use Spark-style shuffle systems.  We do not use MPI-style
collectives.  Everything is direct point-to-point.

For high performance networks you can use either TCP-over-Infiniband for about
1 GB/s bandwidth, or UCX (experimental) for full speed communication.


Are deployments long running, or ephemeral?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We see both, but ephemeral deployments are more common.

Most Dask use today is about enabling data science or data engineering users to
scale their interactive workloads across the cluster.
These are typically either interactive sessions with Jupyter, or batch scripts
that run at a pre-defined time.  In both cases, the user asks the resource
manager for a bunch of machines, does some work, and then gives up those
machines.

Some institutions also use Dask in an always-on fashion, either handling
real-time traffic in a scalable way, or responding to a broad set of
interactive users with large datasets that it keeps resident in memory.


For Technical Leads
-------------------

Will Dask "just work" on our existing code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No, you will need to make modifications,
but these modifications are usually small.

The vast majority of lines of business logic within your institution
will not have to change, assuming that they are in Python and use tooling like
Numpy, Pandas and Scikit-Learn.

How well does Dask scale?  What are Dask's limitations?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The largest Dask deployments that we see today are on around 1000 multi-core
machines, perhaps 20,000 cores in total, but these are rare.
Most institutional-level problems (1-100 TB) are well solved by deployments of 10-50 nodes.

Technically, the back-of-the-envelope number to keep in mind is that each task
(an individual Python function call) in Dask has an overhead of around *200
microseconds*.  So if these tasks take 1 second each, then Dask can saturate
around 5000 cores before scheduling overhead dominates costs.  As workloads
reach this limit they are encouraged to use larger chunk sizes to compensate.
The *vast majority* of institutional users though do not reach this limit.
For more information you may want to peruse our :doc:`best practices
<best-practices>`

Is Dask Resilient?  What happens when a machine goes down?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, Dask is resilient to the failure of worker nodes.  It knows how it came to
any result, and can replay the necessary work on other machines if one goes
down.

If Dask's centralized scheduler goes down then you would need to resubmit the
computation.  This is a fairly standard level of resiliency today, shared with
other tooling like Apache Spark, Flink, and others.

The resource managers that host Dask, like Yarn or Kubernetes, typically
provide long-term 24/7 resilience for always-on operation.

Is the API exactly the same as NumPy/Pandas/Scikit-Learn?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No, but it's very close.  That being said your data scientists will still
have to learn some things.

What we find is that the Numpy/Pandas/Scikit-Learn APIs aren't the challenge
when institutions adopt Dask.  When API consistencies do exist, even
modestly skilled programmers are able to understand why and work around them
without much pain.

Instead, the challenge is building intuition around parallel performance.
We've all built up a mental model for what is fast and slow on a single
machine.  This model changes when we factor in network communication and
parallel algorithms, and the performance that we get for familiar operations
can be surprising.

Our main solution to build this intuition, other than
accumulated experience, is Dask's :doc:`Diagnostic Dashboard
<diagnostics-distributed>`.
The dashboard delivers a ton of visual feedback to users as they are running
their computation to help them understand what is going on.  This both helps
them to identify and resolve immediate bottlenecks, and also builds up that
parallel performance intuition suprisingly quickly.


How much performance tuning does Dask require?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Some other systems are notoriously hard to tune for optimal performance.
What is Dask's story here?  How many knobs are there that we need to be aware
of?*

Like the rest of the Python software tools, Dask puts a lot of effort into
having sane defaults.  Dask workers automatically detect available memory and
cores, and choose sensible defaults that are decent in most situations.  Dask
algorithms similarly provide decent choices by default, and informative warnings
when tricky situations arise, so that, in common cases, things should be fine.

The most common knobs to tune include the following:

-   The thread/process mixture to deal with GIL-holding computations (which are
    rare in Numpy/Pandas/Scikit-Learn workflows)
-   Partition size, like if should you have 100 MB chunks or 1 GB chunks

That being said, almost no institution's needs are met entirely by the common
case, and given the variety of problems that people throw at Dask,
exceptional problems are commonplace.
In these cases we recommend watching the dashboard during execution to see what
is going on.  It can commonly inform you what's going wrong, so that you can
make changes to your system.


What Data formats does Dask support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because Dask builds on NumPy and Pandas, it supports most formats that they
support, which is most formats.
That being said, not all formats are well suited for
parallel access.  In general people using the following formats are usually
pretty happy:

-  **Tabular:** Parquet, ORC, CSV, Line Delimited JSON, Avro, text
-  **Arrays:** HDF5, NetCDF, Zarr, GRIB

More generally, if you have a Python function that turns a chunk of your stored
data into a Pandas dataframe or Numpy array then Dask can probably call that
function many times without much effort.

For groups looking for advice on which formats to use, we recommend Parquet
for tables and Zarr or HDF5 for arrays.


Does Dask have a SQL interface?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Dask provides no SQL support.  Dask dataframe looks like and uses Pandas
for these sorts of operations.  It would be great to see someone build a SQL
interface on top of Pandas, which Dask could then use, but this is out of scope
for the core Dask project itself.

As with Pandas though, we do support a ``dask.dataframe.from_sql`` command for
efficiently pulling data out of SQL databases for Pandas computations.
