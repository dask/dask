Institutional FAQ
=================

Dask is great for accelerating individual data scientists using Python,
is it also appropriate for adoption within a larger institutional context?

**Yes.**  It is.  Dask is used within the world's largest banks, national labs,
retailers, technology companies, and government agencies.  It is used in highly
secure environments.  It is used in conservative institutions as well as fast
moving ones.

This page contains Frequently Asked Questions and concerns from institutions
when they first investigate Dask.


Briefly, what problem does Dask solve for us?
---------------------------------------------

Dask is a general purpose parallel programming solution.
As such it is used in *many* different ways.

However, the most common problem that Dask solves is connecting Python analysts
to distributed hardware.  The institutions for whom Dask has the greatest
impact are those who have a large body of Python users who are accustomed to
libraries like NumPy, Pandas, Jupyter, Scikit-Learn and others, but want to
scale those workloads onto a cluster.

"Help me scale my notebook onto the cluster" is a common pain point for
institutions today, and it is a common entry point for Dask usage.


How would I set up Dask on institutional hardware?
--------------------------------------------------

You probably already have cluster resources.
Dask almost certainly can run on them today with only user permissions.

Most institutional clusters today have a resource manager.
This is typically managed by IT, with some mild permissions given to users to
launch jobs.  Dask works with all major resource managers today, including
those on Hadoop, HPC, Kubernetes, and Cloud clusters.

1.  **Hadoop/Spark**: If you have a Hadoop/Spark cluster, such as one purchased
    through Cloudera/Hortonworks/MapR then you will likely want to deploy Dask
    with YARN, the resource manager that deploys services like Hadoop, Spark,
    Hive, and others.

    To help with this, you'll likely want to use [Dask-Yarn](https://yarn.dask.org).

    For more information see :doc:`setup/yarn`

2.  **HPC**: If you have an HPC machine that runs resource managers like SGE,
    SLLURM, PBS, LSF, Torque, Condor, or other job batchqueuing systems, then
    users can launch Dask on these systems today using either:

    -  `Dask Jobqueue <https://jobqueue.dask.org>`_ , which uses typical
      ``qsub``, ``sbatch``, ``bsub`` or other submission tools in interactive
      settings.
    -  `Dask MPI <https://mpi.dask.org>`_ which uses MPI for deployment in
      batch settings

    For more information see :doc:`setup/hpc`

3.  **Kubernetes**: Newer clusters may employ Kubernetes for deployment.

    For more information see :doc:`setup/kubernetes`

4.  **Cloud**: TODO


How do I manage users?
----------------------

How do I manage software environments?
--------------------------------------

Is Dask Secure?
---------------

Should I deploy a single Dask cluster or many small ones?
---------------------------------------------------------

Why would I use Dask instead of Spark?
--------------------------------------

You should use both.  Some of your users will prefer one over the other.
Fortunately, there is no need to choose.  They both run on the same cluster
infrastructure, and understand many of the same data formats.



Do I need to buy a new Cluster?
-------------------------------

Is Dask Resilient?  What happens when a machine goes down?
----------------------------------------------------------

Is Dask Mature?
---------------

Who else uses Dask?
-------------------

How well does Dask scale?  What are it's limitations?
-----------------------------------------------------

Will Dask "just work" on our existing code?
-------------------------------------------
