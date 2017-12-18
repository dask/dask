Setup
=====

This page describes various ways to set up Dask on different hardware, either
locally on your own machine or on a distributed cluster.  If you are just
getting started then this page is unnecessary.  Dask does not require any setup
if you only want to use it on a single computer.

Dask has two families of task schedulers:

1.  **Single machine scheduler**: This scheudler a local process or thread
    pool.  This scheduler was made first and is the default.  It can only be
    used on a single machine and does not scale.
2.  **Distributed scheduler**: This scheduler is more sophisticated.  It can
    run locally or distributed across a cluster.  It is more heavy-weight but
    also has many more features.  It can be used effectively either on a single
    machine or on a distributed cluster

If you import Dask, set up a computation, and then call ``compute`` then you
will use the single-machine scheduler by default.  To use the dask.distributed
scheduler you must set up a ``Client``

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses the single-machine scheduler by default

.. code-block:: python

   from dask.distributed import Client
   client = Client(...)  # Connect to distributed cluster and override default
   df.x.sum().compute()  # This now runs on the distributed system

Note that the ``dask.distributed`` scheduler is often preferble even on single
workstations.  It contains many diagnostics and features not found in the
single-machine scheduler.

- Single Machine:
    - :doc:`Default Scheduler <setup/single-machine>`: The no-setup default.
      Local threads or processes for larger-than-memory processing

    - :doc:`Dask.distributed <setup/single-distributed>`: The sophistication of
      the distributed system on a single machine.  This provides more advanced
      features while still requiring almost no setup.
-  Distributed Computing:
    - :doc:`Manual Setup <setup/cli>`: The command line interface to set up
      ``dsak-scheduler`` and ``dask-worker`` processes.  Useful for IT or
      anyone building a deployment solution.
    - :doc:`SSH <setup/ssh>`: Use SSH to set up Dask across an un-managed
      cluster
    - :doc:`High Performance Computers <setup/hpc>`: How to run Dask on
      traditional HPC environments using tools like MPI, or job schedulers like
      SLURM, SGE, TORQUE, LSF, and so on
    - :doc:`Kubernetes <setup/kubernetes>`: Deploy Dask on the popular
      Kubernetes resource manager.  This is particularly useful for any cloud
      deployments on Google, Amazon, or Microsoft Azure.
    - :doc:`Python API (advanced) <setup/python-advanced>`: Create
      ``Scheduler`` and ``Worker`` objects from Python as part of a distributed
      Tornado TCP application.  This page is useful for those building custom
      frameworks.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   setup/single-machine.rst
   setup/single-distributed.rst
   setup/cli.rst
   setup/ssh.rst
   setup/hpc.rst
   setup/kubernetes.rst
   setup/python-advanced.rst
