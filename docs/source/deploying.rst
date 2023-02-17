Deploy Dask Clusters
====================

.. toctree::
   :maxdepth: 1
   :hidden:

   deploying-python.rst
   deploying-cli.rst
   deploying-ssh.rst
   deploying-docker.rst
   deploying-hpc.rst
   deploying-kubernetes.rst
   deploying-cloud.rst
   deploying-python-advanced.rst
   deployment-considerations.rst

The ``dask.distributed`` scheduler works well on a single machine and scales to many machines
in a cluster. We recommend using ``dask.distributed`` clusters at all scales for the following
reasons:

1.  It provides access to asynchronous APIs, notably :doc:`Futures <../../futures>`.
2.  It provides a diagnostic dashboard that can provide valuable insight on
    performance and progress (see :doc:`dashboard`).
3.  It handles data locality with sophistication, and so can be more
    efficient than the multiprocessing scheduler on workloads that require
    multiple processes.

This page describes various ways to set up Dask clusters on different hardware, either
locally on your own machine or on a distributed cluster.

You can continue reading or watch the screencast below:

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/TQM9zIBzNBo"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>

If you import Dask, set up a computation, and call ``compute``, then you
will use the single-machine scheduler by default.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses the single-machine scheduler by default

To use the ``dask.distributed`` scheduler you must set up a ``Client``.

.. code-block:: python

   from dask.distributed import Client
   client = Client(...)  # Connect to distributed cluster and override default
   df.x.sum().compute()  # This now runs on the distributed system

There are many ways to start the distributed scheduler and worker components, however, the most straight forward way is to use a *cluster manager* utility class.

.. code-block:: python

   from dask.distributed import Client, LocalCluster
   cluster = LocalCluster()  # Launches a scheduler and workers locally
   client = Client(cluster)  # Connect to distributed cluster and override default
   df.x.sum().compute()  # This now runs on the distributed system

These *cluster managers* deploy a scheduler
and the necessary workers as determined by communicating with the *resource manager*.
All *cluster managers* follow the same interface, but with platform-specific configuration
options, so you can switch from your local machine to a remote cluster without changing platforms.

.. figure:: images/dask-cluster-manager.svg
   :scale: 50%

   An overview of cluster management with Dask distributed.

`Dask Jobqueue <https://github.com/dask/dask-jobqueue>`_, for example, is a set of
*cluster managers* for HPC users and works with job queueing systems
(in this case, the *resource manager*) such as `PBS <https://en.wikipedia.org/wiki/Portable_Batch_System>`_,
`Slurm <https://en.wikipedia.org/wiki/Slurm_Workload_Manager>`_,
and `SGE <https://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_.
Those workers are then allocated physical hardware resources.

.. code-block:: python

   from dask.distributed import Client
   from dask_jobqueue import PBSCluster
   cluster = PBSCluster()  # Launches a scheduler and workers on HPC via PBS
   client = Client(cluster)  # Connect to distributed cluster and override default
   df.x.sum().compute()  # This now runs on the distributed system

.. _deployment-options:

The following resources explain how to set up Dask on a variety of local and distributed hardware.

.. _deployment-single-machine:

Single Machine
--------------

Dask runs perfectly well on a single machine with or without a distributed scheduler.
But once you start using Dask in anger you’ll find a lot of benefit both in terms of scaling
and debugging by using the distributed scheduler.

- :doc:`Default Scheduler <scheduling>`
   The no-setup default. Uses local threads or processes for larger-than-memory processing

- :doc:`dask.distributed <deploying-python>`
   The sophistication of the newer system on a single machine.  This provides more advanced features while still requiring almost no setup.

.. _deployment-distributed:

Distributed Computing
---------------------

There are a number of ways to run Dask on a distributed cluster (see the `Beginner's Guide to Configuring a Distributed Dask Cluster <https://blog.dask.org/2020/07/30/beginners-config>`_).

High Performance Computing
~~~~~~~~~~~~~~~~~~~~~~~~~~

See :doc:`deploying-hpc` for more details.

- `Dask-Jobqueue <https://jobqueue.dask.org>`_
    Provides cluster managers for PBS, SLURM, LSF, SGE and other resource managers.
- `Dask-MPI <http://mpi.dask.org/en/latest/>`_
    Deploy Dask from within an existing MPI environment.
- `Dask Gateway <https://gateway.dask.org/install-jobqueue.html>`_
    Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying HPC backend.

Kubernetes
~~~~~~~~~~

See :doc:`deploying-kubernetes` for more details.

- :doc:`Helm <deploying-kubernetes-helm>`
   An easy way to stand up a long-running Dask cluster.
- `Dask Kubernetes <https://kubernetes.dask.org/en/latest/>`_
   For native Kubernetes integration for fast moving or ephemeral deployments.
- `Dask Gateway <https://gateway.dask.org/install-kube.html>`_
    Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying Kubernetes backend.

Cloud
~~~~~
            
See :doc:`deploying-cloud` for more details.

- `Dask-Yarn <https://yarn.dask.org>`_
    Deploy Dask on YARN clusters, such as are found in traditional Hadoop installations.
- `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_
    Constructing and managing ephemeral Dask clusters on AWS, DigitalOcean, GCP, Azure, and Hertzner

Ad-hoc deployments
~~~~~~~~~~~~~~~~~~

- :doc:`Manual Setup <deploying-cli>`
    The command line interface to set up ``dask-scheduler`` and ``dask-worker`` processes.
- :doc:`deploying-ssh`
    Use SSH to set up Dask across an un-managed cluster.
- :doc:`Python API (advanced) <deploying-python-advanced>`
    Create ``Scheduler`` and ``Worker``   objects from Python as part of a distributed Tornado TCP application.

.. _managed-cluster-solutions:

Managed Solutions
~~~~~~~~~~~~~~~~~

- `Coiled <https://coiled.io/>`_
    Handles the creation and management of Dask clusters on cloud computing environments (AWS, Azure, and GCP).
- `Domino Data Lab <https://www.dominodatalab.com/>`_
    Lets users create Dask clusters in a hosted platform.
- `Saturn Cloud <https://saturncloud.io/>`_
    Lets users create Dask clusters in a hosted platform or within their own AWS accounts.
    
