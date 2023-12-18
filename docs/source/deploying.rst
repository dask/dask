Deploy Dask Clusters
====================

.. grid:: 1 1 2 2

   .. grid-item::
      :columns: 12 12 5 5

      Dask works well at many scales ranging from a single machine to clusters of
      many machines.  This section describes the many ways to deploy and run Dask,
      including the following:

      .. toctree::
         :maxdepth: 1

         deploying-python.rst
         deploying-cli.rst
         deploying-ssh.rst
         deploying-hpc.rst
         deploying-kubernetes.rst
         deploying-cloud.rst
         deploying-extra.rst

   .. grid-item::
      :columns: 12 12 7 7

      .. figure:: images/dask-cluster-manager.svg

         An overview of cluster management with Dask distributed.

Single Machine
--------------

If you import Dask, set up a computation, and call ``compute``, then you
will use the the local threaded scheduler by default.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses threads in your local process by default

Alternatively, you can set up a fully-featured Dask cluster on your local
machine.  This gives you access to multi-process computation and diagnostic
dashboards.

.. code-block:: python

   from dask.distributed import LocalCluster
   cluster = LocalCluster()
   client = cluster.get_client()

   # Dask works as normal and leverages the infrastructure defined above
   df.x.sum().compute()

The ``LocalCluster`` cluster manager defined above is easy to use and works
well on a single machine.  It follows the same interface as all other Dask
cluster managers, and so it's easy to swap out when you're ready to scale up.

.. code-block:: python

   # You can swap out LocalCluster for other cluster types

   from dask.distributed import LocalCluster
   from dask_kubernetes import KubeCluster

   # cluster = LocalCluster()
   cluster = KubeCluster()  # example, you can swap out for Kubernetes

   client = cluster.get_client()

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

Manual deployments (not recommended)
------------------------------------

You can set up Dask clusters by hand, or with tools like SSH.

- :doc:`Manual Setup <deploying-cli>`
    The command line interface to set up ``dask-scheduler`` and ``dask-worker`` processes.
- :doc:`deploying-ssh`
    Use SSH to set up Dask across an un-managed cluster.
- :doc:`Python API (advanced) <deploying-python-advanced>`
    Create ``Scheduler`` and ``Worker``   objects from Python as part of a distributed Tornado TCP application.

However, we don't recommend this path.  Instead, we recommend that you use
some common resource manager to help you manage your machines, and then deploy
Dask on that system.  Those options are described below.

High Performance Computing
--------------------------

See :doc:`deploying-hpc` for more details.

- `Dask-Jobqueue <https://jobqueue.dask.org>`_
    Provides cluster managers for PBS, SLURM, LSF, SGE and other resource managers.
- `Dask-MPI <http://mpi.dask.org/en/latest/>`_
    Deploy Dask from within an existing MPI environment.
- `Dask Gateway for Jobqueue <https://gateway.dask.org/install-jobqueue.html>`_
    Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying HPC backend.

Kubernetes
----------

See :doc:`deploying-kubernetes` for more details.

- `Dask Kubernetes Operator <https://kubernetes.dask.org/en/latest/operator.html>`_
    For native Kubernetes integration for fast moving or ephemeral deployments.
- `Dask Gateway for Kubernetes <https://gateway.dask.org/install-kube.html>`_
    Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying Kubernetes backend.
- `Single Cluster Helm Chart <https://artifacthub.io/packages/helm/dask/dask>`_
    Single Dask cluster and (optionally) Jupyter on deployed with Helm.

Cloud
-----

See :doc:`deploying-cloud` for more details.

- `Dask-Yarn <https://yarn.dask.org>`_
    Deploy Dask on YARN clusters, such as are found in traditional Hadoop installations.
- `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_
    Constructing and managing ephemeral Dask clusters on AWS, DigitalOcean, Google Cloud, Azure, and Hetzner
- `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying>`_
    Commercial Dask deployment option, which handles the creation and management of Dask clusters on cloud computing environments (AWS and GCP).

.. _managed-cluster-solutions:

Managed Solutions
-----------------

- `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying>`_ manages the creation and management of Dask clusters on cloud computing environments (AWS and GCP).
- `Domino Data Lab <https://www.dominodatalab.com/>`_ lets users create Dask clusters in a hosted platform.
- `Saturn Cloud <https://saturncloud.io/>`_ lets users create Dask clusters in a hosted platform or within their own AWS accounts.

Advanced Understanding
----------------------

There are additional concepts to understand if you want to improve your
deployment.
