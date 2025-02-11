Deploy Dask Clusters
====================

.. grid:: 1 1 2 2

   .. grid-item::
      :columns: 12 12 5 5

      Dask works well at many scales ranging from a single machine to clusters of
      many machines.  This page describes the many ways to deploy and run Dask, including the following:

      - :doc:`deploying-python`
      - :doc:`deploying-cloud`
      - :doc:`deploying-hpc`
      - :doc:`deploying-kubernetes`

      .. toctree::
         :maxdepth: 1
         :hidden:

         deploying-python.rst
         deploying-cloud.rst
         deploying-hpc.rst
         deploying-kubernetes.rst
         deploying-cli.rst
         deploying-ssh.rst
         deploying-extra.rst

   .. grid-item::
      :columns: 12 12 7 7

      .. figure:: images/dask-cluster-manager.svg

         An overview of cluster management with Dask distributed.

.. _deployment-single-machine:

Local Machine
-------------

You can run Dask without any setup.  Dask will use threads
on your local machine by default.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses threads on your local machine

Alternatively, you can set up a fully-featured multi-process Dask cluster on
your local machine.  This gives you access to multi-process computation and
diagnostic dashboards.

.. code-block:: python

   from dask.distributed import LocalCluster
   cluster = LocalCluster()          # Fully-featured local Dask cluster
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

.. _cloud-deployment-options:

Cloud
-----

Deploying on commercial cloud like AWS, GCP, or Azure is convenient because you can quickly scale out to many machines for just a few minutes, but also challenging because you need to navigate awkward cloud APIs, manage remote software environments with Docker, send data access credentials, make sure that costly resources are cleaned up, etc.  The following solutions help with this process.

-   |Coiled|_: this commercial SaaS product handles most of the deployment pain Dask users
    encounter, is easy to use, and quite robust.  The free tier is large enough
    for most individual users, even for those who don't want to engage with a
    commercial company.  The API looks like the following.

    .. code-block:: python

       import coiled
       cluster = coiled.Cluster(
           n_workers=100,
           region="us-east-2",
           worker_memory="16 GiB",
           spot_policy="spot_with_fallback",
       )
       client = cluster.get_client()

- `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_: a pure and simple OSS solution that sets up Dask workers on cloud VMs, supporting AWS, GCP, Azure, and also other commercial clouds like Hetzner, Digital Ocean and Nebius.

- `Dask-Yarn <https://yarn.dask.org>`_: deploys Dask on legacy YARN clusters, such as can be set up with AWS EMR or Google Cloud Dataproc.

See :doc:`deploying-cloud` for more details.

.. _Coiled: https://docs.coiled.io/user_guide/index.html?utm_source=dask-docs&utm_medium=deploying
.. |Coiled| replace:: **Coiled (recommended)**


High Performance Computing
--------------------------

Dask runs on traditional HPC systems that use a resource manager like SLURM,
PBS, SGE, LSF, or similar systems, and a network file system.  This is an easy
way to dual-purpose large-scale hardware for analytics use cases.  Dask can
deploy either directly through the resource manager or through
``mpirun``/``mpiexec`` and tends to use the NFS to distribute data and
software.

-   |Dask-Jobqueue|_: interfaces directly with the
    resource manager (SLURM, PBS, SGE, LSF, and others) to launch many Dask
    workers as batch jobs.  It generates batch job scripts and submits them
    automatically to the user's queue.  This approach operates entirely with user
    permissions (no IT support required) and enables interactive and adaptive use
    on large HPC systems.  It looks a little like the following:

    .. code-block:: python

       from dask_jobqueue import PBSCluster
       cluster = PBSCluster(
           cores=24,
           memory="100GB",
           queue="regular",
           account="my-account",
       )
       cluster.scale(jobs=100)
       client = cluster.get_client()

- `Dask-MPI <http://mpi.dask.org/en/latest/>`_: deploys Dask on top of any system that supports MPI using ``mpirun``.  It is helpful for batch processing jobs where you want to ensure a fixed and stable number of workers.
- `Dask Gateway for Jobqueue <https://gateway.dask.org/install-jobqueue.html>`_: Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying HPC backend.

See :doc:`deploying-hpc` for more details.

.. _Dask-Jobqueue: https://jobqueue.dask.org
.. |Dask-Jobqueue| replace:: **Dask-Jobqueue (recommended)**

Kubernetes
----------

Dask runs natively on Kubernetes clusters.  This is a convenient choice when a
company already has dedicated Kubernetes infrastructure set up for running
other services.  When running Dask on Kubernetes users should also have a plan
to distribute software environments (probably with Docker), user credentials,
quota management, etc. In larger organizations with mature Kubernetes
deployments this is often handled by other Kubernetes services.

-   |Dask-Kubernetes|_: The Dask Kubernetes
    Operator makes the most sense for fast moving or ephemeral deployments.  It
    is the most Kubernetes-native solution, and should be comfortable for K8s
    enthusiasts.  It looks a little like this:

    .. code-block:: python

       from dask_kubernetes.operator import KubeCluster
       cluster = KubeCluster(
           name="my-dask-cluster",
           image="ghcr.io/dask/dask:latest",
           resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}},
       )
       cluster.scale(10)
       client = cluster.get_client()

- `Dask Gateway for Kubernetes <https://gateway.dask.org/install-kube.html>`_: Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying Kubernetes backend.
- `Single Cluster Helm Chart <https://artifacthub.io/packages/helm/dask/dask>`_: Single Dask cluster and (optionally) Jupyter on deployed with Helm.

See :doc:`deploying-kubernetes` for more details.

.. _Dask-Kubernetes: https://kubernetes.dask.org/en/latest/operator.html
.. |Dask-Kubernetes| replace:: **Dask Kubernetes Operator (recommended)**

.. _managed-cluster-solutions:

Manual deployments (not recommended)
------------------------------------

You can set up Dask clusters by hand, or with tools like SSH.

- :doc:`Manual Setup <deploying-cli>`: The command line interface to set up ``dask-scheduler`` and ``dask-worker`` processes.
- :doc:`deploying-ssh`: Use SSH to set up Dask across an un-managed cluster.
- :doc:`Python API (advanced) <deploying-python-advanced>`: Create ``Scheduler`` and ``Worker``   objects from Python as part of a distributed Tornado TCP application.

However, we don't recommend this path.  Instead, we recommend that you use
some common resource manager to help you manage your machines, and then deploy
Dask on that system.  Those options are described above.

Advanced Understanding
----------------------

There are additional concepts to understand if you want to improve your
deployment. :doc:`This guide <deployment-considerations>` covers the main topics to consider in addition to running Dask.
