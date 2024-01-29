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

You don't need to do any setup to run Dask. Dask will use threads
on your local machine by default.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv(...)
   df.x.sum().compute()  # This uses threads on your local machine

Alternatively, you can set up a fully-featured Dask cluster on your local
machine.  This gives you access to multi-process computation and diagnostic
dashboards.

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
|Coiled|_ **is recommended for deploying Dask on the cloud.**
Though there are other options you may consider depending on your specific needs:

- `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying>`_: Commercial Dask deployment option, which handles the creation and management of Dask clusters on cloud computing environments (AWS, GCP, and Azure).
- `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_: Constructing and managing ephemeral Dask clusters on AWS, DigitalOcean, Google Cloud, Azure, and Hetzner.
- `Dask-Yarn <https://yarn.dask.org>`_: Deploy Dask on YARN clusters, such as are found in traditional Hadoop installations.

See :doc:`deploying-cloud` for more details.

.. _Coiled: https://coiled.io?utm_source=dask-docs&utm_medium=deploying
.. |Coiled| replace:: **Coiled** 


High Performance Computing
--------------------------
|Dask-Jobqueue|_ **is recommended for deploying Dask on HPC systems.**
Though there are other options you may consider depending on your specific needs:

- `Dask-Jobqueue <https://jobqueue.dask.org>`_: Provides cluster managers for PBS, SLURM, LSF, SGE and other resource managers.
- `Dask-MPI <http://mpi.dask.org/en/latest/>`_: Deploy Dask from within an existing MPI environment.
- `Dask Gateway for Jobqueue <https://gateway.dask.org/install-jobqueue.html>`_: Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying HPC backend.

See :doc:`deploying-hpc` for more details.

.. _Dask-Jobqueue: https://jobqueue.dask.org
.. |Dask-Jobqueue| replace:: **Dask-Jobqueue** 

Kubernetes
----------
|Dask-Kubernetes|_ **is recommended for deploying Dask on Kubernetes.**
Though there are other options you may consider depending on your specific needs:

- `Dask Kubernetes Operator <https://kubernetes.dask.org/en/latest/operator.html>`_: For native Kubernetes integration for fast moving or ephemeral deployments.
- `Dask Gateway for Kubernetes <https://gateway.dask.org/install-kube.html>`_: Multi-tenant, secure clusters. Once configured, users can launch clusters without direct access to the underlying Kubernetes backend.
- `Single Cluster Helm Chart <https://artifacthub.io/packages/helm/dask/dask>`_: Single Dask cluster and (optionally) Jupyter on deployed with Helm.

See :doc:`deploying-kubernetes` for more details.

.. _Dask-Kubernetes: https://kubernetes.dask.org/en/latest/operator.html
.. |Dask-Kubernetes| replace:: **Dask Kubernetes Operator** 

.. _managed-cluster-solutions:

Managed Solutions
-----------------
|Coiled|_ **is recommended for deploying managed Dask clusters.**
Though there are other options you may consider depending on your specific needs:

- `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying>`_: Manages the creation and management of Dask clusters on cloud computing environments (AWS, GCP, and Azure).
- `Domino Data Lab <https://www.dominodatalab.com/>`_: Lets users create Dask clusters in a hosted platform.
- `Saturn Cloud <https://saturncloud.io/>`_: Lets users create Dask clusters in a hosted platform or within their own AWS accounts.


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
