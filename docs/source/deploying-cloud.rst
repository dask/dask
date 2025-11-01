Cloud
=====

There are a variety of ways to deploy Dask on the cloud.
Cloud providers offer managed services,
like VMs, Kubernetes, Yarn, or custom APIs with which Dask can connect easily.

.. grid:: 1 1 2 2

   .. grid-item::
      :columns: 7

      Some common deployment options you may want to consider are:

      -   A commercial Dask deployment option like `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying-cloud>`_
          to handle the creation and management of Dask clusters on AWS, GCP, and Azure.
      -   A managed Kubernetes service and Dask's
          :doc:`Kubernetes integration <deploying-kubernetes>`.
      -   Directly launching cloud resources such as VMs or containers via a cluster manager with
          `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_.
      -   A managed Yarn service,
          like `Amazon EMR <https://aws.amazon.com/emr/>`_
          or `Google Cloud DataProc <https://cloud.google.com/dataproc/>`_
          and `Dask-Yarn <https://yarn.dask.org>`_
          (specific documentation for the popular Amazon EMR service can be found
          `here <https://yarn.dask.org/en/latest/aws-emr.html>`_.)


   .. grid-item::
      :columns: 5
      :child-align: center

      .. figure:: images/cloud-provider-logos.svg

Cloud Deployment Examples
-------------------------

.. tab-set::

   .. tab-item:: Coiled

        `Coiled <https://coiled.io?utm_source=dask-docs&utm_medium=deploying-cloud>`_
        deploys managed Dask clusters on AWS, GCP, and Azure. It's free for most users and
        has several features that address common
        :doc:`deployment pain points <deployment-considerations>` like:

        - Easy to use API
        - Automatic software synchronization
        - Easy access to any cloud hardware (like GPUs) in any region
        - Robust logging, cost controls, and metrics collection

        .. code-block:: python

            >>> import coiled
            >>> cluster = coiled.Cluster(
            ...     n_workers=100,             # Size of cluster
            ...     region="us-west-2",        # Same region as data
            ...     vm_type="m6i.xlarge",      # Hardware of your choosing
            ... )
            >>> client = cluster.get_client()
        
        Coiled is recommended for deploying Dask on the cloud.
        Though there are non-commercial, open source options like
        Dask Cloud Provider, Dask-Gateway, and Dask-Yarn that are also available
        (see :ref:`cloud deployment options <cloud-deployment-options>`
        for additional options.)


   .. tab-item:: Dask Cloud Provider

        Using `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_ to launch a cluster of
        VMs on a platform like `DigitalOcean <https://www.digitalocean.com/>`_ can be as convenient as
        launching a local cluster.

        .. code-block:: python

            >>> import dask.config
            >>> dask.config.set({"cloudprovider.digitalocean.token": "yourAPItoken"})
            >>> from dask_cloudprovider.digitalocean import DropletCluster
            >>> cluster = DropletCluster(n_workers=1)
            Creating scheduler instance
            Created droplet dask-38b817c1-scheduler
            Waiting for scheduler to run
            Scheduler is running
            Creating worker instance
            Created droplet dask-38b817c1-worker-dc95260d

        Many of the cluster managers in Dask Cloud Provider work by launching VMs with a startup script
        that pulls down the :doc:`Dask Docker image <deploying-docker>` and runs Dask components within that container.
        As with all cluster managers the VM resources, Docker image, etc are all configurable.

        You can then connect a client and work with the cluster as if it were on your local machine.

        .. code-block:: python

            >>> client = cluster.get_client()

Data Access
-----------

In addition to deploying Dask clusters on the cloud, most cloud users will also want
to access cloud-hosted data on their respective cloud provider.

We recommend installing additional libraries (listed below) for easy data access on your cloud provider.
See :doc:`how-to/connect-to-remote-data` for more information.

.. tab-set::

   .. tab-item:: AWS

    Use `s3fs <https://s3fs.readthedocs.io/>`_ for accessing data on Amazon's S3.

    .. code-block:: bash

        pip install s3fs

   .. tab-item:: GCP

    Use `gcsfs <https://gcsfs.readthedocs.io/>`_ for accessing data on Google's GCS.

    .. code-block:: bash

        pip install gcsfs

   .. tab-item:: Azure

    Use `adlfs <https://github.com/dask/adlfs/>`_ for accessing data on Microsoft's Data Lake or Blob Storage.

    .. code-block:: bash

        pip install adlfs
