Cloud
=====

There are a variety of ways to deploy Dask on cloud providers.
Cloud providers provide managed services,
like VMs, Kubernetes, Yarn, or custom APIs with which Dask can connect easily.
You may want to consider the following options:

1.  A managed Kubernetes service and Dask's
    :doc:`Kubernetes integration <deploying-kubernetes>`.
2.  A managed Yarn service,
    like `Amazon EMR <https://aws.amazon.com/emr/>`_
    or `Google Cloud DataProc <https://cloud.google.com/dataproc/>`_
    and `Dask-Yarn <https://yarn.dask.org>`_.

    Specific documentation for the popular Amazon EMR service can be found
    `here <https://yarn.dask.org/en/latest/aws-emr.html>`_
3.  Directly launching cloud resources such as VMs or containers via a cluster manager with
    `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_

Cloud Deployment Example
------------------------

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

    >>> from dask.distributed import Client

    >>> client = Client(cluster)

Data Access
-----------

You may want to install additional libraries in your Jupyter and worker images
to access the object stores of each cloud:

-  `s3fs <https://s3fs.readthedocs.io/>`_ for Amazon's S3
-  `gcsfs <https://gcsfs.readthedocs.io/>`_ for Google's GCS
-  `adlfs <https://github.com/dask/adlfs/>`_ for Microsoft's ADL

Historical Libraries
--------------------

Dask previously maintained libraries for deploying Dask on
Amazon's EC2 and Google GKE.
Due to sporadic interest,
and churn both within the Dask library and EC2 itself,
these were not well maintained.
They have since been deprecated in favor of the
:doc:`Kubernetes and Helm <deploying-kubernetes-helm>` solution.
