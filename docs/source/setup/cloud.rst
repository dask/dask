Cloud Deployments
=================

There are a variety of ways to deploy Dask on cloud providers.
Cloud providers provide managed services,
like Kubernetes, Yarn, or custom APIs with which Dask can connect easily.
You may want to consider the following options:

1.  A managed Kubernetes service and Dask's
    :doc:`Kubernetes and Helm integration <kubernetes-helm>`.
2.  A managed Yarn service,
    like `Amazon EMR <https://aws.amazon.com/emr/>`_
    or `Google Cloud DataProc <https://cloud.google.com/dataproc/>`_
    and `Dask-Yarn <https://yarn.dask.org>`_.

    Specific documentation for the popular Amazon EMR service can be found
    `here <https://yarn.dask.org/en/latest/aws-emr.html>`_
3.  Vendor specific services, like Amazon ECS, and
    `Dask Cloud Provider <https://cloudprovider.dask.org/en/latest/>`_

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
:doc:`Kubernetes and Helm <kubernetes-helm>` solution.
