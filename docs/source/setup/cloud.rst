Cloud Deployments
=================

To get started running Dask on common Cloud providers like Amazon,
Google, or Microsoft, we currently recommend deploying
:doc:`Dask with Kubernetes and Helm <kubernetes-helm>`.

All three major cloud vendors now provide managed Kubernetes services.
This allows us to reliably provide the same experience across all clouds,
and ensures that solutions for any one provider remain up-to-date.

Alternatively, if you are deploying on a cloud-hosted Hadoop cluster like
`Amazon EMR <https://aws.amazon.com/emr/>`_ or `Google Cloud DataProc
<https://cloud.google.com/dataproc/>`_, you will want to use `Dask-Yarn
<https://yarn.dask.org/>`_. Documentation on deploying on Amazon EMR
specifically can be found `here
<https://yarn.dask.org/en/latest/aws-emr.html>`_, the process is similar for
Google Cloud DataProc.

Data Access
-----------

You may want to install additional libraries in your Jupyter and worker images
to access the object stores of each cloud:

-  `s3fs <https://s3fs.readthedocs.io/>`_ for Amazon's S3
-  `gcsfs <https://gcsfs.readthedocs.io/>`_ for Google's GCS
-  `adlfs <https://azure-datalake-store.readthedocs.io/>`_ for Microsoft's ADL

Historical Libraries
--------------------

Dask previously maintained libraries for deploying Dask on Amazon's EC2.
Due to sporadic interest, and churn both within the Dask library and EC2 itself,
these were not well maintained.
They have since been deprecated in favor of the
:doc:`Kubernetes and Helm <kubernetes-helm>` solution.
