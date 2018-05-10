Cloud Deployments
=================

To get started running Dask on common Cloud providers
like Amazon, Google, or Microsoft
we currently recommend deploying
:doc:`Dask with Kubernetes and Helm <kubernetes-helm>`.

All three major cloud vendors now provide managed Kubernetes services.
This allows us to reliably provide the same experience across all clouds,
and ensures that solutions for any one provider remain up-to-date.

Data Access
-----------

You may want to install additional libraries in your Jupyter and worker images
to access the object stores of each cloud

-  `s3fs <https://s3fs.readthedocs.io/>`_ for Amazon's S3
-  `gcsfs <https://gcsfs.readthedocs.io/>`_ for Google's GCS
-  `adlfs <http://azure-datalake-store.readthedocs.io/>`_ for Microsoft's ADL

Historical Libraries
--------------------

Dask previously maintained libraries for deploying Dask on Amazon's EC2.
Due to sporadic interest, and churn both within the Dask library and EC2 itself,
these were not well maintained.
They have since been deprecated in favor of the
:doc:`Kubernetes and Helm <kubernetes-helm>` solution.
