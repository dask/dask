Kubernetes
==========

Kubernetes_ is a popular system for deploying distributed applications on clusters,
particularly in the cloud.  You can use Kubernetes to launch Dask clusters in the
following ways:

Dask Kubernetes Operator
------------------------

The Dask Kubernetes Operator is a set of Custom Resource Definitions (CRDs) and a controller that
allows you to create and manage your Dask clusters as native Kubernetes resources.

Creating clusters can either be done via the Kubernetes API with ``kubectl`` or the
Python API with ``KubeCluster``.

.. code-block:: bash

   helm install --repo https://helm.dask.org --create-namespace -n dask-operator --generate-name dask-kubernetes-operator

.. code-block:: bash

   # Create a cluster with kubectl
   kubectl apply -f - <<EOF
   apiVersion: kubernetes.dask.org/v1
   kind: DaskCluster
   metadata:
     name: my-dask-cluster
   spec:
     ...
   EOF

.. code-block:: python

   # Create a cluster in Python
   from dask_kubernetes.operator import KubeCluster
   cluster = KubeCluster(name="my-dask-cluster", image='ghcr.io/dask/dask:latest')
   cluster.scale(10)

This is a good choice if you want to do the following:

1. Have a Kubernetes native experience.
2. Manage Dask clusters via the Kubernetes API and tools like ``kubectl``.
3. Integrate Dask with other tools and workloads running on Kubernetes.
4. Compose Dask clusters as part of a larger Kubernetes application.

Learn more at `kubernetes.dask.org <https://kubernetes.dask.org/en/latest/operator.html>`_.


Dask Gateway
------------

Dask Gateway provides a secure, multi-tenant server for managing Dask clusters.
It allows users to launch and use Dask clusters in a shared, centrally managed cluster environment,
without requiring users to have direct access to the underlying cluster backend
(e.g. Kubernetes, Hadoop/YARN, HPC Job queues, etc…).

.. code-block:: bash

   helm install --repo https://helm.dask.org --create-namespace -n dask-gateway --generate-name dask-gateway

.. code-block:: python

   from dask_gateway import Gateway
   gateway = Gateway("<gateway service address>")
   cluster = gateway.new_cluster()

This is a good choice if you want to do the following:

1. Abstract users away from Kubernetes.
2. Provide a consistent Dask user experience across Kubernetes/Hadoop/HPC.

Learn more at `gateway.dask.org <https://gateway.dask.org/install-kube.html>`_.

DaskHub
^^^^^^^

You can also deploy Dask Gateway alongside `JupyterHub <https://jupyter.org/hub>`_ using the DaskHub helm chart.

.. code-block:: bash

   helm install --repo https://helm.dask.org --create-namespace -n daskhub --generate-name daskhub

Learn more at the `artifacthub.io DaskHub page <https://artifacthub.io/packages/helm/dask/daskhub>`_.


Single Cluster Helm Chart
-------------------------

You can deploy a single Dask cluster and (optionally) Jupyter on Kubernetes
easily using Helm_

.. code-block:: bash

   helm install --repo https://helm.dask.org my-dask dask

This is a good choice if you want to do the following:

1. Try out Dask for the first time on a cloud-based system
   like Amazon, Google, or Microsoft Azure where you already have
   a Kubernetes cluster. If you don't already have Kubernetes deployed,
   see our :doc:`Cloud documentation <deploying-cloud>`.

You can also use the ``HelmCluster`` cluster manager from dask-kubernetes to manage your
Helm Dask cluster from within your Python session.

.. code-block:: python

   from dask_kubernetes import HelmCluster

   cluster = HelmCluster(release_name="myrelease")
   cluster.scale(10)

Learn more at the `artifacthub.io Dask page <https://artifacthub.io/packages/helm/dask/dask>`_.

Further Reading
---------------

You may also want to see the documentation on using
:doc:`Dask with Docker containers <deploying-docker>`
to help you manage your software environments on Kubernetes.

.. _Kubernetes: https://kubernetes.io/
.. _Dask-Kubernetes: https://kubernetes.dask.org/
.. _Helm: https://helm.sh/
