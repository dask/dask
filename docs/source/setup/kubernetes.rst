Kubernetes
==========

.. toctree::
   :maxdepth: 1
   :hidden:

   Helm <kubernetes-helm.rst>
   Native <kubernetes-native.rst>

Kubernetes_ is a popular system for deploying distributed applications on clusters,
particularly in the cloud.  You can use Kubernetes to launch Dask workers in the 
following two ways:

1.  **Helm**:
    You can launch a Dask scheduler, several workers, and an optional Jupyter Notebook server
    on a Kubernetes easily using Helm_

    .. code-block:: bash

       helm repo update          # get latest helm charts
       helm install stable/dask  # deploy standard dask chart

    This is a good choice if you want to do the following:

    1.  Run a managed Dask cluster for a long period of time
    2.  Also deploy a Jupyter server from which to run code
    3.  Share the same Dask cluster between many automated services
    4.  Try out Dask for the first time on a cloud-based system
        like Amazon, Google, or Microsoft Azure
        (see also our :doc:`Cloud documentation <cloud>`)
    
    .. note::
    
      For more information, see :doc:`Dask and Helm documentation <kubernetes-helm>`.

2.  **Native**:
    You can quickly deploy Dask workers on Kubernetes
    from within a Python script or interactive session using Dask-Kubernetes_

    .. code-block:: python

       from dask_kubernetes import KubeCluster
       cluster = KubeCluster.from_yaml('worker-template.yaml')
       cluster.scale(20)  # add 20 workers
       cluster.adapt()    # or create and destroy workers dynamically based on workload

       from dask.distributed import Client
       client = Client(cluster)

    This is a good choice if you want to do the following:

    1.  Dynamically create a personal and ephemeral deployment for interactive use
    2.  Allow many individuals the ability to launch their own custom dask deployments,
        rather than depend on a centralized system
    3.  Quickly adapt Dask cluster size to the current workload

    .. note::
    
      For more information, see Dask-Kubernetes_ documentation.

You may also want to see the documentation on using
:doc:`Dask with Docker containers <docker>`
to help you manage your software environments on Kubernetes.

.. _Kubernetes: https://kubernetes.io/
.. _Dask-Kubernetes: https://kubernetes.dask.org/
.. _Helm: https://helm.sh/
