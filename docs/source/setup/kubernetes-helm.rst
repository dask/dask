Kubernetes and Helm
===================

It is easy to launch a Dask cluster and a Jupyter notebook server on cloud
resources using Kubernetes_ and Helm_.

.. _Kubernetes: https://kubernetes.io/
.. _Helm: https://helm.sh/

This is particularly useful when you want to deploy a fresh Python environment
on Cloud services like Amazon Web Services, Google Compute Engine, or
Microsoft Azure.

If you already have Python environments running in a pre-existing Kubernetes
cluster, then you may prefer the :doc:`Kubernetes native<kubernetes-native>`
documentation, which is a bit lighter weight.


Launch Kubernetes Cluster
-------------------------

This document assumes that you have a Kubernetes cluster and Helm installed.

If this is not the case, then you might consider setting up a Kubernetes cluster
on one of the common cloud providers like Google, Amazon, or
Microsoft.  We recommend the first part of the documentation in the guide
`Zero to JupyterHub <https://zero-to-jupyterhub.readthedocs.io/en/latest/>`_
that focuses on Kubernetes and Helm (you do not need to follow all of these
instructions).  Also, JupyterHub is not necessary to deploy Dask:

- `Creating a Kubernetes Cluster <https://zero-to-jupyterhub.readthedocs.io/en/v0.4-doc/create-k8s-cluster.html>`_
- `Setting up Helm <https://zero-to-jupyterhub.readthedocs.io/en/v0.4-doc/setup-helm.html>`_

Alternatively, you may want to experiment with Kubernetes locally using
`Minikube <https://kubernetes.io/docs/getting-started-guides/minikube/>`_.


Helm Install Dask
-----------------

Dask maintains a Helm chart in the default stable channel at
https://kubernetes-charts.storage.googleapis.com .
This should be added to your helm installation by default.
You can update the known channels to make sure you have up-to-date charts as follows::

   helm repo update

Now, you can launch Dask on your Kubernetes cluster using the Dask Helm_ chart::

   helm install stable/dask

This deploys a ``dask-scheduler``, several ``dask-worker`` processes, and
also an optional Jupyter server.


Verify Deployment
-----------------

This might take a minute to deploy.  You can check its status with
``kubectl``::

   kubectl get pods
   kubectl get services

   $ kubectl get pods
   NAME                                  READY     STATUS              RESTARTS    AGE
   bald-eel-jupyter-924045334-twtxd      0/1       ContainerCreating   0            1m
   bald-eel-scheduler-3074430035-cn1dt   1/1       Running             0            1m
   bald-eel-worker-3032746726-202jt      1/1       Running             0            1m
   bald-eel-worker-3032746726-b8nqq      1/1       Running             0            1m
   bald-eel-worker-3032746726-d0chx      0/1       ContainerCreating   0            1m

   $ kubectl get services
   NAME                 TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)                      AGE
   bald-eel-jupyter     LoadBalancer   10.11.247.201   35.226.183.149   80:30173/TCP                  2m
   bald-eel-scheduler   LoadBalancer   10.11.245.241   35.202.201.129   8786:31166/TCP,80:31626/TCP   2m
   kubernetes           ClusterIP      10.11.240.1     <none>           443/TCP
   48m

You can use the addresses under ``EXTERNAL-IP`` to connect to your now-running
Jupyter and Dask systems.

Notice the name ``bald-eel``.  This is the name that Helm has given to your
particular deployment of Dask.  You could, for example, have multiple
Dask-and-Jupyter clusters running at once, and each would be given a different
name.  Note that you will need to use this name to refer to your deployment in the future.  
Additionally, you can list all active helm deployments with::

   helm list

   NAME            REVISION        UPDATED                         STATUS      CHART           NAMESPACE
   bald-eel        1               Wed Dec  6 11:19:54 2017        DEPLOYED    dask-0.1.0      default


Connect to Dask and Jupyter
---------------------------

When we ran ``kubectl get services``, we saw some externally visible IPs:

.. code-block:: bash

   mrocklin@pangeo-181919:~$ kubectl get services
   NAME                 TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)                       AGE
   bald-eel-jupyter     LoadBalancer   10.11.247.201   35.226.183.149   80:30173/TCP                  2m
   bald-eel-scheduler   LoadBalancer   10.11.245.241   35.202.201.129   8786:31166/TCP,80:31626/TCP   2m
   kubernetes           ClusterIP      10.11.240.1     <none>           443/TCP                       48m

We can navigate to these services from any web browser. Here, one is the Dask diagnostic
dashboard, and the other is the Jupyter server.  You can log into the Jupyter
notebook server with the password, ``dask``.

You can create a notebook and create a Dask client from there.  The
``DASK_SCHEDULER_ADDRESS`` environment variable has been populated with the
address of the Dask scheduler.  This is available in Python in the ``config`` dictionary.

.. code-block:: python

   >>> from dask.distributed import Client, config
   >>> config['scheduler-address']
   'bald-eel-scheduler:8786'

Although you don't need to use this address, the Dask client will find this
variable automatically.

.. code-block:: python

   from dask.distributed import Client, config
   client = Client()


Configure Environment
---------------------

By default, the Helm deployment launches three workers using two cores each and
a standard conda environment.  We can customize this environment by creating a
small yaml file that implements a subset of the values in the
`dask helm chart values.yaml file <https://github.com/dask/helm-chart/blob/master/dask/values.yaml>`_.

For example, we can increase the number of workers, and include extra conda and
pip packages to install on the both the workers and Jupyter server (these two
environments should be matched).

.. code-block:: yaml

   # config.yaml

   worker:
     replicas: 8
     resources:
       limits:
         cpu: 2
         memory: 7.5G
       requests:
         cpu: 2
         memory: 7.5G
     env:
       - name: EXTRA_CONDA_PACKAGES
         value: numba xarray -c conda-forge
       - name: EXTRA_PIP_PACKAGES
         value: s3fs dask-ml --upgrade

   # We want to keep the same packages on the worker and jupyter environments
   jupyter:
     enabled: true
     env:
       - name: EXTRA_CONDA_PACKAGES
         value: numba xarray matplotlib -c conda-forge
       - name: EXTRA_PIP_PACKAGES
         value: s3fs dask-ml --upgrade

This config file overrides the configuration for the number and size of workers and the
conda and pip packages installed on the worker and Jupyter containers.  In
general, we will want to make sure that these two software environments match.

Update your deployment to use this configuration file.  Note that *you will not
use helm install* for this stage: that would create a *new* deployment on the
same Kubernetes cluster.  Instead, you will upgrade your existing deployment by
using the current name::

    helm upgrade bald-eel stable/dask -f config.yaml

This will update those containers that need to be updated.  It may take a minute or so.

As a reminder, you can list the names of deployments you have using ``helm
list``


Check status and logs
---------------------

For standard issues, you should be able to see the worker status and logs using the
Dask dashboard (in particular, you can see the worker links from the ``info/`` page).
However, if your workers aren't starting, you can check the status of pods and
their logs with the following commands:

.. code-block:: bash

   kubectl get pods
   kubectl logs <PODNAME>

.. code-block:: bash

   mrocklin@pangeo-181919:~$ kubectl get pods
   NAME                                  READY     STATUS    RESTARTS   AGE
   bald-eel-jupyter-3805078281-n1qk2     1/1       Running   0          18m
   bald-eel-scheduler-3074430035-cn1dt   1/1       Running   0          58m
   bald-eel-worker-1931881914-1q09p      1/1       Running   0          18m
   bald-eel-worker-1931881914-856mm      1/1       Running   0          18m
   bald-eel-worker-1931881914-9lgzb      1/1       Running   0          18m
   bald-eel-worker-1931881914-bdn2c      1/1       Running   0          16m
   bald-eel-worker-1931881914-jq70m      1/1       Running   0          17m
   bald-eel-worker-1931881914-qsgj7      1/1       Running   0          18m
   bald-eel-worker-1931881914-s2phd      1/1       Running   0          17m
   bald-eel-worker-1931881914-srmmg      1/1       Running   0          17m

   mrocklin@pangeo-181919:~$ kubectl logs bald-eel-worker-1931881914-856mm
   EXTRA_CONDA_PACKAGES environment variable found.  Installing.
   Fetching package metadata ...........
   Solving package specifications: .
   Package plan for installation in environment /opt/conda/envs/dask:
   The following NEW packages will be INSTALLED:
       fasteners: 0.14.1-py36_2 conda-forge
       monotonic: 1.3-py36_0    conda-forge
       zarr:      2.1.4-py36_0  conda-forge
   Proceed ([y]/n)?
   monotonic-1.3- 100% |###############################| Time: 0:00:00  11.16 MB/s
   fasteners-0.14 100% |###############################| Time: 0:00:00 576.56 kB/s
   ...


Delete a Helm deployment
------------------------

You can always delete a helm deployment using its name::

   helm delete bald-eel --purge

Note that this does not destroy any clusters that you may have allocated on a
Cloud service (you will need to delete those explicitly).


Avoid the Jupyter Server
------------------------

Sometimes you do not need to run a Jupyter server alongside your Dask cluster.

.. code-block:: yaml

   jupyter:
     enabled: false
