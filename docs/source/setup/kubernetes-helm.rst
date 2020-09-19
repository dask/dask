Kubernetes and Helm
===================

It is easy to launch a Dask cluster and a Jupyter_ notebook server on cloud
resources using Kubernetes_ and Helm_.

.. _Kubernetes: https://kubernetes.io/
.. _Helm: https://helm.sh/
.. _Jupyter: https://jupyter.org/

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
instructions). In particular, you don't need to install JupyterHub.

- `Creating a Kubernetes Cluster <https://zero-to-jupyterhub.readthedocs.io/en/latest/create-k8s-cluster.html>`_
- `Setting up Helm <https://zero-to-jupyterhub.readthedocs.io/en/latest/setup-helm.html>`_

Alternatively, you may want to experiment with Kubernetes locally using
`Minikube <https://kubernetes.io/docs/getting-started-guides/minikube/>`_.

Which Chart is Right for You?
-----------------------------

Dask maintains a Helm chart repository containing various charts for the Dask community
https://helm.dask.org/ .
You will need to add this to your known channels and update your local charts::

   helm repo add dask https://helm.dask.org/
   helm repo update

We provides two Helm charts. The right one to choose depends on whether you're
deploying Dask for a single user or for many users.


================  =====================================================================
Helm Chart        Use Case
================  =====================================================================
``dask/dask``     Single-user deployment with one notebook server and one Dask Cluster.
``dask/daskhub``  Multi-user deployment with JupyterHub and Dask Gateway.
================  =====================================================================

See :ref:`kubernetes-helm.single` or :ref:`kubernetes-helm.multi` for detailed
instructions on deploying either of these.
As you might suspect, deploying ``dask/daskhub`` is a bit more complicated since
there are more components. If you're just deploying for a single user we'd recommend
using ``dask/dask``.

.. _kubernetes-helm.single:

Helm Install Dask for a Single User
-----------------------------------

Once your Kubernetes cluster is ready, you can deploy dask using the Dask Helm_ chart::

   helm install my-dask dask/dask

This deploys a ``dask-scheduler``, several ``dask-worker`` processes, and
also an optional Jupyter server.


Verify Deployment
^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
address of the Dask scheduler.  This is available in Python from the ``dask.config`` object.

.. code-block:: python

   >>> import dask
   >>> dask.config.get('scheduler_address')
   'bald-eel-scheduler:8786'

Although you don't need to use this address, the Dask client will find this
variable automatically.

.. code-block:: python

   from dask.distributed import Client, config
   client = Client()


Configure Environment
^^^^^^^^^^^^^^^^^^^^^

By default, the Helm deployment launches three workers using one core each and
a standard conda environment. We can customize this environment by creating a
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

    helm upgrade bald-eel dask/dask -f config.yaml

This will update those containers that need to be updated.  It may take a minute or so.

As a reminder, you can list the names of deployments you have using ``helm
list``


Check status and logs
^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^

You can always delete a helm deployment using its name::

   helm delete bald-eel --purge

Note that this does not destroy any clusters that you may have allocated on a
Cloud service (you will need to delete those explicitly).


Avoid the Jupyter Server
^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes you do not need to run a Jupyter server alongside your Dask cluster.

.. code-block:: yaml

   jupyter:
     enabled: false

.. _kubernetes-helm.multi:

Helm Install Dask for Mulitple Users
------------------------------------

The ``dask/daskhub`` Helm Chart deploys JupyterHub_, `Dask Gateway`_, and configures
the two to work well together. In particular, Dask Gateway is registered as
a JupyterHub service so that Dask Gateway can re-use JupyterHub's authentication,
and the JupyterHub environment is configured to connect to the Dask Gateway
without any arguments.

.. note::

   The ``dask/daskhub`` helm chart came out of the `Pangeo`_ project, a community
   platform for big data geoscience.
   
.. _Pangeo: http://pangeo.io/
.. _Dask Gateway: https://gateway.dask.org/
.. _JupyterHub: https://jupyterhub.readthedocs.io/en/stable/

The ``dask/daskhub`` helm chart uses the JupyterHub and Dask-Gateway helm charts.
You'll want to consult the `JupyterHub helm documentation <https://zero-to-jupyterhub.readthedocs.io/en/latest/setup-jupyterhub/setup-jupyterhub.html>`_ and
and `Dask Gateway helm documentation <https://gateway.dask.org/install-kube.html>`_ for further customization. The default values
are at https://github.com/dask/helm-chart/blob/master/daskhub/values.yaml.

Verify that you've set up a Kubernetes cluster and added Dask's helm charts:

.. code-block:: console

   $ helm repo add dask https://helm.dask.org/
   $ helm repo update

JupyterHub and Dask Gateway require a few secret tokens. We'll generate them
on the command line and insert the tokens in a ``secrets.yaml`` file that will
be passed to Helm.

Run the following command, and copy the output. This is our `token-1`.

.. code-block:: console

   $ openssl rand -hex 32  # generate token-1

Run command again and copy the output again. This is our `token-2`.

.. code-block:: console

   $ openssl rand -hex 32  # generate token-2

Now substitute those two values for ``<token-1>`` and ``<token-2>`` below.
Note that ``<token-2>`` is used twice, once for ``jupyterhub.hub.services.dask-gateway.apiToken``, and a second time for ``dask-gateway.gateway.auth.jupyterhub.apiToken``.

.. code-block:: yaml

   # file: secrets.yaml
   jupyterhub:
     proxy:
       secretToken: "<token-1>"
     hub:
       services:
         dask-gateway:
           apiToken: "<token-2>"

   dask-gateway:
     gateway:
       auth:
         jupyterhub:
           apiToken: "<token-2>"

Now we're ready to install DaskHub

.. code-block:: console

   $ helm upgrade --wait --install --render-subchart-notes \
       dhub dask/daskhub \
       --values=secrets.yaml


The output explains how to find the IPs for your JupyterHub depoyment.

.. code-block:: console

   $ kubectl get service proxy-public
   NAME           TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)                      AGE
   proxy-public   LoadBalancer   10.43.249.239   35.202.158.223   443:31587/TCP,80:30500/TCP   2m40s


Creating a Dask Cluster
^^^^^^^^^^^^^^^^^^^^^^^

To create a Dask cluster on this deployment, users need to connect to the Dask Gateway

.. code-block:: python

   >>> from dask_gateway import GatewayCluster
   >>> cluster = GatewayCluster()
   >>> client = cluster.get_client()
   >>> cluster

Depending on the configuration, users may need to ``cluster.scale(n)`` to
get workers. See https://gateway.dask.org/ for more on Dask Gateway.

Matching the User Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dask Clients will be running the JupyterHub's singleuser environment. To ensure
that the same environment is used for the scheduler and workers, you can provide
it as a Gateway option and configure the ``singleuser`` environment to default
to the value set by JupyterHub.

.. code-block:: yaml

   # config.yaml
   jupyterhub:
     singleuser:
       extraEnv:
         DASK_GATEWAY__CLUSTER__OPTIONS__IMAGE: '{JUPYTER_IMAGE_SPEC}'

   dask-gateway:
     gateway:
       extraConfig:
         optionHandler: |
           from dask_gateway_server.options import Options, Integer, Float, String
           def option_handler(options):
               if ":" not in options.image:
                   raise ValueError("When specifying an image you must also provide a tag")
               return {
                   "image": options.image,
               }
           c.Backend.cluster_options = Options(
               String("image", default="pangeo/base-notebook:2020.07.28", label="Image"),
               handler=option_handler,
           )

The user environment will need to include ``dask-gateway``. Any packages installed
manually after the ``singleuser`` pod started will not be included in the worker
environment.
