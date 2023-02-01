Deploying Best Practices
========================

Deploying Dask might seem like a small problem, but there is a lot of hidden work. Borrowing machines, for example, includes a number of details including: securely connecting to those machines, reliably turning them off, shipping software, tracking users and costs, integrations, etc.

Problems
--------

Borrowing machines
~~~~~~~~~~~~~~~~~~

Comprehensive list of Dask projects under here, e.g.

- LocalCluster
- dask_yarn (YarnCluster)
- dask_kubernetes (KubeCluster)
- dask_cloudprovider
- dask_jobqueue
- dask_mpi
- etc.

Installing software
~~~~~~~~~~~~~~~~~~~

docker images, OS, private github repos, locally editable packages, etc.

HPC vs. Docker

Network access
~~~~~~~~~~~~~~

How do you get the right machines to talk to each other? local machine communicating w/ databases, remote machines, etc.

Link to `Dask Gateway <https://gateway.dask.org/>`_

Observability
~~~~~~~~~~~~~

How do you see what happened when things break?

- :doc:`distributed:logging`
- :doc:`dashboard`
- :doc:`distributed:prometheus`

Cost management
~~~~~~~~~~~~~~~

- Are things turned off?
- How much are we spending?
- Who is responsible?

Solutions
---------

Go over a number of different solutions

Nebari (formerly QHub)
~~~~~~~~~~~~~~~~~~~~~~

Kubeflow
~~~~~~~~

Saturn Cloud
~~~~~~~~~~~~

Coiled
~~~~~~

IBM/GCP/AzureML
~~~~~~~~~~~~~~~
