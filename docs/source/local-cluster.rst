Local Cluster
=============

For convenience you can start a local cluster from your Python session.

.. code-block:: python

   >>> from distributed import Client, LocalCluster
   >>> cluster = LocalCluster()
   LocalCluster("127.0.0.1:8786", workers=8, ncores=8)
   >>> client = Client(cluster)
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>

You can dynamically scale this cluster up and down:

.. code-block:: python

   >>> worker = cluster.add_worker()
   >>> cluster.remove_worker(worker)

Alternatively, a ``LocalCluster`` is made for you automatically if you create
an ``Client`` with no arguments:

.. code-block:: python

   >>> from distributed import Client
   >>> client = Client()
   >>> client
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>

API
---

.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:

