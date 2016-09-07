Local Cluster
=============

For convenience you can start a local cluster from your Python session.

.. code-block:: python

   >>> from distributed import Client, LocalCluster
   >>> cluster = LocalCluster()
   LocalCluster("127.0.0.1:8786", workers=8, ncores=8)
   >>> client = Client(cluster)
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>


Alternatively, a ``LocalCluster`` is made for you automatically if you create
an ``Client`` with no arguments.

.. code-block:: python

   >>> from distributed import Client
   >>> client = Client()
   >>> client
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>


.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:

