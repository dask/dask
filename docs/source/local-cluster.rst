Local Cluster
=============

For convenience you can start a local cluster from your Python session.

.. code-block:: python

   >>> from distributed import Executor, LocalCluster
   >>> c = LocalCluster()
   LocalCluster("127.0.0.1:8786", workers=8, ncores=8)
   >>> e = Executor(c)
   <Executor: scheduler=127.0.0.1:8786 processes=8 cores=8>


Alternatively, a ``LocalCluster`` is made for you automatically if you create
an ``Executor`` with no arguments.

.. code-block:: python

   >>> from distributed import Executor
   >>> e = Executor()
   >>> e
   <Executor: scheduler=127.0.0.1:8786 processes=8 cores=8>


.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:

