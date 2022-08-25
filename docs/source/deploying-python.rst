Python API
==========

You can create a ``dask.distributed`` scheduler by importing and creating a
``Client`` with no arguments.  This overrides whatever default was previously
set.

.. code-block:: python

   from dask.distributed import Client
   client = Client()

You can navigate to ``http://localhost:8787/status`` to see the diagnostic
dashboard if you have Bokeh installed.

Client
------

You can trivially set up a local cluster on your machine by instantiating a Dask
Client with no arguments

.. code-block:: python

   from dask.distributed import Client
   client = Client()

This sets up a scheduler in your local process along with a number of workers and
threads per worker related to the number of cores in your machine.

If you want to run workers in your same process, you can pass the
``processes=False`` keyword argument.

.. code-block:: python

   client = Client(processes=False)

This is sometimes preferable if you want to avoid inter-worker communication
and your computations release the GIL.  This is common when primarily using
NumPy or Dask Array.


LocalCluster
------------

The ``Client()`` call described above is shorthand for creating a LocalCluster
and then passing that to your client.

.. code-block:: python

   from dask.distributed import Client, LocalCluster
   cluster = LocalCluster()
   client = Client(cluster)

This is equivalent, but somewhat more explicit.

You may want to look at the
keyword arguments available on ``LocalCluster`` to understand the options available
to you on handling the mixture of threads and processes, like specifying explicit
ports, and so on.

Cluster manager features
------------------------

Instantiating a cluster manager class like ``LocalCluster`` and then passing it to the
``Client`` is a common pattern. Cluster managers also provide useful utilities to help
you understand what is going on.

For example you can retrieve the Dashboard URL.

.. code-block:: python

   >>> cluster.dashboard_link
   'http://127.0.0.1:8787/status'

You can retrieve logs from cluster components.

.. code-block:: python

   >>> cluster.get_logs()
   {'Cluster': '',
   'Scheduler': "distributed.scheduler - INFO - Clear task state\ndistributed.scheduler - INFO -   S...

If you are using a cluster manager that supports scaling you can modify the number of workers manually
or automatically based on workload.

.. code-block:: python

   >>> cluster.scale(10)  # Sets the number of workers to 10

   >>> cluster.adapt(minimum=1, maximum=10)  # Allows the cluster to auto scale to 10 when tasks are computed

Reference
---------

.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:
