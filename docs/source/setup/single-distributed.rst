Single Machine: dask.distributed
================================

The ``dask.distributed`` scheduler works well on a single machine.  It is sometimes
preferred over the default scheduler for the following reasons:

1.  It provides access to asynchronous API, notably :doc:`Futures <../futures>`
2.  It provides a diagnostic dashboard that can provide valuable insight on
    performance and progress
3.  It handles data locality with more sophistication, and so can be more
    efficient than the multiprocessing scheduler on workloads that require
    multiple processes

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

This sets up a scheduler in your local process and several processes running
single-threaded Workers.

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

This is equivalent, but somewhat more explicit.  You may want to look at the
keyword arguments available on ``LocalCluster`` to understand the options available
to you on handling the mixture of threads and processes, like specifying explicit
ports, and so on.

.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:
