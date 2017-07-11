Quickstart
==========

Install
-------

::

    $ pip install dask distributed --upgrade

See :doc:`installation <install>` document for more information.


Setup Dask.distributed the Easy Way
-----------------------------------

If you create an client without providing an address it will start up a local
scheduler and worker for you.

.. code-block:: python

   >>> from dask.distributed import Client
   >>> client = Client()  # set up local cluster on your laptop
   >>> client
   <Client: scheduler="127.0.0.1:8786" processes=8 cores=8>


Setup Dask.distributed the Hard Way
-----------------------------------
This allows dask.distributed to use multiple machines as workers.

Set up scheduler and worker processes on your local computer::

   $ dask-scheduler
   Scheduler started at 127.0.0.1:8786

   $ dask-worker 127.0.0.1:8786
   $ dask-worker 127.0.0.1:8786
   $ dask-worker 127.0.0.1:8786

.. note:: At least one ``dask-worker`` must be running after launching a
          scheduler.

Launch an Client and point it to the IP/port of the scheduler.

.. code-block:: python

   >>> from dask.distributed import Client
   >>> client = Client('127.0.0.1:8786')

See :doc:`setup <setup>` for advanced use.


Map and Submit Functions
~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``map`` and ``submit`` methods to launch computations on the cluster.
The ``map/submit`` functions send the function and arguments to the remote
workers for processing.  They return ``Future`` objects that refer to remote
data on the cluster.  The ``Future`` returns immediately while the computations
run remotely in the background.

.. code-block:: python

   >>> def square(x):
           return x ** 2

   >>> def neg(x):
           return -x

   >>> A = client.map(square, range(10))
   >>> B = client.map(neg, A)
   >>> total = client.submit(sum, B)
   >>> total.result()
   -285


Gather
~~~~~~

The ``map/submit`` functions return ``Future`` objects, lightweight tokens that
refer to results on the cluster.  By default the results of computations
*stay on the cluster*.

.. code-block:: python

   >>> total  # Function hasn't yet completed
   <Future: status: waiting, key: sum-58999c52e0fa35c7d7346c098f5085c7>

   >>> total  # Function completed, result ready on remote worker
   <Future: status: finished, key: sum-58999c52e0fa35c7d7346c098f5085c7>

Gather results to your local machine either with the ``Future.result`` method
for a single future, or with the ``Client.gather`` method for many futures at
once.

.. code-block:: python

   >>> total.result()   # result for single future
   -285
   >>> client.gather(A) # gather for many futures
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]


Restart
~~~~~~~

When things go wrong, or when you want to reset the cluster state, call the
``restart`` method.

.. code-block:: python

   >>> client.restart()

See :doc:`client <client>` for advanced use.
