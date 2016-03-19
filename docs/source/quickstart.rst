Quickstart
==========

Install
-------

::

    $ pip install distributed --upgrade

See :doc:`installation <install>` document for more information.

Setup Cluster
-------------

Set up a fake cluster on your local computer::

   $ dscheduler
   $ dworker 127.0.0.1:8786
   $ dworker 127.0.0.1:8786
   $ dworker 127.0.0.1:8786

Or if you can ssh into your own computer (or others) then use the ``dcluster``
command, providing hostnames or IP addresses::

   $ dcluster 127.0.0.1 127.0.0.1 127.0.0.1 127.0.0.1

See :doc:`setup <setup>` for advanced use or :doc:`EC2 quickstart <dec2>` for
instructions on how to deploy on Amazon's Elastic Compute Cloud.

Launch Executor
---------------

Launch an Executor to interact with the network.  Point to the center
IP/port.::

   $ ipython

.. code-block:: python

   >>> from distributed import Executor
   >>> executor = Executor('127.0.0.1:8786')

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

   >>> A = executor.map(square, range(10))
   >>> B = executor.map(neg, A)
   >>> total = executor.submit(sum, B)
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
for a single future, or with the ``Executor.gather`` method for many futures at
once.

.. code-block:: python

   >>> total.result()     # result for single future
   -285
   >>> executor.gather(A) # gather for many futures
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]


Restart
~~~~~~~

When things go wrong, or when you want to reset the cluster state, call the
``restart`` method.

.. code-block:: python

   >>> executor.restart()

See :doc:`executor <executor>` for advanced use.
