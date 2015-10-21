Quickstart
==========

Install
-------

::

    $ pip install distributed

Setup Cluster
-------------

Set up a fake cluster on your local computer::

   $ dcenter & \
     dworker 127.0.0.1:8787 & \
     dworker 127.0.0.1:8787 & \
     dworker 127.0.0.1:8787 &

Or if you can ssh into your own computer (or others) then use the ``dcluster``
command, providing hostnames or IP addresses::

   $ dcluster 127.0.0.1 127.0.0.1 127.0.0.1 127.0.0.1

See :doc:`setup <setup>` for advanced use.

Launch Executor
---------------

Launch an Executor to interact with the network.  Point to the center
IP/port.::

   $ ipython

.. code-block:: python

   >>> from distributed import Executor
   >>> executor = Executor('127.0.0.1:8787')

Map and Submit Functions
~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``map`` and ``submit`` methods to launch computation on the cluster.
Results of these functions are ``Future`` objects that proxy remote data on the
cluster.

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

Gather results to your local machine either with the ``Future.result`` method
as shown above for a single future, or with the ``Executor.gather`` method for
many futures at once.

.. code-block:: python

   >>> executor.gather(A)
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

get
~~~

Use the ``Executor.get`` method to interact with dask collections.

Get works with raw dask graphs:

.. code-block:: python

   >>> dsk = {'a': 12, 'b': (square, 'a')}
   >>> executor.get(dsk, 'b')
   144

Get works with dask collections (like dask.array or dask.dataframe):

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.arange(10, chunks=(5,))
   >>> x.sum().compute(get=executor.get)
   45

Shutdown
~~~~~~~~

Shut down the executor (and background thread) with the shutdown method.  This
does not close the worker processes.

.. code-block:: python

   >>> executor.shutdown()

See :doc:`executor <executor>` for advanced use.
