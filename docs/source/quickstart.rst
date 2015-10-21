Basic Model and Quickstart
==========================

Install
-------

::

    $ pip install distributed

Setup Cluster
-------------

Set up a local cluster::

   $ bin/dcenter & \
     bin/dworker 127.0.0.1:8787 & \
     bin/dworker 127.0.0.1:8787 & \
     bin/dworker 127.0.0.1:8787 &

Or if you can ssh into your own computer use the ``dcluster`` command::

   $ dcenter 127.0.0.1 127.0.0.1 127.0.0.1 127.0.0.1

See :doc:`setup <setup>` for advanced use.

Launch Executor
---------------

Launch an Executor to interact with the network.  Point to the center IP/port.

.. code-block:: python

   >>> from distributed import Executor
   >>> executor = Executor('127.0.0.1:8787')

Map and Submit
~~~~~~~~~~~~~~

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
as shown below for a single future, or with the ``Executor.gather`` method for
many futures at once.

.. code-block:: python

   >>> executor.gather(A)
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

get
~~~

Use the ``Executor.get`` method to interact with dask collections.

Get works with raw dask graphs:

.. code-block:: python

   >>> dsk = {'a': 1, 'b': (inc, 'a')}
   >>> executor.get(dsk, 'b')
   2

Get works with dask collections (like dask.array or dask.dataframe):

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.arange(10, chunks=(5,))
   >>> x.sum().compute(get=executor.get)
   45

See :doc:`executor <executor>` for advanced use.
