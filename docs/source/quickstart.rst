Basic Model and Quickstart
==========================

Install
-------

::

    $ pip install distributed

Setup Cluster
-------------

Set up center and worker nodes on your local computer with the ``dcluster``
command::

   $ dcenter 127.0.0.1 127.0.0.1 127.0.0.1 127.0.0.1

See setup_ for more advanced use.

.. _setup: setup.rst

Launch Executor
---------------

Launch an Executor to interact with network.  Provide the first address listed
in the ``dcenter`` call.

.. code-block:: python

   >>> from distributed import Executor
   >>> executor = Executor('127.0.0.1:8787')

Map and Submit
~~~~~~~~~~~~~~

The executor provides ``map`` and ``submit`` functions like
``concurrent.futures.Executor``.  Results of these functions are ``Future``
objects, proxies for data that lives on the cluster.

.. code-block:: python

   >>> A = executor.map(lambda x: x**2, range(10))
   >>> B = executor.map(lambda x: -x, A)
   >>> total = executor.submit(sum, B)
   >>> total.result()
   -285

Gather
~~~~~~

Gather results to your local machine with the gather method

.. code-block:: python

   >>> executor.gather(A)
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

get
~~~

The ``Executor.get`` method operates like a typical dask scheduler.

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
