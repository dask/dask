Basic Model and Quickstart
==========================

Install
-------

::

    $ pip install distributed

Setup Network
-------------

Given a network of computers ``192.168.0.{100,101,102}`` we run the ``dcenter``
command on one of them::

   $ ssh 192.168.1.100
   $ dcenter
   Start center at 192.168.1.100:8787

We run ``dworker`` on the others, passing the address of the first as a command
line argument::

   $ ssh 192.168.1.101
   $ dworker 192.168.1.100:8787
   Start worker at:            192.168.1.101:8788
   Registered with center at:  192.168.1.100:8787

   $ ssh 192.168.1.102
   $ dworker 192.168.1.100:8787
   Start worker at:            192.168.1.102:8788
   Registered with center at:  192.168.1.100:8787

These processes talk to each other over the network.  Each worker serves
data and runs arbitrary functions on that data.  The workers share data with
each other.  The center knows all of the workers and what data they have.

If you want to do this on your local computer then omit the ``ssh`` commands
and replace the IP addresses above with ``127.0.0.1``.


User Clients
------------

We use our network with two client modules:

*  ``Pool`` - mimics ``multiprocessing.Pool``
*  ``get`` - mimics ``dask.get``.

Pool
````

The pool provides ``map``, ``apply``, and ``apply_async`` functions.  Results
of these functions are ``RemoteData`` objects, proxies that live on the
cluster.

.. code-block:: python

   >>> from distributed import Pool
   >>> pool = Pool('192.168.1.100:8787')  # Provide address of center

   >>> A = pool.map(lambda x: x**2, range(10))
   >>> B = pool.map(lambda x: -x, A)
   >>> total = pool.apply(sum, [B])
   >>> total.get()
   -285

By default pool does not bring results back to your local computer but leaves
them on the distributed network.  As a result, computations on returned results
like the following don't require any data transfer.

.. code-block:: python

   >>> B = pool.map(lambda x: -x, A)

Gather results to your local machine with the gather method

.. code-block:: python

   >>> pool.gather(A)
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

get
```

The ``distributed.dask.get`` function operates like a typical dask scheduler.

Get works with raw dask graphs:

.. code-block:: python

   >>> from distributed.dask import get
   >>> inc = lambda x: x + 1
   >>> dsk = {'a': 1, 'b': (inc, 'a')}
   >>> get('192.168.1.100', 8787, dsk, 'b')
   2

Get works with dask collections (like dask.array or dask.dataframe):

.. code-block:: python

   >>> from functools import partial
   >>> get2 = partial(get, '192.168.1.100', 8787)

   >>> import dask.array as da
   >>> x = da.arange(10, chunks=(5,))
   >>> x.sum().compute(get=get2)
   45


Benefits
--------

Both the Pool and get scheduler provide data

*  Data locality: computations prefer to run on workers that have the inputs
*  Limited resilience:  computations can recover from catastrophic failures of
   worker nodes during computation.

However at the moment there is no provision for worker failure between
computations.  There is no persistence layer.
