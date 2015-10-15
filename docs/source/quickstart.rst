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
and replace the IP addresses above with ``127.0.0.1``::

   $ dcenter
   $ dworker 127.0.0.1:8787
   $ dworker 127.0.0.1:8787


Executor
--------

Locally we launch an Executor to interact with the distributed network.

.. code-block:: python

   >>> from distributed import Executor
   >>> executor = Executor('192.168.1.100:8787')  # Provide address of center
   >>> executor.start()

The executor provides ``map``, ``submit`` functions like
``concurrent.futures.Executor``.  Results of these functions are ``Future``
objects, proxies for data that lives on the cluster.

.. code-block:: python

   >>> A = executor.map(lambda x: x**2, range(10))
   >>> B = executor.map(lambda x: -x, A)
   >>> total = executor.submit(sum, [B])
   >>> total.get()
   -285

By default the executor does not bring results back to your local computer but
leaves them on the distributed network.  As a result, computations on returned
results like the following don't require any data transfer.

.. code-block:: python

   >>> B = executor.map(lambda x: -x, A)

Gather results to your local machine with the gather method

.. code-block:: python

   >>> executor.gather(A)
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

get
```

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


Benefits
--------

The executor provides:

*  Data locality: computations prefer to run on workers that have the inputs
*  Limited resilience:  computations can recover from catastrophic failures of
   worker nodes during computation.

However at the moment there is no provision for worker failure between
computations.  There is no persistence layer.
