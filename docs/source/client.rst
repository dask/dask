Client
========

The Client is the primary entry point for users of ``dask.distributed``.

After we :doc:`setup a cluster <setup>`, we initialize a ``Client`` by pointing
it to the address of a ``Scheduler``:

.. code-block:: python

   >>> from distributed import Client
   >>> client = Client('127.0.0.1:8786')

There are a few different ways to interact with the cluster through the client:

1.  The Client satisfies most of the standard concurrent.futures_ - PEP-3148_
    interface with ``.submit``, ``.map`` functions and ``Future`` objects,
    allowing the immediate and direct submission of tasks.
2.  The Client registers itself as the default Dask_ scheduler, and so runs all
    dask collections like dask.array_, dask.bag_, dask.dataframe_ and dask.delayed_
3.  The Client has additional methods for manipulating data remotely.  See the
    full :doc:`API <api>` for a thorough list.


Concurrent.futures
------------------

We can submit individual function calls with the ``client.submit`` method or
many function calls with the ``client.map`` method

.. code-block:: python

   >>> def inc(x):
           return x + 1

   >>> x = client.submit(inc, 10)
   >>> x
   <Future - key: inc-e4853cffcc2f51909cdb69d16dacd1a5>

   >>> L = client.map(inc, range(1000))
   >>> L
   [<Future - key: inc-e4853cffcc2f51909cdb69d16dacd1a5>,
    <Future - key: inc-...>,
    <Future - key: inc-...>,
    <Future - key: inc-...>, ...]

These results live on distributed workers.

We can submit tasks on futures.  The function will go to the machine where the
futures are stored and run on the result once it has completed.

.. code-block:: python

   >>> y = client.submit(inc, x)      # Submit on x, a Future
   >>> total = client.submit(sum, L)  # Map on L, a list of Futures

We gather back the results using either the ``Future.result`` method for single
futures or ``client.gather`` method for many futures at once.

.. code-block:: python

   >>> x.result()
   11

   >>> client.gather(L)
   [1, 2, 3, 4, 5, ...]

But, as always, we want to minimize communicating results back to the local
process.  It's often best to leave data on the cluster and operate on it
remotely with functions like ``submit``, ``map``, ``get`` and ``compute``.
See :doc:`efficiency <efficiency>` for more information on efficient use of
distributed.


Dask
----

The parent library Dask_ contains objects like dask.array_, dask.dataframe_,
dask.bag_, and dask.delayed_, which automatically produce parallel algorithms
on larger datasets.  All dask collections work smoothly with the distributed
scheduler.

When we create a ``Client`` object it registers itself as the default Dask
scheduler.  All ``.compute()`` methods will automatically start using the
distributed system.

.. code-block:: python

   client = Client('scheduler:8786')

   my_dataframe.sum().compute()  # Now uses the distributed system by default

We can stop this behavior by using the ``set_as_default=False`` keyword
argument when starting the Client.

Dask's normal ``.compute()`` methods are *synchronous*, meaning that they block
the interpreter until they complete.  Dask.distributed allows the new ability
of *asynchronous* computing, we can trigger computations to occur in the
background and persist in memory while we continue doing other work.  This is
typically handled with the ``Client.persist`` and ``Client.compute`` methods
which are used for larger and smaller result sets respectively.

.. code-block:: python

   >>> df = client.persist(df)  # trigger all computations, keep df in memory
   >>> type(df)
   dask.DataFrame

For more information see the page on :doc:`Managing Computation <manage-computation>`.


Pure Functions by Default
-------------------------

By default we assume that all functions are pure_.  If this is not the case we
should use the ``pure=False`` keyword argument.

The client associates a key to all computations.  This key is accessible on
the Future object.

.. code-block:: python

   >>> from operator import add
   >>> x = client.submit(add, 1, 2)
   >>> x.key
   'add-ebf39f96ad7174656f97097d658f3fa2'

This key should be the same across all computations with the same inputs and
across all machines.  If we run the computation above on any computer with the
same environment then we should get the exact same key.

The scheduler avoids redundant computations.  If the result is already in
memory from a previous call then that old result will be used rather than
recomputing it.  Calls to submit or map are idempotent in the common case.

While convenient, this feature may be undesired for impure functions, like
``random``.  In these cases two calls to the same function with the same inputs
should produce different results.  We accomplish this with the ``pure=False``
keyword argument.  In this case keys are randomly generated (by ``uuid4``.)

.. code-block:: python

   >>> import numpy as np
   >>> client.submit(np.random.random, 1000, pure=False).key
   'random_sample-fc814a39-ee00-42f3-8b6f-cac65bcb5556'
   >>> client.submit(np.random.random, 1000, pure=False).key
   'random_sample-a24e7220-a113-47f2-a030-72209439f093'

.. _pure: https://toolz.readthedocs.io/en/latest/purity.html


Tornado Coroutines
------------------

If we are operating in an asynchronous environment then the blocking functions
listed above become asynchronous equivalents.  You must start your client
with the ``asynchronous=True`` keyword and ``yield`` or ``await`` blocking
functions.

.. code-block:: python

   @gen.coroutine
   def f():
       client = yield Client(asynchronous=True)
       future = client.submit(func, *args)
       result = yield future
       return result

If you want to reuse the same client in asynchronous and synchronous
environments you can apply the ``asynchronous=True`` keyword at each method
call.

.. code-block:: python

   client = Client()  # normal blocking client

   @gen.coroutine
   def f():
       futures = client.map(func, L)
       results = yield client.gather(futures, asynchronous=True)
       return results

See the :doc:`Asynchronous <asynchronous>` documentation for more information.


Additional Links
----------------

For more information on how to use dask.distributed you may want to look at the
following pages:

*  :doc:`Managing Memory <memory>`
*  :doc:`Managing Computation <manage-computation>`
*  :doc:`Data Locality <locality>`
*  :doc:`API <api>`

.. _concurrent.futures:  https://docs.python.org/3/library/concurrent.futures.html
.. _PEP-3148: https://www.python.org/dev/peps/pep-3148/
.. _dask.array: https://docs.dask.org/en/latest/array.html
.. _dask.bag: https://docs.dask.org/en/latest/bag.html
.. _dask.dataframe: https://docs.dask.org/en/latest/dataframe.html
.. _dask.delayed: https://docs.dask.org/en/latest/delayed.html
.. _Dask: https://dask.org
