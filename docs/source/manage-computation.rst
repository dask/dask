Managing Computation
====================

Data and Computation in Dask.distributed are always in one of three states

1.  Concrete values in local memory.  Example include the integer ``1`` or a
    numpy array in the local process.
2.  Lazy computations in a dask graph, perhaps stored in a ``dask.delayed`` or
    ``dask.dataframe`` object.
3.  Running computations or remote data, represented by ``Future`` objects
    pointing to computations currently in flight.

All three of these forms are important and there are functions that convert
between all three states.


Dask Collections to Concrete Values
-----------------------------------

You can turn any dask collection into a concrete value by calling the
``.compute()`` method or ``dask.compute(...)`` function.  This function will
block until the computation is finished, going straight from a lazy dask
collection to a concrete value in local memory.

This approach is the most familiar and straightforward, especially for people
coming from the standard single-machine Dask experience or from just normal
programming.  It is great when you have data already in memory and want to get
small fast results right to your local process.

.. code-block:: python

   >>> df = dd.read_csv('s3://...')
   >>> df.value.sum().compute()
   100000000

However, this approach often breaks down if you try to bring the entire dataset
back to local RAM

.. code-block:: python

   >>> df.compute()
   MemoryError(...)

It also forces you to wait until the computation finishes before handing back
control of the interpreter.


Dask Collections to Futures
---------------------------

You can asynchronously submit lazy dask graphs to run on the cluster with the
``client.compute`` and ``client.persist`` methods.  These functions return Future objects
immediately.  These futures can then be queried to determine the state of the
computation.

client.compute
~~~~~~~~~~~~~~

The ``.compute`` method takes a collection and returns a single future.

.. code-block:: python

   >>> df = dd.read_csv('s3://...')
   >>> total = client.compute(df.sum())  # Return a single future
   >>> total
   Future(..., status='pending')

   >>> total.result()               # Block until finished
   100000000

Because this is a single future the result must fit on a single worker machine.
Like ``dask.compute`` above, the ``client.compute`` method is only appropriate when
results are small and should fit in memory.  The following would likely fail:

.. code-block:: python

   >>> future = client.compute(df)       # Blows up memory

Instead, you should use ``client.persist``

client.persist
~~~~~~~~~~~~~~

The ``.persist`` method submits the task graph behind the Dask collection to
the scheduler, obtaining Futures for all of the top-most tasks (for example one
Future for each Pandas DataFrame in a Dask DataFrame).  It then returns a copy
of the collection pointing to these futures instead of the previous graph.
This new collection is semantically equivalent but now points to actively
running data rather than a lazy graph.  If you look at the dask graph within
the collection you will see the Future objects directly:

.. code-block:: python

   >>> df = dd.read_csv('s3://...')
   >>> df.dask                          # Recipe to compute df in chunks
   {('read', 0): (load_s3_bytes, ...),
    ('parse', 0): (pd.read_csv, ('read', 0)),
    ('read', 1): (load_s3_bytes, ...),
    ('parse', 1): (pd.read_csv, ('read', 1)),
    ...
   }

   >>> df = client.persist(df)               # Start computation
   >>> df.dask                          # Now points to running futures
   {('parse', 0): Future(..., status='finished'),
    ('parse', 1): Future(..., status='pending'),
    ...
   }

The collection is returned immediately and the computation happens in the
background on the cluster.  Eventually all of the futures of this collection
will be completed at which point further queries on this collection will likely
be very fast.

Typically the workflow is to define a computation with a tool like
``dask.dataframe`` or ``dask.delayed`` until a point where you have a nice
dataset to work from, then persist that collection to the cluster and then
perform many fast queries off of the resulting collection.


Concrete Values to Futures
--------------------------

We obtain futures through a few different ways.  One is the mechanism above, by
wrapping Futures within Dask collections.  Another is by submitting data or
tasks directly to the cluster with ``client.scatter``, ``client.submit`` or ``client.map``.

.. code-block:: python

   futures = client.scatter(args)                        # Send data
   future = client.submit(function, *args, **kwrags)     # Send single task
   futures = client.map(function, sequence, **kwargs)    # Send many tasks

In this case ``*args`` or ``**kwargs`` can be normal Python objects, like ``1``
or ``'hello'``, or they can be other ``Future`` objects if you want to link
tasks together with dependencies.

Unlike Dask collections like dask.delayed these task submissions happen
immediately.  The concurrent.futures interface is very similar to dask.delayed
except that execution is immediate rather than lazy.


Futures to Concrete Values
--------------------------

You can turn an individual ``Future`` into a concrete value in the local
process by calling the ``Future.result()`` method.  You can convert a
collection of futures into concrete values by calling the ``client.gather`` method.

.. code-block:: python

   >>> future.result()
   1

   >>> client.gather(futures)
   [1, 2, 3, 4, ...]


Futures to Dask Collections
---------------------------

As seen in the Collection to futures section it is common to have currently
computing ``Future`` objects within Dask graphs.  This lets us build further
computations on top of currently running computations.  This is most often done
with dask.delayed workflows on custom computations:

.. code-block:: python

   >>> x = delayed(sum)(futures)
   >>> y = delayed(product)(futures)
   >>> future = client.compute(x + y)

Mixing the two forms allow you to build and submit a computation in stages like
``sum(...) + product(...)``.  This is often valuable if you want to wait to see
the values of certain parts of the computation before determining how to
proceed.  Submitting many computations at once allows the scheduler to be
slightly more intelligent when determining what gets run.


*If this page interests you then you may also want to check out the doc page
on* :doc:`Managing Memory<memory>`
