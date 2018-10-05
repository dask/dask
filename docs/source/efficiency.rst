Efficiency
==========

Parallel computing done well is responsive and rewarding.  However, several
speed-bumps can get in the way.  This section describes common ways to ensure
performance.


Leave data on the cluster
-------------------------

Wait as long as possible to gather data locally.  If you want to ask a question
of a large piece of data on the cluster it is often faster to submit a function
onto that data then to bring the data down to your local computer.


For example if we have a numpy array on the cluster and we want to know its
shape we might choose one of the following options:

1.  **Slow:** Gather the numpy array to the local process, access the ``.shape``
    attribute
2.  **Fast:** Send a lambda function up to the cluster to compute the shape

.. code-block:: python

   >>> x = client.submit(np.random.random, (1000, 1000))
   >>> type(x)
   Future

**Slow**

.. code-block:: python

   >>> x.result().shape()  # Slow from lots of data transfer
   (1000, 1000)

**Fast**

.. code-block:: python

   >>> client.submit(lambda a: a.shape, x).result()  # fast
   (1000, 1000)


Use larger tasks
----------------

The scheduler adds about *one millisecond* of overhead per task or Future
object.  While this may sound fast it's quite slow if you run a billion tasks.
If your functions run faster than 100ms or so then you might not see any
speedup from using distributed computing.

A common solution is to batch your input into larger chunks.

**Slow**

.. code-block:: python

   >>> futures = client.map(f, seq)
   >>> len(futures)  # avoid large numbers of futures
   1000000000

**Fast**

.. code-block:: python

   >>> def f_many(chunk):
   ...     return [f(x) for x in chunk]

   >>> from toolz import partition_all
   >>> chunks = partition_all(1000000, seq)  # Collect into groups of size 1000

   >>> futures = client.map(f_many, chunks)
   >>> len(futures)  # Compute on larger pieces of your data at once
   1000


Adjust between Threads and Processes
------------------------------------

By default a single ``Worker`` runs many computations in parallel using as many
threads as your compute node has cores.  When using pure Python functions
this may not be optimal and you may instead want to run several separate
worker processes on each node, each using one thread.  When configuring your
cluster you may want to use the options to the ``dask-worker`` executable as
follows::

   $ dask-worker ip:port --nprocs 8 --nthreads 1

Note that if you're primarily using NumPy, Pandas, SciPy, Scikit Learn, Numba,
or other C/Fortran/LLVM/Cython-accelerated libraries then this is not an issue
for you.  Your code is likely optimal for use with multi-threading.


Don't go distributed
--------------------

Consider the dask_ and concurrent.futures_ modules, which have similar APIs to
distributed but operate on a single machine.  It may be that your problem
performs well enough on a laptop or large workstation.

Consider accelerating your code through other means than parallelism.  Better
algorithms, data structures, storage formats, or just a little bit of
C/Fortran/Numba code might be enough to give you the 10x speed boost that
you're looking for.  Parallelism and distributed computing are expensive ways
to accelerate your application.

.. _dask: https://dask.org
.. _concurrent.futures: https://docs.python.org/3/library/concurrent.futures.html
