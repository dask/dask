Worker Resources
================

Access to scarce resources like memory, GPUs, or special hardware may constrain
how many of certain tasks can run on particular machines.

For example, we may have a cluster with ten computers, four of which have two
GPUs each.  We may have a thousand tasks, a hundred of which require a GPU and
ten of which require two GPUs at once.  In this case we want to balance tasks
across the cluster with these resource constraints in mind, allocating
GPU-constrained tasks to GPU-enabled workers.  Additionally we need to be sure
to constrain the number of GPU tasks that run concurrently on any given worker
to ensure that we respect the provided limits.

This situation arises not only for GPUs but for many resources like tasks that
require a large amount of memory at runtime, special disk access, or access to
special hardware.  Dask allows you to specify abstract arbitrary resources to
constrain how your tasks run on your workers.  Dask does not model these
resources in any particular way (Dask does not know what a GPU is) and it is up
to the user to specify resource availability on workers and resource demands on
tasks.

Example
-------

We consider a computation where we load data from many files, process each one
with a function that requires a GPU, and then aggregate all of the intermediate
results with a task that takes up 70GB of memory.

We operate on a three-node cluster that has two machines with two GPUs each and
one machine with 100GB of RAM.

When we set up our cluster we define resources per worker::

   dask-worker scheduler:8786 --resources "GPU=2"
   dask-worker scheduler:8786 --resources "GPU=2"
   dask-worker scheduler:8786 --resources "MEMORY=100e9"

When we submit tasks to the cluster we specify constraints per task

.. code-block:: python

   from distributed import Client
   client = Client('scheduler:8786')

   data = [client.submit(load, fn) for fn in filenames]
   processed = [client.submit(process, d, resources={'GPU': 1}) for d in data]
   final = client.submit(aggregate, processed, resources={'MEMORY': 70e9})


Resources are applied separately to each worker process
-------------------------------------------------------

If you are using ``dask-worker --nprocs <nprocs>`` the resource will be applied
separately to each of the ``nprocs`` worker processes. Suppose you have 2 GPUs
on your machine, if you want to use two worker processes, you have 1 GPU per
worker process so you need to do something like this::

   dask-worker scheduler:8786 --nprocs 2 --resources "GPU=1"

Here is an example that illustrates how to use resources to ensure each task is
run inside a separate process, which is useful to execute non thread-safe tasks
or tasks that uses multithreading internally::

   dask-worker scheduler:8786 --nprocs 3 --nthreads 2 --resources "process=1"

With the code below, there will be at most 3 tasks running concurrently and
each task will run in a separate process:

.. code-block:: python

   from distributed import Client
   client = Client('scheduler:8786')

   futures = [client.submit(non_thread_safe_function, arg,
                            resources={'process': 1}) for arg in args]


Resources are Abstract
----------------------

Resources listed in this way are just abstract quantities.  We could equally
well have used terms "mem", "memory", "bytes" etc. above because, from Dask's
perspective, this is just an abstract term.  You can choose any term as long as
you are consistent across workers and clients.

It's worth noting that Dask separately track number of cores and available
memory as actual resources and uses these in normal scheduling operation.


Resources with collections
--------------------------

You can also use resources with Dask collections, like arrays, dataframes, and
delayed objects.  You can pass a dictionary mapping keys of the collection to
resource requirements during compute or persist calls.

.. code-block:: python

    x = dd.read_csv(...)
    y = x.map_partitions(func1)
    z = y.map_partitions(func2)

    z.compute(resources={tuple(y.__dask_keys__()): {'GPU': 1})

In some cases (such as the case above) the keys for ``y`` may be optimized away
before execution.  You can avoid that either by requiring them as an explicit
output, or by passing the ``optimize_graph=False`` keyword.


.. code-block:: python

    z.compute(resources={tuple(y.__dask_keys__()): {'GPU': 1}, optimize_graph=False)
