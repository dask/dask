Launch Tasks from Tasks
=======================

Sometimes it is convenient to launch tasks from other tasks.
For example you may not know what computations to run until you have the
results of some initial computations.

Motivating example
------------------

We want to download one piece of data and turn it into a list.  Then we want to
submit one task for every element of that list.  We don't know how long the
list will be until we have the data.

So we send off our original ``download_and_convert_to_list`` function, which
downloads the data and converts it to a list on one of our worker machines:

.. code-block:: python

   future = client.submit(download_and_convert_to_list, uri)

But now we need to submit new tasks for individual parts of this data.  We have
three options.

1.  Gather the data back to the local process and then submit new jobs from the
    local process
2.  Gather only enough information about the data back to the local process and
    submit jobs from the local process
3.  Submit a task to the cluster that will submit other tasks directly from
    that worker

Gather the data locally
-----------------------

If the data is not large then we can bring it back to the client to perform the
necessary logic on our local machine:

.. code-block:: python

   >>> data = future.result()                  # gather data to local process
   >>> data                                    # data is a list
   [...]

   >>> futures = e.map(process_element, data)  # submit new tasks on data
   >>> analysis = e.submit(aggregate, futures) # submit final aggregation task

This is straightforward and, if ``data`` is small then it is probably the
simplest, and therefore correct choice.  However, if ``data`` is large then we
have to choose another option.


Submit tasks from client
------------------------

We can run small functions on our remote data to determine enough to submit the
right kinds of tasks.  In the following example we compute the ``len`` function
on ``data`` remotely and then break up data into its various elements.

.. code-block:: python

   >>> n = client.submit(len, data)            # compute number of elements
   >>> n = n.result()                          # gather n (small) locally

   >>> from operator import getitem
   >>> elements = [client.submit(getitem, data, i) for i in range(n)]  # split data

   >>> futures = client.map(process_element, elements)
   >>> analysis = client.submit(aggregate, futures)

We compute the length remotely, gather back this very small result, and then
use it to submit more tasks to break up the data and process on the cluster.
This is more complex because we had to go back and forth a couple of times
between the cluster and the local process, but the data moved was very small,
and so this only added a few milliseconds to our total processing time.

Extended Example
~~~~~~~~~~~~~~~~

Computing the Fibonacci numbers creates involves a recursive function. When the
function is run, it calls itself using values it computed. We will use this as
an example throughout this documentation to illustrate different techniques of
submitting tasks from tasks.

.. code-block:: python

   def fib(n):
       if n < 2:
           return n
       a = fib(n - 1)
       b = fib(n - 2)
       return a + b

   print(fib(10))  # prints "55"

We will use this example to show the different interfaces.

Submit tasks from worker
------------------------

*Note: this interface is new and experimental.  It may be changed without
warning in future versions.*

We can submit tasks from other tasks.  This allows us to make decisions while
on worker nodes.

To submit new tasks from a worker that worker must first create a new client
object that connects to the scheduler. There are three options for this:

1. ``dask.delayed`` and ``dask.compute``
2. ``get_client`` with ``secede`` and ``rejoin``
3. ``worker_client``


dask.delayed
~~~~~~~~~~~~

The Dask delayed behaves as normal: it submits the functions to the graph,
optimizes for less bandwidth/computation and gathers the results.  For more
detail, see `dask.delayed`_.

.. code-block:: python

    from distributed import Client
    from dask import delayed, compute


    @delayed
    def fib(n):
        if n < 2:
            return n
        # We can use dask.delayed and dask.compute to launch
        # computation from within tasks
        a = fib(n - 1)  # these calls are delayed
        b = fib(n - 2)
        a, b = compute(a, b)  # execute both in parallel
        return a + b

    if __name__ == "__main__":
        # these features require the dask.distributed scheduler
        client = Client()

        result = fib(10).compute()
        print(result)  # prints "55"

.. _dask.delayed: https://docs.dask.org/en/latest/delayed.html

Getting the client on a worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :py:func:`get_client <distributed.get_client>` function provides a normal
Client object that gives full access to the dask cluster, including the ability
to submit, scatter, and gather results.

.. code-block:: python

    from distributed import Client, get_client, secede, rejoin

    def fib(n):
        if n < 2:
            return n
        client = get_client()
        a_future = client.submit(fib, n - 1)
        b_future = client.submit(fib, n - 2)
        a, b = client.gather([a_future, b_future])
        return a + b

    if __name__ == "__main__":
        client = Client()
        future = client.submit(fib, 10)
        result = future.result()
        print(result)  # prints "55"

However, this can deadlock the scheduler if too many tasks request jobs at
once. Each task does not communicate to the scheduler that they are waiting on
results and are free to compute other tasks. This can deadlock the cluster if
every scheduling slot is running a task and they all request more tasks.

To avoid this deadlocking issue we can use :py:func:`secede
<distributed.secede>` and :py:func:`rejoin <distributed.rejoin>`. These
functions will remove and rejoin the current task from the cluster
respectively.

.. code-block:: python

    def fib(n):
        if n < 2:
            return n
        client = get_client()
        a_future = client.submit(fib, n - 1)
        b_future = client.submit(fib, n - 2)
        secede()
        a, b = client.gather([a_future, b_future])
        rejoin()
        return a + b

Connection with context manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :py:func:`worker_client <distributed.worker_client>` function performs the
same task as :py:func:`get_client <distributed.get_client>`, but is implemented
as a context manager.  Using :py:func:`worker_client
<distributed.worker_client>` as a context manager ensures proper cleanup on the
worker.

.. code-block:: python

    from dask.distributed import worker_client


    def fib(n):
        if n < 2:
            return n
         with worker_client() as client:
             a_future = client.submit(fib, n - 1)
             b_future = client.submit(fib, n - 2)
             a, b = client.gather([a_future, b_future])
         return a + b

    if __name__ == "__main__":
        client = Client()
        future = client.submit(fib, 10)
        result = future.result()
        print(result)  # prints "55"

Tasks that invoke :py:func:`worker_client <distributed.worker_client>` are
conservatively assumed to be *long running*.  They can take a long time,
waiting for other tasks to finish, gathering results, etc. In order to avoid
having them take up processing slots the following actions occur whenever a
task invokes :py:func:`worker_client <distributed.worker_client>`.

1.  The thread on the worker running this function *secedes* from the thread
    pool and goes off on its own.  This allows the thread pool to populate that
    slot with a new thread and continue processing additional tasks without
    counting this long running task against its normal quota.
2.  The Worker sends a message back to the scheduler temporarily increasing its
    allowed number of tasks by one.  This likewise lets the scheduler allocate
    more tasks to this worker, not counting this long running task against it.

Establishing a connection to the scheduler takes a few milliseconds and so it
is wise for computations that use this feature to be at least a few times
longer in duration than this.
