Comparison to other projects
============================

IPython Parallel
----------------

Short Description
~~~~~~~~~~~~~~~~~

`IPython Parallel`_ is a distributed computing framework from the IPython
project.  It uses a centralized hub to farm out jobs to several ``ipengine``
processes running on remote workers.  It communicates over ZeroMQ sockets and
centralizes communication through the central hub.

IPython parallel has been around for a while and, while not particularly fancy,
is quite stable and robust.

IPython Parallel offers parallel ``map`` and remote ``apply`` functions that
route computations to remote workers

.. code-block:: python

   >>> view = Client(...)[:]
   >>> results = view.map(func, sequence)
   >>> result = view.apply(func, *args, **kwargs)
   >>> future = view.apply_async(func, *args, **kwargs)

It also provides direct execution of code in the remote process and collection
of data from the remote namespace.

.. code-block:: python

   >>> view.execute('x = 1 + 2')
   >>> view['x']
   [3, 3, 3, 3, 3, 3]

Brief Comparison
~~~~~~~~~~~~~~~~

Distributed and IPython Parallel are similar in that they provide ``map`` and
``apply/submit`` abstractions over distributed worker processes running Python.
Both manage the remote namespaces of those worker processes.

They are dissimilar in terms of their maturity, how worker nodes communicate to
each other, and in the complexity of algorithms that they enable.

Distributed Advantages
~~~~~~~~~~~~~~~~~~~~~~

The primary advantages of ``distributed`` over IPython Parallel include

1.  Peer-to-peer communication between workers
2.  Dynamic task scheduling

``Distributed`` workers share data in a peer-to-peer fashion, without having to
send intermediate results through a central bottleneck.  This allows
``distributed`` to be more effective for more complex algorithms and to manage
larger datasets in a more natural manner.  IPython parallel does not provide a
mechanism for workers to communicate with each other, except by using the
central node as an intermediary for data transfer or by relying on some other
medium, like a shared file system.  Data transfer through the central node can
easily become a bottleneck and so IPython parallel has been mostly helpful in
embarrassingly parallel work (the bulk of applications) but has not been used
extensively for more sophisticated algorithms that require non-trivial
communication patterns.

The distributed executor includes a dynamic task scheduler capable of managing
deep data dependencies between tasks.  The IPython parallel docs include `a
recipe`_ for executing task graphs with data dependencies.  This same idea is
core to all of ``distributed``, which uses a dynamic task scheduler for all
operations.  Notably, ``distributed.Future`` objects can be used within
``submit/map/get`` calls before they have completed.

.. code-block:: python

   >>> x = executor.submit(f, 1)  # returns a future
   >>> y = executor.submit(f, 2)  # returns a future
   >>> z = executor.submit(add, x, y)  # consumes futures

The ability to use futures cheaply within ``submit`` and ``map`` methods
enables the construction of very sophisticated data pipelines with simple code.
Additionally, distributed can serve as a full dask task scheduler, enabling
support for distributed arrays, dataframes, machine learning pipelines, and any
other application build on dask graphs.  The dynamic task schedulers within
``distributed`` are adapted from the dask_ task schedulers and so are fairly
sophisticated/efficient.

IPython Parallel Advantages
~~~~~~~~~~~~~~~~~~~~~~~~~~~

IPython Parallel has the following advantages over ``distributed``

1.  Maturity:  IPython Parallel has been around for a while.
2.  Explicit control over the worker processes:  IPython parallel
    allows you to execute arbitrary statements on the workers, allowing it to
    serve in system administration tasks.
3.  Deployment help:  IPython Parallel has mechanisms built-in to aid
    deployment on SGE, MPI, etc..  Distributed does not have any such sugar,
    though is fairly simple to :doc:`set up <setup>` by hand.
4.  Various other advantages:  Over the years IPython parallel has accrued a
    variety of helpful features like IPython interaction magics, ``@parallel``
    decorators, etc..

.. _`IPython Parallel`: http://ipython.org/ipython-doc/dev/parallel/
.. _`a recipe`: https://ipython.org/ipython-doc/3/parallel/dag_dependencies.html#dag-dependencies
.. _dask: http://dask.pydata.org/en/latest/
