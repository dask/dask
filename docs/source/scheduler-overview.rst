Scheduler Overview
==================

After we create a dask graph, we use a scheduler to run it. Dask currently
implements a few different schedulers:

- ``dask.threaded.get``: a scheduler backed by a thread pool
- ``dask.multiprocessing.get``: a scheduler backed by a process pool
- ``dask.get``: a synchronous scheduler, good for debugging
- ``distributed.Client.get``: a distributed scheduler for executing graphs
   on multiple machines.  This lives in the external distributed_ project.

.. _distributed: https://distributed.dask.org/en/latest/


The ``get`` function
--------------------

The entry point for all schedulers is a ``get`` function. This takes a dask
graph, and a key or list of keys to compute:

.. code-block:: python

   >>> from operator import add

   >>> dsk = {'a': 1,
   ...        'b': 2,
   ...        'c': (add, 'a', 'b'),
   ...        'd': (sum, ['a', 'b', 'c'])}

   >>> get(dsk, 'c')
   3

   >>> get(dsk, 'd')
   6

   >>> get(dsk, ['a', 'b', 'c'])
   [1, 2, 3]


Using ``compute`` methods
-------------------------

When working with dask collections, you will rarely need to
interact with scheduler ``get`` functions directly. Each collection has a
default scheduler, and a built-in ``compute`` method that calculates the output
of the collection:

.. code-block:: python

    >>> import dask.array as da
    >>> x = da.arange(100, chunks=10)
    >>> x.sum().compute()
    4950

The compute method takes a number of keywords:

- ``scheduler``: the name of the desired scheduler like ``"threads"``, ``"processes"``, or ``"single-threaded"`, a ``get`` function, or a ``dask.distributed.Client`` object.  Overrides the default for the collection.
- ``**kwargs``: extra keywords to pass on to the scheduler ``get`` function.

See also: :ref:`configuring-schedulers`.


The ``compute`` function
------------------------

You may wish to compute results from multiple dask collections at once.
Similar to the ``compute`` method on each collection, there is a general
``compute`` function that takes multiple collections and returns multiple
results. This merges the graphs from each collection, so intermediate results
are shared:

.. code-block:: python

    >>> y = (x + 1).sum()
    >>> z = (x + 1).mean()
    >>> da.compute(y, z)    # Compute y and z, sharing intermediate results
    (5050, 50.5)

Here the ``x + 1`` intermediate was only computed once, while calling
``y.compute()`` and ``z.compute()`` would compute it twice. For large graphs
that share many intermediates, this can be a big performance gain.

The ``compute`` function works with any dask collection, and is found in
``dask.base``. For convenience it has also been imported into the top level
namespace of each collection.

.. code-block:: python

    >>> from dask.base import compute
    >>> compute is da.compute
    True


.. _configuring-schedulers:

Configuring the schedulers
--------------------------

The dask collections each have a default scheduler:

- ``dask.array`` and ``dask.dataframe`` use the threaded scheduler by default
- ``dask.bag`` uses the multiprocessing scheduler by default.

For most cases, the default settings are good choices. However, sometimes you
may want to use a different scheduler. There are two ways to do this.

1. Using the ``get`` keyword in the ``compute`` method:

    .. code-block:: python

        >>> x.sum().compute(scheduler='processes')

2. Using ``dask.config.set``. This can be used either as a context manager, or to
   set the scheduler globally:

    .. code-block:: python

        # As a context manager
        >>> with dask.config.set(scheduler='processes'):
        ...     x.sum().compute()

        # Set globally
        >>> dask.config.set(scheduler='processes')
        >>> x.sum().compute()


Additionally, each scheduler may take a few extra keywords specific to that
scheduler. For example, the multiprocessing and threaded schedulers each take a
``num_workers`` keyword, which sets the number of processes or threads to use
(defaults to number of cores). This can be set by passing the keyword when
calling ``compute``:

.. code-block:: python

    # Compute with 4 threads
    >>> x.compute(num_workers=4)

Alternatively, the multiprocessing and threaded schedulers will check for a
global pool set with ``dask.config.set``:

.. code-block:: python

    >>> from multiprocessing.pool import ThreadPool
    >>> with dask.config.set(pool=ThreadPool(4)):
    ...     x.compute()

The multiprocessing scheduler also supports `different contexts`_ ("spawn",
"forkserver", "fork") which you can set with ``dask.config.set``:

.. code-block:: python

   >>> with dask.config.set({"multiprocessing.context": "forkserver"}):
   ...     x.compute()

.. _different contexts: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods

For more information on the individual options for each scheduler, see the
docstrings for each scheduler ``get`` function.


Debugging the schedulers
------------------------

Debugging parallel code can be difficult, as conventional tools such as ``pdb``
don't work well with multiple threads or processes. To get around this when
debugging, we recommend using the synchronous scheduler found at
``dask.get``. This runs everything serially, allowing it to work
well with ``pdb``:

.. code-block:: python

    >>> dask.config.set(scheduler='single-threaded')
    >>> x.sum().compute()    # This computation runs serially instead of in parallel


The shared memory schedulers also provide a set of callbacks that can be used
for diagnosing and profiling. You can learn more about scheduler callbacks and
diagnostics :doc:`here <diagnostics-local>`.


More Information
----------------

- See :doc:`shared` for information on the design of the shared memory
  (threaded or multiprocessing) schedulers
- See distributed_ for information on the distributed memory scheduler
