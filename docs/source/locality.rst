Data Locality
=============

*Data movement often needlessly limits performance.*

This is especially true for analytic computations.  Dask.distributed minimizes
data movement when possible and enables the user to take control when
necessary.  This document describes current scheduling policies and user API
around data locality.

Current Policies
----------------

Task Submission
```````````````

In the common case distributed runs tasks on workers that already hold
dependent data.  If you have a task ``f(x)`` that requires some data ``x`` then
that task will very likely be run on the worker that already holds ``x``.

If a task requires data split among multiple workers, then the scheduler chooses
to run the task on the worker that requires the least data transfer to it.
The size of each data element is measured by the workers using the
``sys.getsizeof`` function, which depends on the ``__sizeof__`` protocol
generally available on most relevant Python objects.

Data Scatter
````````````

When a user scatters data from their local process to the distributed network
this data is distributed in a round-robin fashion grouping by number of cores.
So for example If we have two workers ``Alice`` and ``Bob``, each with two
cores and we scatter out the list ``range(10)`` as follows:

.. code-block:: python

   futures = client.scatter(range(10))

Then Alice and Bob receive the following data

*  Alice: ``[0, 1, 4, 5, 8, 9]``
*  Bob: ``[2, 3, 6, 7]``


User Control
------------

Complex algorithms may require more user control.

For example the existence of specialized hardware such as GPUs or database
connections may restrict the set of valid workers for a particular task.

In these cases use the ``workers=`` keyword argument to the ``submit``,
``map``, or ``scatter`` functions, providing a hostname, IP address, or alias
as follows:

.. code-block:: python

   future = client.submit(func, *args, workers=['Alice'])

*  Alice: ``[0, 1, 4, 5, 8, 9, new_result]``
*  Bob: ``[2, 3, 6, 7]``

Required data will always be moved to these workers, even if the volume of that
data is significant.  If this restriction is only a preference and not a strict
requirement, then add the ``allow_other_workers`` keyword argument to signal
that in extreme cases such as when no valid worker is present, another may be
used.

.. code-block:: python

   future = client.submit(func, *args, workers=['Alice'],
                          allow_other_workers=True)

Additionally the ``scatter`` function supports a ``broadcast=`` keyword
argument to enforce that the all data is sent to all workers rather than
round-robined.  If new workers arrive they will not automatically receive this
data.

.. code-block:: python

    futures = client.scatter([1, 2, 3], broadcast=True)  # send data to all workers

*  Alice: ``[1, 2, 3]``
*  Bob: ``[1, 2, 3]``


Valid arguments for ``workers=`` include the following:

*  A single IP addresses, IP/Port pair, or hostname like the following::

      192.168.1.100, 192.168.1.100:8989, alice, alice:8989

*  A list or set of the above::

      ['alice'], ['192.168.1.100', '192.168.1.101:9999']

If only a hostname or IP is given then any worker on that machine will be
considered valid.  Additionally, you can provide aliases to workers upon
creation.::

    $ dask-worker scheduler_address:8786 --name worker_1

And then use this name when specifying workers instead.

.. code-block:: python

   client.map(func, sequence, workers='worker_1')


Specify workers with Compute/Persist
------------------------------------

The ``workers=`` keyword in ``scatter``, ``submit``, and ``map`` is fairly
straightforward, taking either a worker hostname, host:port pair or a sequence
of those as valid inputs:

.. code-block:: python

   client.submit(f, x, workers='127.0.0.1')
   client.submit(f, x, workers='127.0.0.1:55852')
   client.submit(f, x, workers=['192.168.1.101', '192.168.1.100'])

For more complex computations, such as occur with dask collections like
dask.dataframe or dask.delayed, we sometimes want to specify that certain parts
of the computation run on certain workers while other parts run on other
workers.

.. code-block:: python

   x = delayed(f)(1)
   y = delayed(f)(2)
   z = delayed(g)(x, y)

   future = client.compute(z, workers={z: '127.0.0.1',
                                       x: '192.168.0.1'})

Here the values of the dictionary are of the same form as before, a host, a
host:port pair, or a list of these.  The keys in this case are either dask
collections or tuples of dask collections.  All of the *final* keys of these
collections will run on the specified machines; dependencies can run anywhere
unless they are also listed in ``workers=``.  We explore this through a set of
examples:

The computation ``z = f(x, y)`` runs on the host ``127.0.0.1``.  The other
two computations for ``x`` and ``y`` can run anywhere.

.. code-block:: python

   future = client.compute(z, workers={z: '127.0.0.1'})


The computations for both ``z`` and ``x`` must run on ``127.0.0.1``

.. code-block:: python

   future = client.compute(z, workers={z: '127.0.0.1',
                                       x: '127.0.0.1'})

Use a tuple to group collections.  This is shorthand for the above.

.. code-block:: python

   future = client.compute(z, workers={(x, y): '127.0.0.1'})

Recall that all options for ``workers=`` in ``scatter/submit/map`` hold here as
well.

.. code-block:: python

   future = client.compute(z, workers={(x, y): ['192.168.1.100', '192.168.1.101:9999']})

Set ``allow_other_workers=True`` to make these loose restrictions rather than
hard requirements.

.. code-block:: python

   future = client.compute(z, workers={(x, y): '127.0.0.1'},
                           allow_other_workers=True)

Provide a collection to ``allow_other_workers=[...]`` to say that
the keys for only some of the collections are loose.  In the case below ``z``
*must* run on ``127.0.0.1`` while ``x`` *should* run on ``127.0.0.1`` but can
run elsewhere if necessary:

.. code-block:: python

   future = client.compute(z, workers={(x, y): '127.0.0.1'},
                           allow_other_workers=[x])

This works fine with ``persist`` and with any dask collection (any object with
a ``.__dask_graph__()`` method):

.. code-block:: python

   df = dd.read_csv('s3://...')
   df = client.persist(df, workers={df: ...})

See the :doc:`efficiency <efficiency>` page to learn about best practices.
