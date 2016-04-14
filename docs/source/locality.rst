Data Locality
=============

*Data movement often needlessly limits performance.*

This is especially true for analytic computations.  ``Distributed`` minimizes
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

   futures = e.scatter(range(10))

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

   future = e.submit(func, *args, workers=['Alice'])

*  Alice: ``[0, 1, 4, 5, 8, 9, new_result]``
*  Bob: ``[2, 3, 6, 7]``

Required data will always be moved to these workers, even if the volume of that
data is significant.  If this restriction is only a preference and not a strict
requirement, then add the ``allow_other_workers`` keyword argument to signal
that in extreme cases such as when no valid worker is present, another may be
used.

.. code-block:: python

   future = e.submit(func, *args, workers=['Alice'],
                     allow_other_workers=True)

Additionally the ``scatter`` function supports a ``broadcast=`` keyword
argument to enforce that the all data is sent to all workers rather than
round-robined.  If new workers arrive they will not automatically receive this
data.

.. code-block:: python

    futures = e.scatter([1, 2, 3], broadcast=True)  # send data to all workers

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

    $ dworker scheduler_address:8786 --name worker_1

And then use this name when specifying workers instead.

.. code-block:: python

   e.map(func, sequence, workers='worker_1')

See the :doc:`efficiency <efficiency>` page to learn about best practices.
