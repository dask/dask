Shared Futures With Channels
============================

A channel is a changing stream of futures shared between any number of clients
connected to the same scheduler.


Examples
--------

Basic Usage
~~~~~~~~~~~
Create channels from your Client:

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')

Append futures onto a channel

.. code-block:: python

    >>> future = client.submit(add, 1, 2)
    >>> chan.append(future)

A channel maintains a collection of current futures added by both your
client, and others.

.. code-block:: python

    >>> chan.futures
    deque([<Future: status: pending, key: add-12345>,
           <Future: status: pending, key: sub-56789>])

If you wish to persist the current status of tasks outside of the distributed
cluster (e.g. to take a snapshot in case of shutdown) you can copy a channel full
of futures as it is a python deque_.

.. _deque: https://docs.python.org/3.5/library/collections.html#collections.deque`

.. code-block:: python

    channelcopy = list(chan.futures)

You can iterate over a channel to get back futures.

.. code-block:: python

    >>> anotherclient = Client('scheduler-address:8786')
    >>> chan = anotherclient.channel('my-channel')
    >>> for future in chan:
    ...     pass

When done writing, call flush to wait until your appends have been
fully registered with the scheduler.

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')
    >>> future2 = client.submit(time.sleep,2)
    >>> chan.append(future2)
    >>> chan.flush()


Example with local_client
~~~~~~~~~~~~~~~~~~~~~~~~~

Using channels with `local client`_ allows for a more decoupled version
of what is possible with :doc:`Data Streams with Queues<queues>`
in that independent worker clients can build up a set of results
which can be read later by a different client.
This opens up Dask/Distributed to being integrated in a wider application
environment similar to other python task queues such as Celery_.

.. _local client: http://distributed.readthedocs.io/en/latest/task-launch.html#submit-tasks-from-worker
.. _Celery: http://www.celeryproject.org/

.. code-block:: python

    import random, time, operator
    from distributed import Client, local_client
    from time import sleep

    def emit(name):
        with local_client() as c:
           chan = c.channel(name)
           while True:
               future = c.submit(random.random, pure=False)
               chan.append(future)
               sleep(1)

    def combine():
        with local_client() as c:
            a_chan = c.channel('a')
            b_chan = c.channel('b')
            out_chan = c.channel('adds')
            for a, b in zip(a_chan, b_chan):
                future = c.submit(operator.add, a, b)
                out_chan.append(future)

    client = Client()

    emitters = (client.submit(emit, 'a'), client.submit(emit, 'b'))
    combiner = client.submit(combine)
    chan = client.channel('adds')


    for future in chan:
        print(future.result())
       ...:
    1.782009416831722
    ...

All iterations on a channel by different clients can be stopped using the ``stop`` method

.. code-block:: python

    chan.stop()


Very short-lived clients
~~~~~~~~~~~~~~~~~~~~~~~~

If you wish to submit work to your cluster from a short lived client such as a
web application view, an AWS Lambda function or some other fire and forget script,
channels give a way to do this.

Further Details
---------------

Often it is desirable to respond to events from outside the distributed cluster
or to instantiate a new client in order to check on the progress of a set of tasks.
The channels feature makes these and many other workflows possible.

This functionality is similar to queues but
additionally means that multiple clients can send data to a long running function
rather than one client holding a queue instance.

Several clients connected to the same scheduler can communicate a sequence
of futures between each other through shared channels. All clients can
append to the channel at any time. All clients will be updated when a
channel updates. The central scheduler maintains consistency and ordering
of events. It also allows the Dask Scheduler to be extended in a clean way
using the normal Distributed task submission.

