Shared Futures With Channels
============================

A channel is a changing stream of data or futures shared between any number of
clients connected to the same scheduler.

Any client can push msgpack-encodable data (numbers, strings, lists, dicts) or
Future objects into a channel.  All other clients listening to the channel will
receive that data or future as a linear sequence.  Communication happens
through the scheduler, which linearizes everything.  Channels should only be
used to move small amounts of administrative data.  For larger data, create
futures and send the futures instead.


Examples
--------

Basic Usage
~~~~~~~~~~~

Create channels from your Client:

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')

Append futures or data onto a channel

.. code-block:: python

    >>> future = client.submit(add, 1, 2)
    >>> chan.append(future)
    >>> chan.append({'x': 123})

A channel maintains a collection of current futures added by both your
client, and others.

.. code-block:: python

    >>> chan.data
    deque([<Future: status: pending, key: add-12345>,
           {'x': 123}])

If you wish to persist the current status of tasks outside of the distributed
cluster (e.g. to take a snapshot in case of shutdown) you can copy a channel full
of data as it is a python deque_.

.. _deque: https://docs.python.org/3.5/library/collections.html#collections.deque`

.. code-block:: python

    channelcopy = list(chan.data)

You can iterate over a channel to get back data.

.. code-block:: python

    >>> anotherclient = Client('scheduler-address:8786')
    >>> chan = anotherclient.channel('my-channel')
    >>> for element in chan:
    ...     pass

When done writing, call flush to wait until your appends have been
fully registered with the scheduler.

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')
    >>> future2 = client.submit(time.sleep, 2)
    >>> chan.append(future2)
    >>> chan.flush()


Example with worker_client
~~~~~~~~~~~~~~~~~~~~~~~~~~

Using channels with `worker_client`_ allows for a more decoupled version
of what is possible with :doc:`Data Streams with Queues<queues>`
in that independent worker clients can build up a set of results
which can be read later by a different client.
This opens up Dask/Distributed to being integrated in a wider application
environment similar to other python task queues such as Celery_.

.. _worker_client: http://distributed.readthedocs.io/en/latest/task-launch.html#submit-tasks-from-worker
.. _Celery: http://www.celeryproject.org/

.. code-block:: python

    import random, time, operator
    from distributed import Client, worker_client
    from time import sleep

    def emit(name):
        with worker_client() as c:
           chan = c.channel(name)
           while True:
               future = c.submit(random.random, pure=False)
               chan.append(future)
               sleep(1)

    def combine():
        with worker_client() as c:
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


Additional Applications
-----------------------

Channels can serve as a coordination point or semaphore.  They can signal
stopping criteria for iterative processes.

Short lived clients, such as occur when firing off controlling tasks from a web
application, AWS Lambda, or other fire and forget script, often need a place to
store their futures so that in-flight work doesn't get garbage collected.
Because channels act as clients for the purpose of garbage collection (all
futures within a Channel are considered desired) they can serve as this
repository after short-lived clients die off.

Worker clients can communicate large amounts of data to each other using
channels by first scattering local data to themselves, creating futures, and
then pushing those futures down a shared channel.  When subscribers to the
channel gather these futures they will engage the normal high-bandwidth
inter-worker communication mechanism.
