Data Streams with Queues
========================

The ``Executor`` methods ``scatter``, ``map``, and ``gather`` can consume and
produce standard Python ``Queue`` objects.  This is useful for processing
continuous streams of data.  However, it does not constitute a full streaming
data processing pipeline like Storm.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/hfmwXSeM8pk?rel=0"
           frameborder="0"
           allowfullscreen>
   </iframe>

Example
-------

We connect to a local Executor.

.. code-block:: python

   >>> from distributed import Executor
   >>> e = Executor('127.0.0.1:8786')
   >>> e
   <Executor: scheduler=127.0.0.1:8786 workers=1 threads=4>

We build a couple of toy data processing functions:

.. code-block:: python

   from time import sleep
   from random import random

   def inc(x):
       from random import random
       sleep(random() * 2)
       return x + 1

   def double(x):
       from random import random
       sleep(random())
       return 2 * x

And we set up an input Queue and map our functions across it.

.. code-block:: python

   >>> from queue import Queue
   >>> input_q = Queue()
   >>> remote_q = e.scatter(input_q)
   >>> inc_q = e.map(inc, remote_q)
   >>> double_q = e.map(double, inc_q)

We will fill the ``input_q`` with local data from some stream, and then
``remote_q``, ``inc_q`` and ``double_q`` will fill with ``Future`` objects as
data gets moved around.

We gather the futures from the ``double_q`` back to a queue holding local
data in the local process.

.. code-block:: python

   >>> result_q = e.gather(double_q)

Insert Data Manually
~~~~~~~~~~~~~~~~~~~~

Because we haven't placed any data into any of the queues everything is empty,
including the final output, ``result_q``.

.. code-block:: python

   >>> result_q.qsize()
   0

But when we insert an entry into the ``input_q``, it starts to make its way
through the pipeline and ends up in the ``result_q``.

.. code-block:: python

   >>> input_q.put(10)
   >>> result_q.get()
   22

Insert data in a separate thread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We simulate a slightly more realistic situation by dumping data into the
``input_q`` in a separate thread.  This simulates what you might get if you
were to read from an active data source.

.. code-block:: python

   def load_data(q):
       i = 0
       while True:
           q.put(i)
           sleep(random())
           i += 1

    >>> from threading import Thread
    >>> load_thread = Thread(target=load_data, args=(input_q,))
    >>> load_thread.start()

    >>> result_q.qsize()
    4
    >>> result_q.qsize()
    9

We consume data from the ``result_q`` and print results to the screen.

.. code-block:: python

   >>> while True:
   ...     item = result_q.get()
   ...     print(item)
   2
   4
   6
   8
   10
   12
   ...

Limitations
-----------

*  This doesn't do any sort of auto-batching of computations, so ideally you
   batch your data to take significantly longer than 1ms to run.
*  This isn't a proper streaming system.  There is no support outside of what
   you see here.  In particular there are no policies for dropping data, joining
   over time windows, etc..

Extensions
----------

We can extend this small example to more complex systems that have buffers,
split queues, merge queues, etc. all by manipulating normal Python Queues.

Here are a couple of useful function to multiplex and merge queues:

.. code-block:: python

    from queue import Queue
    from threading import Thread

    def multiplex(n, q, **kwargs):
        """ Convert one queue into several equivalent Queues

        >>> q1, q2, q3 = multiplex(3, in_q)
        """
        out_queues = [Queue(**kwargs) for i in range(n)]
        def f():
            while True:
                x = q.get()
                for out_q in out_queues:
                    out_q.put(x)
        t = Thread(target=f)
        t.daemon = True
        t.start()
        return out_queues

    def push(in_q, out_q):
        while True:
            x = in_q.get()
            out_q.put(x)

    def merge(*in_qs, **kwargs):
        """ Merge multiple queues together

        >>> out_q = merge(q1, q2, q3)
        """
        out_q = Queue(**kwargs)
        threads = [Thread(target=push, args=(q, out_q)) for q in in_qs]
        for t in threads:
            t.daemon = True
            t.start()
        return out_q

With useful functions like these we can build out more sophisticated data
processing pipelines that split off and join back together.  By creating queues
with ``maxsize=`` we can control buffering and apply back pressure.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/4IzuoV6XW_4?rel=0"
           frameborder="0"
           allowfullscreen>
   </iframe>
