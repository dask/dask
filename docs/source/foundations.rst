Foundations
===========

You should read through the :doc:`quickstart <quickstart>` before reading this document.

.. _quickstart: quickstart.html

Distributed computing is hard for two reasons:

1.  Consistent coordination of distributed systems requires sophistication
2.  Concurrent network programming is tricky and error prone

The foundations of ``distributed`` provide abstractions to hide some
complexity of concurrent network programming (#2).  These abstractions ease the
construction of sophisticated parallel systems (#1) in a safer envirotnment.
However, as with all layered abstractions, ours has flaws.  Critical feedback
is welcome.


Concurrency with Tornado Coroutines
-----------------------------------

Worker and Scheduler nodes operate concurrently.  They serve several overlapping
requests and perform several overlapping computations at the same time without
blocking.  There are several approaches for concurrent programming, we've
chosen to use Tornado for the following reasons:

1.  Developing and debugging is more comfortable without threads
2.  `Tornado's documentation`_ is excellent
3.  Stackoverflow coverage is excellent
4.  Performance is satisfactory

.. _`Tornado's documentation`: http://tornado.readthedocs.io/en/latest/coroutine.html


Communication with Tornado Streams (raw sockets)
------------------------------------------------

Workers, the Scheduler, and clients communicate with each other over the
network.  They use *raw sockets* as mediated by tornado streams.  We separate
messages by a sentinel value.

.. autofunction:: distributed.core.read
.. autofunction:: distributed.core.write


Servers
-------

Worker and Scheduler nodes serve requests over TCP.  Both Worker and Scheduler
objects inherit from a ``Server`` class.  This Server class thinly wraps
``tornado.tcpserver.TCPServer``.  These servers expect requests of a particular
form.

.. autoclass:: distributed.core.Server


RPC
---

To interact with remote servers we typically use ``rpc`` objects.

.. autoclass:: distributed.core.rpc


Example
-------

Here is a small example using distributed.core to create and interact with a
custom server.


Server Side
~~~~~~~~~~~

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import write, Server

   def add(stream, x=None, y=None):  # simple handler, just a function
       return x + y

   @gen.coroutine
   def stream_data(stream, interval=1):  # complex handler, multiple responses
       data = 0
       while True:
           yield gen.sleep(interval)
           data += 1
           yield write(stream, data)

   s = Server({'add': add, 'stream': stream_data})
   s.listen(8888)

   IOLoop.current().start()


Client Side
~~~~~~~~~~~

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import connect, read, write

   @gen.coroutine
   def f():
       stream = yield connect('127.0.0.1', 8888)
       yield write(stream, {'op': 'add', 'x': 1, 'y': 2})
       result = yield read(stream)
       print(result)

   >>> IOLoop().run_sync(f)
   3

   @gen.coroutine
   def g():
       stream = yield connect('127.0.0.1', 8888)
       yield write(stream, {'op': 'stream', 'interval': 1})
       while True:
           result = yield read(stream)
           print(result)

   >>> IOLoop().run_sync(g)
   1
   2
   3
   ...


Client Side with rpc
~~~~~~~~~~~~~~~~~~~~

RPC provides a more pythonic interface.  It also provides other benefits, such
as using multiple streams in concurrent cases.  Most distributed code uses
rpc.  The exception is when we need to perform multiple reads or writes, as
with the stream data case above.

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import rpc

   @gen.coroutine
   def f():
       # stream = yield connect('127.0.0.1', 8888)
       # yield write(stream, {'op': 'add', 'x': 1, 'y': 2})
       # result = yield read(stream)
       r = rpc(ip='127.0.0.1', 8888)
       result = yield r.add(x=1, y=2)

       print(result)

   >>> IOLoop().run_sync(f)
   3

Everything is a Server
----------------------

Workers, Scheduler, and Nanny objects all inherit from Server.  Each maintains
separate state and serves separate functions but all communicate in the way
shown above.  They talk to each other by opening connections, writing messages
that trigger remote functions, and then collect the results with read.
