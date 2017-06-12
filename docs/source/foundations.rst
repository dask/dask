===========
Foundations
===========

You should read through the :doc:`quickstart <quickstart>` before reading this document.

.. _quickstart: quickstart.html

Distributed computing is hard for two reasons:

1.  Consistent coordination of distributed systems requires sophistication
2.  Concurrent network programming is tricky and error prone

The foundations of ``dask.distributed`` provide abstractions to hide some
complexity of concurrent network programming (#2).  These abstractions ease the
construction of sophisticated parallel systems (#1) in a safer environment.
However, as with all layered abstractions, ours has flaws.  Critical feedback
is welcome.


Concurrency with Tornado Coroutines
===================================

Worker and Scheduler nodes operate concurrently.  They serve several overlapping
requests and perform several overlapping computations at the same time without
blocking.  There are several approaches for concurrent programming, we've
chosen to use Tornado for the following reasons:

1.  Developing and debugging is more comfortable without threads
2.  `Tornado's documentation`_ is excellent
3.  Stackoverflow coverage is excellent
4.  Performance is satisfactory

.. _`Tornado's documentation`: https://tornado.readthedocs.io/en/latest/coroutine.html


Endpoint-to-endpoint Communication
==================================

The various distributed endpoints (Client, Scheduler, Worker) communicate
by sending each other arbitrary Python objects.  Encoding, sending and then
decoding those objects is the job of the :ref:`communication layer <communications>`.

Ancillary services such as a Bokeh-based Web interface, however, have their
own implementation and semantics.


Protocol Handling
=================

While the abstract communication layer can transfer arbitrary Python
objects (as long as they are serializable),  participants in a ``distributed``
cluster concretely obey the distributed :ref:`protocol`, which specifies
request-response semantics using a well-defined message format.

Dedicated infrastructure in ``distributed`` handles the various aspects
of the protocol, such as dispatching the various operations supported by
an endpoint.

Servers
-------

Worker, Scheduler, and Nanny objects all inherit from a ``Server`` class.

.. autoclass:: distributed.core.Server


RPC
---

To interact with remote servers we typically use ``rpc`` objects which
expose a familiar method call interface to invoke remote operations.

.. autoclass:: distributed.core.rpc


Examples
========

Here is a small example using distributed.core to create and interact with a
custom server.


Server Side
-----------

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import Server

   def add(comm, x=None, y=None):  # simple handler, just a function
       return x + y

   @gen.coroutine
   def stream_data(comm, interval=1):  # complex handler, multiple responses
       data = 0
       while True:
           yield gen.sleep(interval)
           data += 1
           yield comm.write(data)

   s = Server({'add': add, 'stream_data': stream_data})
   s.listen('tcp://:8888')   # listen on TCP port 8888

   IOLoop.current().start()


Client Side
-----------

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import connect

   @gen.coroutine
   def f():
       comm = yield connect('tcp://127.0.0.1:8888')
       yield comm.write({'op': 'add', 'x': 1, 'y': 2})
       result = yield comm.read()
       yield comm.close()
       print(result)

   >>> IOLoop().run_sync(f)
   3

   @gen.coroutine
   def g():
       comm = yield connect('tcp://127.0.0.1:8888')
       yield comm.write({'op': 'stream_data', 'interval': 1})
       while True:
           result = yield comm.read()
           print(result)

   >>> IOLoop().run_sync(g)
   1
   2
   3
   ...


Client Side with ``rpc``
------------------------

RPC provides a more pythonic interface.  It also provides other benefits, such
as using multiple streams in concurrent cases.  Most distributed code uses
``rpc``.  The exception is when we need to perform multiple reads or writes, as
with the stream data case above.

.. code-block:: python

   from tornado import gen
   from tornado.ioloop import IOLoop
   from distributed.core import rpc

   @gen.coroutine
   def f():
       # comm = yield connect('tcp://127.0.0.1', 8888)
       # yield comm.write({'op': 'add', 'x': 1, 'y': 2})
       # result = yield comm.read()
       r = rpc('tcp://127.0.0.1:8888')
       result = yield r.add(x=1, y=2)
       r.close_comms()

       print(result)

   >>> IOLoop().run_sync(f)
   3

