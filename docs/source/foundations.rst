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


Network Communication
=====================

Workers, the Scheduler, and Clients communicate with each other over the
network.  By default they use TCP connections as mediated by the abstract
communications layer.  The basic unit for dealing with established
communications is the ``Comm`` object:

.. autoclass:: distributed.comm.Comm
   :members:

You don't create ``Comm`` objects directly: you either ``listen`` for
incoming communications, or ``connect`` to a peer listening for connections:

.. autofunction:: distributed.comm.connect

.. autofunction:: distributed.comm.listen

Listener objects expose the following interface:

.. autoclass:: distributed.comm.core.Listener
   :members:


Addresses
---------

Communication addresses are canonically represented as URIs, such as
``tcp://127.0.0.1:1234``.  For compatibility with existing code, if the
URI scheme is omitted, a default scheme of ``tcp`` is assumed (so
``127.0.0.1:456`` is really the same as ``tcp://127.0.0.1:456``).
The default scheme may change in the future.

The following schemes are currently implemented in the ``distributed``
source tree:

* ``tcp`` is the main transport; it uses TCP sockets and allows for IPv4
  and IPv6 addresses.

* ``zmq`` is an experimental transport using ZeroMQ sockets; it is not
  recommended for production use.

Note that some URIs may be valid for listening but not for connecting.
For example, the URI ``tcp://`` will listen on all IPv4 and IPv6 addresses
and on an arbitrary port, but you cannot connect to that address.

Higher-level APIs in ``distributed`` may accept other address formats for
convenience or compatibility, for example a ``(host, port)`` pair.  However,
the abstract communications layer always deals with URIs.


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
   s.listen(8888)

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
       # comm = yield connect('127.0.0.1', 8888)
       # yield comm.write({'op': 'add', 'x': 1, 'y': 2})
       # result = yield comm.read()
       r = rpc('tcp://127.0.0.1:8888')
       result = yield r.add(x=1, y=2)
       r.close_comms()

       print(result)

   >>> IOLoop().run_sync(f)
   3

