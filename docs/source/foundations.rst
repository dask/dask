Foundations
===========

You should read through the quickstart_ before reading this document.  This
document is for developers of ``distributed``, not users.

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

Worker and Center nodes operate concurrently.  They serve several overlapping
requests and perform several overlapping computations at the same time without
blocking.  There are several approaches for concurrent programming, we've
chosen to use Tornado for the following reasons:

1.  Developing and debugging is more comfortable without threads
2.  `Tornado's documentation`_ is excellent
3.  Stackoverflow coverage is excellent
4.  Performance is satisfactory

.. _`Tornado's documentation`: http://tornado.readthedocs.org/en/latest/coroutine.html


Communication with Tornado Streams (raw sockets)
------------------------------------------------

Workers, the Center, and clients must all communicate with each other over the
network.  We use *raw sockets* as mediated by tornado streams.  We separate
messages by a sentinel value.

.. autofunction:: distributed.core.read
.. autofunction:: distributed.core.write


Servers
-------

The Worker and Center nodes serve requests over TCP.  Both the Worker and
Center objects inherit from a ``Server`` class.  This Server class thinly wraps
``tornado.tcpserver.TCPServer``.  These servers expect requests of a
particular form.

.. autoclass:: distributed.core.Server


RPC
---

To interact with remote servers we typically use ``rpc`` objects.

.. autoclass:: distributed.core.rpc
