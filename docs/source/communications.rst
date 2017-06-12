.. _communications:

==============
Communications
==============

Workers, the Scheduler, and Clients communicate by sending each other
Python objects (such as :ref:`protocol` messages or user data).
The communication layer handles appropriate encoding and shipping
of those Python objects between the distributed endpoints.  The
communication layer is able to select between different transport
implementations, depending on user choice or (possibly) internal
optimizations.

The communication layer lives in the :mod:`distributed.comm` package.


Addresses
=========

Communication addresses are canonically represented as URIs, such as
``tcp://127.0.0.1:1234``.  For compatibility with existing code, if the
URI scheme is omitted, a default scheme of ``tcp`` is assumed (so
``127.0.0.1:456`` is really the same as ``tcp://127.0.0.1:456``).
The default scheme may change in the future.

The following schemes are currently implemented in the ``distributed``
source tree:

* ``tcp`` is the main transport; it uses TCP sockets and allows for IPv4
  and IPv6 addresses.

* ``tls`` is a secure transport using the well-known `TLS protocol`_ over
  TCP sockets.  Using it requires specifying keys and
  certificates as outlined in :ref:`tls`.

* ``inproc`` is an in-process transport using simple object queues; it
  eliminates serialization and I/O overhead, providing almost zero-cost
  communication between endpoints as long as they are situated in the
  same process.

Some URIs may be valid for listening but not for connecting.
For example, the URI ``tcp://`` will listen on all IPv4 and IPv6 addresses
and on an arbitrary port, but you cannot connect to that address.

Higher-level APIs in ``distributed`` may accept other address formats for
convenience or compatibility, for example a ``(host, port)`` pair.  However,
the abstract communications layer always deals with URIs.


.. _TLS protocol: https://en.wikipedia.org/wiki/Transport_Layer_Security


Functions
---------

There are a number of top-level functions in :mod:`distributed.comm`
to help deal with addresses:

.. autofunction:: distributed.comm.parse_address

.. autofunction:: distributed.comm.unparse_address

.. autofunction:: distributed.comm.normalize_address

.. autofunction:: distributed.comm.resolve_address

.. autofunction:: distributed.comm.get_address_host


Communications API
==================

The basic unit for dealing with established communications is the ``Comm``
object:

.. autoclass:: distributed.comm.Comm
   :members:

You don't create ``Comm`` objects directly: you either ``listen`` for
incoming communications, or ``connect`` to a peer listening for connections:

.. autofunction:: distributed.comm.connect

.. autofunction:: distributed.comm.listen

Listener objects expose the following interface:

.. autoclass:: distributed.comm.core.Listener
   :members:


Extending the Communication Layer
=================================

Each transport is represented by a URI scheme (such as ``tcp``) and
backed by a dedicated :class:`Backend` implementation, which provides
entry points into all transport-specific routines.


.. autoclass:: distributed.comm.registry.Backend
   :members:
