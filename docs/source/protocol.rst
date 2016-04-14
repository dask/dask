Protocol
========

The scheduler, workers, and clients pass messages between each other.
Semantically these messages encode commands, status updates, and data, like the
following:

*  Please compute the function ``sum`` on the data ``x`` and store in ``y``
*  The computation ``y`` has been completed
*  Be advised that a new worker named ``alice`` is available for use
*  Here is the data for the keys ``'x'``, and ``'y'``

In practice we represent these messages with dictionaries/mappings::

   {'op': 'compute',
    'function': ...
    'args': ['x']}

   {'op': 'task-complete',
    'key': 'y',
    'nbytes': 26}

   {'op': 'register-worker',
    'address': '192.168.1.42',
    'name': 'alice',
    'ncores': 4}

   {'x': b'...',
    'y': b'...'}

When we communicate these messages between nodes we need to serialize these
messages down to a string of bytes that can then be deserialized on the other
end to their in-memory dictionary form.  For simple cases several options exist
like JSON, MsgPack, Protobuffers, and Thrift.  The situation is made more
complex by concerns like serializing Python functions and Python objects,
optional compression, cross-language support, large messages, and efficiency.

This document describes the protocol used by ``dask.distributed`` today.  Be
advised that this protocol changes rapidly as we continue to optimize for
performance.


Overview
--------

We may split a single message into multiple message-part to suit different
protocols.  Generally small bits of data are encoded with MsgPack while large
bytestrings are handled specially by a custom format.  Each message-part gets
its own header, which is always encoded as msgpack.  After serializing all
message parts we have a sequence of bytestrings or *frames* which we send along
the wire, prepended with length information.

The application doesn't know any of this, it just sends us Python dictionaries
with various datatypes and we produce a list of bytestrings that get written to
a socket.  This format is fast both for many frequent messages and for large
messages.


MsgPack for Messages
--------------------

Most messages are encoded with MsgPack_, a self describing semi-structured
serialization format that is very similar to JSON, but smaller, faster, not
human-readable, and supporting of bytestrings and (soon) timestamps.  We chose
MsgPack as a base serialization format for the following reasons:

*  It does not require separate headers, and so is easy and flexible to use
   which is particularly important in an early stage project like
   ``dask.distributed``
*  It is very fast, much faster than JSON, and there are nicely optimized
   implementations, particularly within the ``pandas.msgpack`` module.  With
   few exceptions (described later) MsgPack does not come anywhere near being a
   bottleneck, even under heavy use.
*  Unlike JSON it supports bytestrings
*  It covers the standard set of types necessary to encode most information
*  It is widely implemented in a number of languages (see cross language
   section below)

However, MsgPack fails (correctly) in the following ways:

*  It does not provide any way for us to encode Python functions or user
   defined data types
*  It does not support bytestrings greater than 4GB and is generally
   inefficient for very large messages.

Because of these failings we supplement it with a language-specific protocol
and a special case for large bytestrings.


CloudPickle for Functions and Data
----------------------------------

Pickle and CloudPickle are Python libraries to serialize almost any Python
object, including functions.  We use these libraries to transform the users'
functions and data into bytes before we include them in the dictionary/map that
we pass off to msgpack.  In the introductory example you may have noticed that
we skipped providing an example for the function argument::

   {'op': 'compute',
    'function': ...
    'args': ['x']}

That is because this value ``...`` will actually be the result of calling
``cloudpickle.dumps(myfunction)``.  Those bytes will then be included in the
dictionary that we send off to msgpack, which will only have to deal with
bytes rather than obscure Python functions.

Cross Language Specialization
-----------------------------

The Client and Workers must agree on a language-specific serialization format.
In the standard ``dask.distributed`` client and worker objects this ends up
being the following::

   bytes = cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
   obj = cloudpickle.loads(bytes)

This varies between Python 2 and 3 and so your client and workers must match
their Python versions and software environments.

However, the Scheduler never uses the language-specific serialization and
instead only deals with MsgPack.  If the client sends a pickled function up to
the scheduler the scheduler will not unpack function but will instead keep it
as bytes.  Eventually those bytes will be sent to a worker, which will then
unpack the bytes into a proper Python function.  Because the Scheduler never
unpacks language-specific serialized bytes it may be in a different language.

**The client and workers must share the same language and software environment,
the scheduler may differ.**

This has a few advantages:

1.  The Scheduler is protected from unpickling unsafe code
2.  The Scheduler can be run under ``pypy`` for improved performance.  This is
    only useful for larger clusters.
3.  We could conceivably implement workers and clients for other languages
    (like R or Julia) and reuse the Python scheduler.  The worker and client
    code is fairly simple and much easier to reimplement than the scheduler,
    which is complex.
4.  The scheduler might some day be rewritten in more heavily optimized C or Go

Compression
-----------

Fast compression libraries like LZ4 or Snappy may increase effective bandwidth
by compressing data before sending and decompressing it after reception.  This
is especially valuable on lower-bandwidth networks.

If either of these libraries is available (we prefer LZ4 to Snappy) then for
every message greater than 1kB we try to compress the message and, if the
compression is at least a 10% improvement, we send the compressed bytes rather
than the original payload.  We record the compression used within the header as
a string like ``'lz4'`` or ``'snappy'``.

To avoid compressing large amounts of uncompressable data we first try to
compress a sample.  We take 10kB chunks from five locations in the dataset,
arrange them together, and try compressing the result.  If this doesn't result
in significant compression then we don't try to compress the full result.


Header
------

The header is a small dictionary encoded in msgpack that includes some metadata
about the message, such as compression.


Large Bytestrings
-----------------

Whenever a message comes in with very large byte values like the following::

   {'key': 'x',
    'address': 'alice',
    'data-1': b'...'  # very long bytestring
    'data-2': b'...'  # very long bytestring
    }

We separate the message into two messages, one encoding all of the large
bytestrings, and one encoding everything else::

   {'key': 'x', 'addresss': 'alice'}
   {'data-1': b'...', 'data-2': b'...'}

The first message we pass normally with msgpack, the second we pass in multiple
parts, including a header that contains the keys and compression used for each
value::

   {'keys': ['data-1', 'data-2'],
    'compression': ['lz4', None]}
   b'...'
   b'...'


Frames
------

At the end of the pipeline we have a sequence of bytestrings or frames.  We
need to tell the receiving end how many frames there are and how long each
these frames are.  We order the frames and lengths of frames as follows:

1.  The number of frames, stored as an 8 byte unsigned integer
2.  The length of each frame, each stored as an 8 byte unsigned integer
3.  Each of the frames

In the following sections we describe how we create these frames.


Performance
-----------

For large numpy arrays we currently suffer three memory copies.  On a nice
machine this ends up being a 1-1.5 GB/s bottleneck, which is almost always
faster than the network bandwidth.  These copies come from NumPy (two
memcopies) and Tornado (one memcopy).

For small messages we generally serialize in around 5 microseconds.

.. _MsgPack: http://msgpack.org/index.html
