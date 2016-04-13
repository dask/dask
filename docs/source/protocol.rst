Protocol
========

The Scheduler, Workers, and Clients must pass messages between each other.
Semantically these messages encode commands, status updates, and data, like the
following:

*  Please compute the function ``sum`` on the data ``x`` and store in ``y``
*  The computation ``y`` has been completed
*  Be advised that a new worker named ``alice`` is available for use

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

When we communicate these messages between nodes we need to serialize these
messages down to a string of bytes that can then be deserialized on the other
end to their in-memory dictionary form.  For simple cases several options exist
like JSON, MsgPack, Protobuffers, and Thrift.  The situation is made more
complex by concerns like serializing Python functions and Python objects,
optional compression, cross-language support, and efficiency.

This document describes the protocol used by ``dask.distributed`` today.
Be advised that this protocol is rapidly changing as we continue to optimize
for performance.

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

However, msgpack does not provide any way for us to encode Python functions or
user defined data structures (objects), and so must be complemented with a
language-specific protocol

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


Header
------

The header is a small dictionary encoded in msgpack that includes some metadata
about the message, such as compression.


Frames
------

Every message often has a header and a main body, both encoded as msgpack, the
latter of which is possibly compressed.  We send these bytestrings one after
another, each prepended with a length, encoded as an eight byte unsigned
integer (typecode ``'L'`` in the Python ``struct`` module).


All together
------------

And so our final message looks like the following::

   0-8:         Eight byte encoded length of header, N
   8-N+8:       Header, encoded as msgpack
   N+8:N+16:    Eight byte encoded length of message, M
   N+16:N+M+16: Message, encoded as msgpack, possibly compressed

.. _MsgPack: http://msgpack.org/index.html

Copies
------

Current performance is limited mostly by msgpack and making copies.  For
example for large array data our data gets copied in the following stages:

1.  From numpy array to bytes with ``pickle``, around 1000 MB/s
2.  Include pickled bytes in msgpack dictionary, around 500 MB/s
3.  Pass bytes through Tornado, which currently does another memory copy,
    around 2000 MB/s

While each stage is fairly high bandwidth the chain can add up to slow things
down considerably.  In the future we'll likely treat long bytestrings
differently and bypass MsgPack for these cases.  We're also looking into
changes in Tornado to avoid the final memcopy.
