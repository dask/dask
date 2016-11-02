Custom Serialization
====================

When we communicate data between computers we first convert that data into a
sequence of bytes that can be communicated across the network.

Dask can convert data to bytes using the standard solutions of Pickle and
Cloudpickle.  However, sometimes pickle and cloudpickle are suboptimal so Dask
also supports custom serialization formats for special types.  This helps Dask
to be faster on common formats like NumPy and Pandas and gives power-users more
control about how their objects get moved around on the network if they want to
extend the system.

We include a small example and then follow with the full API documentation
describing the ``serialize`` and ``deserialize`` functions, which convert
objects into a msgpack header and a list of bytestrings and back.

Example
-------

Here is how we special case handling raw Python bytes objects.  In this case
there is no need to call ``pickle.dumps`` on the object.  The object is
already a sequnce of bytes.

.. code-block:: python

   def serialize_bytes(obj):
       header = {}  # no special metadata
       frames = [obj]
       return header, frames

   def deserialize_bytes(header, frames):
       return frames[0]

   register_serialization(bytes, serialize_bytes, deserialize_bytes)


API
---

.. currentmodule:: distributed.protocol.serialize

.. autosummary:: register_serialization
                 serialize
                 deserialize

.. autofunction:: register_serialization
.. autofunction:: serialize
.. autofunction:: deserialize
