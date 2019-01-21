.. _serialization:

Serialization
=============

.. currentmodule:: distributed.protocol.serialize

When we communicate data between computers we first convert that data into a
sequence of bytes that can be communicated across the network.  Choices made in
serialization can affect performance and security.

The standard Python solution to this, Pickle, is often but not always the right
solution.  Dask uses a number of different serialization schemes in different
situations.  These are extensible to allow users to control in sensitive
situations and also to enable library developers to plug in more performant
serialization solutions.

This document first describes Dask's default solution for serialization and
then discusses ways to control and extend that serialiation.

Defaults
--------

There are three kinds of messages passed through the Dask network:

1.  Small administrative messages like "Worker A has finished task X" or "I'm
    running out of memory".  These are always serialized with msgpack.
2.  Movement of program data, such as Numpy arrays and Pandas dataframes.  This
    uses a combination of pickle and custom serializers and is the topic of the
    next section
3.  Computational tasks like ``f(x)`` that are defined and serialized on client
    processes and deserialized and run on worker processes.  These are
    serialized using a fixed scheme decided on by those libraries.  Today this
    is a combination of pickle and cloudpickle.

Serialization families
----------------------

Use
+++

For the movement of program data (item 2 above) we can use a few different
families of serializers.  By default the following families are built in:

1.  Pickle and cloudpickle
2.  Msgpack
3.  Custom per-type serializers that come with Dask for the special
    serialization of important classes of data like Numpy arrays

You can choose which families you want to use to serialize data and to
deserialize data when you create a Client

.. code-block:: python

   from dask.distributed import Client
   client = Client('tcp://scheduler-address:8786',
                   serializers=['dask', 'pickle'],
                   deserializers=['dask', 'msgpack'])

This can be useful if, for example, you are sensitive about receiving
Pickle-serialized data for security reasons.

Dask uses the serializers ``['dask', 'pickle']`` by default, trying to use dask
custom serializers (described below) if they work and then falling back to
pickle/cloudpickle.


Extend
++++++

These families can be extended by creating two functions, dumps and loads,
which return and consume a msgpack-encodable header, and a list of byte-like
objects.  These must then be included in the ``distributed.protocol.serialize``
dictionary with an appropriate name.  Here is the definition of
``pickle_dumps`` and ``pickle_loads`` to serve as an example.

.. code-block:: python

   import pickle

   def pickle_dumps(x):
       header = {'serializer': 'pickle'}
       frames = [pickle.dumps(x)]
       return header, frames

   def pickle_loads(header, frames):
       if len(frames) > 1:  # this may be cut up for network reasons
           frame = ''.join(frames)
       else:
           frame = frames[0]
       return pickle.loads(frame)

   from distributed.protocol.serialize import register_serialization_family
   register_serialization_family('pickle', pickle_dumps, pickle_loads)

After this the name ``'pickle'`` can be used in the ``serializers=`` and
``deserializers=`` keywords in ``Client`` and other parts of Dask.


Communication Context
+++++++++++++++++++++

.. note:: This is an experimental feature and may change without notice

Dask :doc:`Comms <communications>` can provide additional context to
serialization family functions if they provide a ``context=`` keyword.
This allows serialization to behave differently according to how it is being
used.

.. code-block:: python

   def my_dumps(x, context=None):
       if context and 'recipient' in context:
           # check if we're sending to the same host or not

The context depends on the kind of communication.  For example when sending
over TCP, the address of the sender (us) and the recipient are available in a
dictionary.

.. code-block:: python

   >>> context
   {'sender': 'tcp://127.0.0.1:1234', 'recipient': 'tcp://127.0.0.1:5678'}

Other comms may provide other information.


Dask Serialization Family
-------------------------

Use
+++

Dask maintains its own custom serialization family that special cases a few
important types, like Numpy arrays.  These serializers either operate more
efficiently than Pickle, or serialize types that Pickle can not handle.

You don't need to do anything special to use this family of serializers.  It is
on by default (along with pickle).  Note that Dask custom serializers may use
pickle internally in some cases.  It should not be considered more secure.

Extend
++++++

.. autosummary::
   dask_serialize
   dask_deserialize

As with serialization families in general, the Dask family in particular is
*also* extensible.  This is a good way to support custom serialization of a
single type of object.  The method is similar, you create serialize and
deserialize function that create and consume a header and frames, and then
register them with Dask.

.. code-block:: python

    class Human(object):
        def __init__(self, name):
            self.name = name

    from distributed.protocol import dask_serialize, dask_deserialize

    @dask_serialize.register(Human)
    def serialize(human: Human) -> Tuple[Dict, List[bytes]]:
        header = {}
        frames = [human.name.encode()]
        return header, frames

    @dask_deserialize.register(Human)
    def deserialize(header: Dict, frames: List[bytes]) -> Human:
        return Human(frames[0].decode())


Traverse attributes
+++++++++++++++++++

.. autosummary::
   register_generic

A common case is that your object just wraps Numpy arrays or other objects that
Dask already serializes well.  For example, Scikit-Learn estimators mostly
surround Numpy arrays with a bit of extra metadata.  In these cases you can
register your class for custom Dask serialization with the
``register_generic``
function.

API
---

.. autosummary:: serialize
                 deserialize
                 dask_serialize
                 dask_deserialize
                 register_generic

.. autofunction:: serialize
.. autofunction:: deserialize
.. autofunction:: dask_serialize
.. autofunction:: dask_deserialize
.. autofunction:: register_generic
