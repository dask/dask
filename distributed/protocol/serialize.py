from __future__ import print_function, division, absolute_import

from functools import partial

from dask.base import normalize_token

from . import pickle


serializers = {}
deserializers = {None: lambda header, frames: pickle.loads(b''.join(frames))}


def register_serialization(cls, serialize, deserialize):
    """ Register a new class for custom serialization

    Parameters
    ----------
    cls: type
    serialize: function
    deserialize: function

    Examples
    --------
    >>> class Human(object):
    ...     def __init__(self, name):
    ...         self.name = name

    >>> def serialize(human):
    ...     header = {}
    ...     frames = [human.name.encode()]
    ...     return header, frames

    >>> def deserialize(header, frames):
    ...     return Human(frames[0].decode())

    >>> register_serialization(Human, serialize, deserialize)
    >>> serialize(Human('Alice'))
    ({}, [b'Alice'])

    See Also
    --------
    serialize
    deserialize
    """
    name = typename(cls)
    serializers[name] = serialize
    deserializers[name] = deserialize


def typename(typ):
    """ Return name of type

    Examples
    --------
    >>> from distributed import Scheduler
    >>> typename(Scheduler)
    'distributed.scheduler.Scheduler'
    """
    return typ.__module__ + '.' + typ.__name__


def serialize(x):
    r"""
    Convert object to a header and list of bytestrings

    This takes in an arbitrary Python object and returns a msgpack serializable
    header and a list of bytes or memoryview objects.  By default this uses
    pickle/cloudpickle but can use special functions if they have been
    pre-registered.

    Examples
    --------
    >>> serialize(1)
    ({}, [b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'])

    >>> serialize(b'123')  # some special types get custom treatment
    ({'type': 'builtins.bytes'}, [b'123'])

    >>> deserialize(*serialize(1))
    1

    Returns
    -------
    header: dictionary containing any msgpack-serializable metadata
    frames: list of bytes or memoryviews, commonly of length one

    See Also
    --------
    deserialize: Convert header and frames back to object
    to_serialize: Mark that data in a message should be serialized
    register_serialization: Register custom serialization functions
    """
    if isinstance(x, Serialized):
        return x.header, x.frames

    name = typename(type(x))
    if name in serializers:
        header, frames = serializers[name](x)
        header['type'] = name
    else:
        header, frames = {}, [pickle.dumps(x)]

    return header, frames


def deserialize(header, frames):
    """
    Convert serialized header and list of bytestrings back to a Python object

    Parameters
    ----------
    header: dict
    frames: list of bytes

    See Also
    --------
    serialize
    """
    f = deserializers[header.get('type')]
    return f(header, frames)


class Serialize(object):
    """ Mark an object that should be serialized

    Example
    -------
    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> msg  # doctest: +SKIP
    {'op': 'update', 'data': <Serialize: 123>}

    See also
    --------
    distributed.protocol.dumps
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return "<Serialize: %s>" % str(self.data)

    __repr__ = __str__


to_serialize = Serialize


class Serialized(object):
    """
    An object that is already serialized into header and frames

    Normal serialization operations pass these objects through.  This is
    typically used within the scheduler which accepts messages that contain
    data without actually unpacking that data.
    """
    def __init__(self, header, frames):
        self.header = header
        self.frames = frames


@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames  # for dask.base.tokenize


# Teach serialize how to handle bytestrings
def serialize_bytes(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


def deserialize_bytes(header, frames):
    return frames[0]


register_serialization(bytes, serialize_bytes, deserialize_bytes)
