from __future__ import print_function, division, absolute_import
from functools import partial
import traceback

from dask.base import normalize_token
try:
    from cytoolz import valmap, get_in
except ImportError:
    from toolz import valmap, get_in

import msgpack

from . import pickle
from ..compatibility import PY2
from ..utils import has_keyword
from .compression import maybe_compress, decompress
from .utils import unpack_frames, pack_frames_prelude, frame_split_size


class_serializers = {}

lazy_registrations = {}


def dask_dumps(x, context=None):
    """Serialise object using the class-based registry"""
    typ = typename(type(x))
    if typ in class_serializers:
        dumps, loads, has_context = class_serializers[typ]
        if has_context:
            header, frames = dumps(x, context=context)
        else:
            header, frames = dumps(x)
        header['type'] = typ
        header['serializer'] = 'dask'
        return header, frames
    elif _find_lazy_registration(typ):
        return dask_dumps(x)  # recurse
    else:
        raise NotImplementedError(typ)


def dask_loads(header, frames):
    typ = header['type']

    if typ not in class_serializers:
        _find_lazy_registration(typ)

    try:
        dumps, loads, _ = class_serializers[typ]
    except KeyError:
        raise TypeError("Serialization for type %s not found" % typ)
    else:
        return loads(header, frames)


def pickle_dumps(x):
    return {'serializer': 'pickle'}, [pickle.dumps(x)]


def pickle_loads(header, frames):
    return pickle.loads(b''.join(frames))


def msgpack_dumps(x):
    return {'serializer': 'msgpack'}, [msgpack.dumps(x, use_bin_type=True)]


def msgpack_loads(header, frames):
    return msgpack.loads(b''.join(frames), encoding='utf8', use_list=False)


def serialization_error_loads(header, frames):
    msg = '\n'.join([frame.decode('utf8') for frame in frames])
    raise TypeError(msg)


families = {}


def register_serialization_family(name, dumps, loads):
    families[name] = (dumps, loads, dumps and has_keyword(dumps, 'context'))


register_serialization_family('dask', dask_dumps, dask_loads)
register_serialization_family('pickle', pickle_dumps, pickle_loads)
register_serialization_family('msgpack', msgpack_dumps, msgpack_loads)
register_serialization_family('error', None, serialization_error_loads)


def serialize(x, serializers=None, on_error='message', context=None):
    r"""
    Convert object to a header and list of bytestrings

    This takes in an arbitrary Python object and returns a msgpack serializable
    header and a list of bytes or memoryview objects.

    The serialization protocols to use are configurable: a list of names
    define the set of serializers to use, in order. These names are keys in
    the ``serializer_registry`` dict (e.g., 'pickle', 'msgpack'), which maps
    to the de/serialize functions. The name 'dask' is special, and will use the
    per-class serialization methods. ``None`` gives the default list
    ``['dask', 'pickle']``.

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
    if serializers is None:
        serializers = ('dask', 'pickle')  # TODO: get from configuration

    if isinstance(x, Serialized):
        return x.header, x.frames

    tb = ''

    for name in serializers:
        dumps, loads, wants_context = families[name]
        try:
            header, frames = dumps(x, context=context) if wants_context else dumps(x)
            header['serializer'] = name
            return header, frames
        except NotImplementedError:
            continue
        except Exception:
            tb = traceback.format_exc()
            continue

    msg = "Could not serialize object of type %s" % type(x).__name__
    if on_error == 'message':
        frames = [msg]
        if tb:
            frames.append(tb[:100000])

        frames = [frame.encode() for frame in frames]

        return {'serializer': 'error'}, frames
    elif on_error == 'raise':
        raise TypeError(msg)


def deserialize(header, frames, deserializers=None):
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
    name = header.get('serializer')
    if deserializers is not None and name not in deserializers:
        raise TypeError("Data serialized with %s but only able to deserialize "
                        "data with %s" % (name, str(list(deserializers))))
    dumps, loads, wants_context = families[name]
    return loads(header, frames)


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

    def __repr__(self):
        return "<Serialize: %s>" % str(self.data)

    def __eq__(self, other):
        return (isinstance(other, Serialize) and
                other.data == self.data)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.data)


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

    def deserialize(self):
        from .core import decompress
        frames = decompress(self.header, self.frames)
        return deserialize(self.header, frames)

    def __eq__(self, other):
        return (isinstance(other, Serialized) and
                other.header == self.header and
                other.frames == self.frames)

    def __ne__(self, other):
        return not (self == other)


def container_copy(c):
    typ = type(c)
    if typ is list:
        return list(map(container_copy, c))
    if typ is dict:
        return valmap(container_copy, c)
    return c


def extract_serialize(x):
    """ Pull out Serialize objects from message

    This also remove large bytestrings from the message into a second
    dictionary.

    Examples
    --------
    >>> from distributed.protocol import to_serialize
    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> extract_serialize(msg)
    ({'op': 'update'}, {('data',): <Serialize: 123>}, set())
    """
    ser = {}
    _extract_serialize(x, ser)
    if ser:
        x = container_copy(x)
        for path in ser:
            t = get_in(path[:-1], x)
            if isinstance(t, dict):
                del t[path[-1]]
            else:
                t[path[-1]] = None

    bytestrings = set()
    for k, v in ser.items():
        if type(v) in (bytes, bytearray):
            ser[k] = to_serialize(v)
            bytestrings.add(k)
    return x, ser, bytestrings


def _extract_serialize(x, ser, path=()):
    if type(x) is dict:
        for k, v in x.items():
            typ = type(v)
            if typ is list or typ is dict:
                _extract_serialize(v, ser, path + (k,))
            elif (typ is Serialize or typ is Serialized
                  or typ in (bytes, bytearray) and len(v) > 2**16):
                ser[path + (k,)] = v
    elif type(x) is list:
        for k, v in enumerate(x):
            typ = type(v)
            if typ is list or typ is dict:
                _extract_serialize(v, ser, path + (k,))
            elif (typ is Serialize or typ is Serialized
                  or typ in (bytes, bytearray) and len(v) > 2**16):
                ser[path + (k,)] = v


def nested_deserialize(x):
    """
    Replace all Serialize and Serialized values nested in *x*
    with the original values.  Returns a copy of *x*.

    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> nested_deserialize(msg)
    {'op': 'update', 'data': 123}
    """
    def replace_inner(x):
        if type(x) is dict:
            x = x.copy()
            for k, v in x.items():
                typ = type(v)
                if typ is dict or typ is list:
                    x[k] = replace_inner(v)
                elif typ is Serialize:
                    x[k] = v.data
                elif typ is Serialized:
                    x[k] = deserialize(v.header, v.frames)

        elif type(x) is list:
            x = list(x)
            for k, v in enumerate(x):
                typ = type(v)
                if typ is dict or typ is list:
                    x[k] = replace_inner(v)
                elif typ is Serialize:
                    x[k] = v.data
                elif typ is Serialized:
                    x[k] = deserialize(v.header, v.frames)

        return x

    return replace_inner(x)


def serialize_bytelist(x, **kwargs):
    header, frames = serialize(x, **kwargs)
    frames = frame_split_size(frames)
    if frames:
        compression, frames = zip(*map(maybe_compress, frames))
    else:
        compression = []
    header['compression'] = compression
    header['count'] = len(frames)

    header = msgpack.dumps(header, use_bin_type=True)
    frames2 = [header] + list(frames)
    return [pack_frames_prelude(frames2)] + frames2


def serialize_bytes(x, **kwargs):
    L = serialize_bytelist(x, **kwargs)
    if PY2:
        L = [bytes(y) for y in L]
    return b''.join(L)


def deserialize_bytes(b):
    frames = unpack_frames(b)
    header, frames = frames[0], frames[1:]
    if header:
        header = msgpack.loads(header, encoding='utf8', use_list=False)
    else:
        header = {}
    frames = decompress(header, frames)
    return deserialize(header, frames)


################################
# Class specific serialization #
################################


def register_serialization(cls, serialize, deserialize):
    """ Register a new class for dask-custom serialization

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
    if isinstance(cls, type):
        name = typename(cls)
    elif isinstance(cls, str):
        name = cls
    class_serializers[name] = (serialize,
                               deserialize,
                               has_keyword(serialize, 'context'))


def register_serialization_lazy(toplevel, func):
    """Register a registration function to be called if *toplevel*
    module is ever loaded.
    """
    lazy_registrations[toplevel] = func


def typename(typ):
    """ Return name of type

    Examples
    --------
    >>> from distributed import Scheduler
    >>> typename(Scheduler)
    'distributed.scheduler.Scheduler'
    """
    return typ.__module__ + '.' + typ.__name__


def _find_lazy_registration(typename):
    toplevel, _, _ = typename.partition('.')
    if toplevel in lazy_registrations:
        lazy_registrations.pop(toplevel)()
        return True
    else:
        return False


@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames  # for dask.base.tokenize


# Teach serialize how to handle bytestrings
def _serialize_bytes(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


def _deserialize_bytes(header, frames):
    return frames[0]


# NOTE: using the same exact serialization means a bytes object may be
# deserialized as bytearray or vice-versa...  Not sure this is a problem
# in practice.
register_serialization(bytes, _serialize_bytes, _deserialize_bytes)
register_serialization(bytearray, _serialize_bytes, _deserialize_bytes)
