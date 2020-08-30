from array import array
from functools import partial
import traceback
import importlib
from enum import Enum

import dask
from dask.base import normalize_token

from tlz import valmap, get_in

import msgpack

from . import pickle
from ..utils import has_keyword, nbytes, typename, ensure_bytes, is_writeable
from .compression import maybe_compress, decompress
from .utils import (
    unpack_frames,
    pack_frames_prelude,
    frame_split_size,
    merge_frames,
    msgpack_opts,
)


lazy_registrations = {}


dask_serialize = dask.utils.Dispatch("dask_serialize")
dask_deserialize = dask.utils.Dispatch("dask_deserialize")


def dask_dumps(x, context=None):
    """Serialise object using the class-based registry"""
    type_name = typename(type(x))
    try:
        dumps = dask_serialize.dispatch(type(x))
    except TypeError:
        raise NotImplementedError(type_name)
    if has_keyword(dumps, "context"):
        header, frames = dumps(x, context=context)
    else:
        header, frames = dumps(x)

    header["type"] = type_name
    header["type-serialized"] = pickle.dumps(type(x), protocol=4)
    header["serializer"] = "dask"
    return header, frames


def dask_loads(header, frames):
    typ = pickle.loads(header["type-serialized"])
    loads = dask_deserialize.dispatch(typ)
    return loads(header, frames)


def pickle_dumps(x, context=None):
    header = {"serializer": "pickle"}
    frames = [None]
    buffer_callback = lambda f: frames.append(memoryview(f))
    frames[0] = pickle.dumps(
        x,
        buffer_callback=buffer_callback,
        protocol=context.get("pickle-protocol", None) if context else None,
    )
    return header, frames


def pickle_loads(header, frames):
    x, buffers = frames[0], frames[1:]
    return pickle.loads(x, buffers=buffers)


def msgpack_decode_default(obj):
    """
    Custom packer/unpacker for msgpack to support Enums
    """
    if "__Enum__" in obj:
        mod = importlib.import_module(obj["__module__"])
        enum_type = getattr(mod, obj["__name__"])
        obj = getattr(enum_type, obj["name"])
    return obj


def msgpack_encode_default(obj):
    """
    Custom packer/unpacker for msgpack to support Enums
    """

    if isinstance(obj, Enum):
        return {
            "__Enum__": True,
            "name": obj.name,
            "__module__": obj.__module__,
            "__name__": type(obj).__name__,
        }
    return obj


def msgpack_dumps(x):
    try:
        frame = msgpack.dumps(x, use_bin_type=True)
    except Exception:
        raise NotImplementedError()
    else:
        return {"serializer": "msgpack"}, [frame]


def msgpack_loads(header, frames):
    return msgpack.loads(b"".join(frames), use_list=False, **msgpack_opts)


def serialization_error_loads(header, frames):
    msg = "\n".join([ensure_bytes(frame).decode("utf8") for frame in frames])
    raise TypeError(msg)


families = {}


def register_serialization_family(name, dumps, loads):
    families[name] = (dumps, loads, dumps and has_keyword(dumps, "context"))


register_serialization_family("dask", dask_dumps, dask_loads)
register_serialization_family("pickle", pickle_dumps, pickle_loads)
register_serialization_family("msgpack", msgpack_dumps, msgpack_loads)
register_serialization_family("error", None, serialization_error_loads)


def check_dask_serializable(x):
    if type(x) in (list, set, tuple) and len(x):
        return check_dask_serializable(next(iter(x)))
    elif type(x) is dict and len(x):
        return check_dask_serializable(next(iter(x.items()))[1])
    else:
        try:
            dask_serialize.dispatch(type(x))
            return True
        except TypeError:
            pass
    return False


def serialize(x, serializers=None, on_error="message", context=None):
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
        serializers = ("dask", "pickle")  # TODO: get from configuration

    if isinstance(x, Serialized):
        return x.header, x.frames

    if type(x) in (list, set, tuple, dict):
        iterate_collection = False
        if type(x) is list and "msgpack" in serializers:
            # Note: "msgpack" will always convert lists to tuples
            #       (see GitHub #3716), so we should iterate
            #       through the list if "msgpack" comes before "pickle"
            #       in the list of serializers.
            iterate_collection = ("pickle" not in serializers) or (
                serializers.index("pickle") > serializers.index("msgpack")
            )
        if not iterate_collection:
            # Check for "dask"-serializable data in dict/list/set
            iterate_collection = check_dask_serializable(x)

    # Determine whether keys are safe to be serialized with msgpack
    if type(x) is dict and iterate_collection:
        try:
            msgpack.dumps(list(x.keys()))
        except Exception:
            dict_safe = False
        else:
            dict_safe = True

    if (
        type(x) in (list, set, tuple)
        and iterate_collection
        or type(x) is dict
        and iterate_collection
        and dict_safe
    ):
        if isinstance(x, dict):
            headers_frames = []
            for k, v in x.items():
                _header, _frames = serialize(
                    v, serializers=serializers, on_error=on_error, context=context
                )
                _header["key"] = k
                headers_frames.append((_header, _frames))
        else:
            headers_frames = [
                serialize(
                    obj, serializers=serializers, on_error=on_error, context=context
                )
                for obj in x
            ]

        frames = []
        lengths = []
        compressions = []
        for _header, _frames in headers_frames:
            frames.extend(_frames)
            length = len(_frames)
            lengths.append(length)
            compressions.extend(_header.get("compression") or [None] * len(_frames))

        headers = [obj[0] for obj in headers_frames]
        headers = {
            "sub-headers": headers,
            "is-collection": True,
            "frame-lengths": lengths,
            "type-serialized": type(x).__name__,
        }
        if any(compression is not None for compression in compressions):
            headers["compression"] = compressions
        return headers, frames

    tb = ""

    for name in serializers:
        dumps, loads, wants_context = families[name]
        try:
            header, frames = dumps(x, context=context) if wants_context else dumps(x)
            header["serializer"] = name
            return header, frames
        except NotImplementedError:
            continue
        except Exception as e:
            tb = traceback.format_exc()
            break

    msg = "Could not serialize object of type %s." % type(x).__name__
    if on_error == "message":
        frames = [msg]
        if tb:
            frames.append(tb[:100000])

        frames = [frame.encode() for frame in frames]

        return {"serializer": "error"}, frames
    elif on_error == "raise":
        raise TypeError(msg, str(x)[:10000])


def deserialize(header, frames, deserializers=None):
    """
    Convert serialized header and list of bytestrings back to a Python object

    Parameters
    ----------
    header: dict
    frames: list of bytes
    deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
        An optional dict mapping a name to a (de)serializer.
        See `dask_serialize` and `dask_deserialize` for more.

    See Also
    --------
    serialize
    """
    if "is-collection" in header:
        headers = header["sub-headers"]
        lengths = header["frame-lengths"]
        cls = {"tuple": tuple, "list": list, "set": set, "dict": dict}[
            header["type-serialized"]
        ]

        start = 0
        if cls is dict:
            d = {}
            for _header, _length in zip(headers, lengths):
                k = _header.pop("key")
                d[k] = deserialize(
                    _header,
                    frames[start : start + _length],
                    deserializers=deserializers,
                )
                start += _length
            return d
        else:
            lst = []
            for _header, _length in zip(headers, lengths):
                lst.append(
                    deserialize(
                        _header,
                        frames[start : start + _length],
                        deserializers=deserializers,
                    )
                )
                start += _length
            return cls(lst)

    name = header.get("serializer")
    if deserializers is not None and name not in deserializers:
        raise TypeError(
            "Data serialized with %s but only able to deserialize "
            "data with %s" % (name, str(list(deserializers)))
        )
    dumps, loads, wants_context = families[name]
    return loads(header, frames)


class Serialize:
    """Mark an object that should be serialized

    Examples
    --------
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
        return isinstance(other, Serialize) and other.data == self.data

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.data)


to_serialize = Serialize


class Serialized:
    """
    An object that is already serialized into header and frames

    Normal serialization operations pass these objects through.  This is
    typically used within the scheduler which accepts messages that contain
    data without actually unpacking that data.
    """

    def __init__(self, header, frames):
        self.header = header
        self.frames = frames

    def __eq__(self, other):
        return (
            isinstance(other, Serialized)
            and other.header == self.header
            and other.frames == self.frames
        )

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
    """Pull out Serialize objects from message

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
            elif (
                typ is Serialize
                or typ is Serialized
                or typ in (bytes, bytearray)
                and len(v) > 2 ** 16
            ):
                ser[path + (k,)] = v
    elif type(x) is list:
        for k, v in enumerate(x):
            typ = type(v)
            if typ is list or typ is dict:
                _extract_serialize(v, ser, path + (k,))
            elif (
                typ is Serialize
                or typ is Serialized
                or typ in (bytes, bytearray)
                and len(v) > 2 ** 16
            ):
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
    if "writeable" not in header:
        header["writeable"] = tuple(map(is_writeable, frames))
    if "lengths" not in header:
        header["lengths"] = tuple(map(nbytes, frames))
    if frames:
        frames = sum(map(frame_split_size, frames), [])
        compression, frames = zip(*map(maybe_compress, frames))
    else:
        compression = []
    header["compression"] = compression
    header["count"] = len(frames)

    header = msgpack.dumps(header, use_bin_type=True)
    frames2 = [header, *frames]
    frames2.insert(0, pack_frames_prelude(frames2))
    return frames2


def serialize_bytes(x, **kwargs):
    L = serialize_bytelist(x, **kwargs)
    return b"".join(L)


def deserialize_bytes(b):
    frames = unpack_frames(b)
    header, frames = frames[0], frames[1:]
    if header:
        header = msgpack.loads(header, raw=False, use_list=False)
    else:
        header = {}
    frames = decompress(header, frames)
    frames = merge_frames(header, frames)
    return deserialize(header, frames)


################################
# Class specific serialization #
################################


def register_serialization(cls, serialize, deserialize):
    """Register a new class for dask-custom serialization

    Parameters
    ----------
    cls: type
    serialize: callable(cls) -> Tuple[Dict, List[bytes]]
    deserialize: callable(header: Dict, frames: List[bytes]) -> cls

    Examples
    --------
    >>> class Human:
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
    if isinstance(cls, str):
        raise TypeError(
            "Strings are no longer accepted for type registration. "
            "Use dask_serialize.register_lazy instead"
        )
    dask_serialize.register(cls)(serialize)
    dask_deserialize.register(cls)(deserialize)


def register_serialization_lazy(toplevel, func):
    """Register a registration function to be called if *toplevel*
    module is ever loaded.
    """
    raise Exception("Serialization registration has changed. See documentation")


@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames  # for dask.base.tokenize


# Teach serialize how to handle bytes
@dask_serialize.register(bytes)
def _serialize_bytes(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


# Teach serialize how to handle bytestrings
@dask_serialize.register(bytearray)
def _serialize_bytearray(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


@dask_deserialize.register(bytes)
def _deserialize_bytes(header, frames):
    if len(frames) == 1 and isinstance(frames[0], bytes):
        return frames[0]
    else:
        return bytes().join(frames)


@dask_deserialize.register(bytearray)
def _deserialize_bytearray(header, frames):
    if len(frames) == 1 and isinstance(frames[0], bytearray):
        return frames[0]
    else:
        return bytearray().join(frames)


@dask_serialize.register(array)
def _serialize_array(obj):
    header = {"typecode": obj.typecode, "writeable": (None,)}
    frames = [memoryview(obj)]
    return header, frames


@dask_deserialize.register(array)
def _deserialize_array(header, frames):
    a = array(header["typecode"])
    for f in map(memoryview, frames):
        try:
            f = f.cast("B")
        except TypeError:
            f = f.tobytes()
        a.frombytes(f)
    return a


@dask_serialize.register(memoryview)
def _serialize_memoryview(obj):
    if obj.format == "O":
        raise ValueError("Cannot serialize `memoryview` containing Python objects")
    header = {"format": obj.format, "shape": obj.shape}
    frames = [obj]
    return header, frames


@dask_deserialize.register(memoryview)
def _deserialize_memoryview(header, frames):
    if len(frames) == 1:
        out = memoryview(frames[0]).cast("B")
    else:
        out = memoryview(b"".join(frames))
    out = out.cast(header["format"], header["shape"])
    return out


#########################
# Descend into __dict__ #
#########################


def _is_msgpack_serializable(v):
    typ = type(v)
    return (
        v is None
        or typ is str
        or typ is bool
        or typ is int
        or typ is float
        or isinstance(v, dict)
        and all(map(_is_msgpack_serializable, v.values()))
        and all(typ is str for x in v.keys())
        or isinstance(v, (list, tuple))
        and all(map(_is_msgpack_serializable, v))
    )


class ObjectDictSerializer:
    def __init__(self, serializer):
        self.serializer = serializer

    def serialize(self, est):
        header = {
            "serializer": self.serializer,
            "type-serialized": pickle.dumps(type(est), protocol=4),
            "simple": {},
            "complex": {},
        }
        frames = []

        if isinstance(est, dict):
            d = est
        else:
            d = est.__dict__

        for k, v in d.items():
            if _is_msgpack_serializable(v):
                header["simple"][k] = v
            else:
                if isinstance(v, dict):
                    h, f = self.serialize(v)
                else:
                    h, f = serialize(v, serializers=(self.serializer, "pickle"))
                header["complex"][k] = {
                    "header": h,
                    "start": len(frames),
                    "stop": len(frames) + len(f),
                }
                frames += f
        return header, frames

    def deserialize(self, header, frames):
        cls = pickle.loads(header["type-serialized"])
        if issubclass(cls, dict):
            dd = obj = {}
        else:
            obj = object.__new__(cls)
            dd = obj.__dict__
        dd.update(header["simple"])
        for k, d in header["complex"].items():
            h = d["header"]
            f = frames[d["start"] : d["stop"]]
            v = deserialize(h, f)
            dd[k] = v

        return obj


dask_object_with_dict_serializer = ObjectDictSerializer("dask")

dask_deserialize.register(dict)(dask_object_with_dict_serializer.deserialize)


def register_generic(
    cls,
    serializer_name="dask",
    serialize_func=dask_serialize,
    deserialize_func=dask_deserialize,
):
    """Register (de)serialize to traverse through __dict__

    Normally when registering new classes for Dask's custom serialization you
    need to manage headers and frames, which can be tedious.  If all you want
    to do is traverse through your object and apply serialize to all of your
    object's attributes then this function may provide an easier path.

    This registers a class for the custom Dask serialization family.  It
    serializes it by traversing through its __dict__ of attributes and applying
    ``serialize`` and ``deserialize`` recursively.  It collects a set of frames
    and keeps small attributes in the header.  Deserialization reverses this
    process.

    This is a good idea if the following hold:

    1.  Most of the bytes of your object are composed of data types that Dask's
        custom serializtion already handles well, like Numpy arrays.
    2.  Your object doesn't require any special constructor logic, other than
        object.__new__(cls)

    Examples
    --------
    >>> import sklearn.base
    >>> from distributed.protocol import register_generic
    >>> register_generic(sklearn.base.BaseEstimator)

    See Also
    --------
    dask_serialize
    dask_deserialize
    """
    object_with_dict_serializer = ObjectDictSerializer(serializer_name)
    serialize_func.register(cls)(object_with_dict_serializer.serialize)
    deserialize_func.register(cls)(object_with_dict_serializer.deserialize)
