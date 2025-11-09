from __future__ import annotations

import codecs
import importlib
import traceback
import warnings
from array import array
from enum import Enum
from functools import partial
from pickle import PickleBuffer
from types import ModuleType
from typing import Any, Generic, Literal, TypeVar

import msgpack

import dask
from dask.sizeof import sizeof
from dask.tokenize import normalize_token
from dask.utils import typename

from distributed.metrics import context_meter
from distributed.protocol import pickle
from distributed.protocol.compression import decompress, maybe_compress
from distributed.protocol.utils import (
    frame_split_size,
    host_array_from_buffers,
    merge_memoryviews,
    msgpack_opts,
    pack_frames_prelude,
    unpack_frames,
)
from distributed.utils import ensure_memoryview, has_keyword

T = TypeVar("T")

dask_serialize = dask.utils.Dispatch("dask_serialize")
dask_deserialize = dask.utils.Dispatch("dask_deserialize")

_cached_allowed_modules: dict[str, ModuleType] = {}


def dask_dumps(x, context=None):
    """Serialize object using the class-based registry"""
    type_name = typename(type(x))
    try:
        dumps = dask_serialize.dispatch(type(x))
    except TypeError:
        raise NotImplementedError(type_name)
    if has_keyword(dumps, "context"):
        sub_header, frames = dumps(x, context=context)
    else:
        sub_header, frames = dumps(x)

    header = {
        "sub-header": sub_header,
        "type": type_name,
        "type-serialized": pickle.dumps(type(x)),
        "serializer": "dask",
    }
    return header, frames


def dask_loads(header, frames):
    typ = pickle.loads(header["type-serialized"])
    loads = dask_deserialize.dispatch(typ)
    return loads(header["sub-header"], frames)


def pickle_dumps(x, context=None):
    frames = [None]
    writeable = []

    def buffer_callback(f):
        f = memoryview(f)
        frames.append(f)
        writeable.append(not f.readonly)

    frames[0] = pickle.dumps(
        x,
        buffer_callback=buffer_callback,
        protocol=context.get("pickle-protocol", None) if context else None,
    )
    header = {
        "serializer": "pickle",
        "writeable": tuple(writeable),
    }

    return header, frames


def pickle_loads(
    header: dict[str, Any], frames: list[bytes | bytearray | memoryview | PickleBuffer]
) -> Any:
    pik, buffers = frames[0], frames[1:]

    def ensure_writeable_flag(mv: memoryview, w: bool) -> memoryview:
        if w and mv.readonly:
            # Can't avoid a deep copy
            return memoryview(bytearray(mv))
        elif not w and not mv.readonly:
            # Zero copy - this is just a flag
            return mv.toreadonly()
        else:
            return mv

    buffers = [
        ensure_writeable_flag(ensure_memoryview(mv), w)
        for mv, w in zip(buffers, header["writeable"])
    ]

    return pickle.loads(pik, buffers=buffers)


def import_allowed_module(name):
    if name in _cached_allowed_modules:
        return _cached_allowed_modules[name]

    # Check for non-ASCII characters
    name = name.encode("ascii").decode()
    # We only compare the root module
    root = name.split(".", 1)[0]

    # Note, if an empty string creeps into allowed-imports it is disallowed explicitly
    if root and root in dask.config.get("distributed.scheduler.allowed-imports"):
        _cached_allowed_modules[name] = importlib.import_module(name)
        return _cached_allowed_modules[name]
    else:
        raise RuntimeError(
            f"Importing {repr(name)} is not allowed, please add it to the list of "
            "allowed modules the scheduler can import via the "
            "distributed.scheduler.allowed-imports configuration setting."
        )


def msgpack_decode_default(obj):
    """
    Custom packer/unpacker for msgpack
    """
    if "__Enum__" in obj:
        mod = import_allowed_module(obj["__module__"])
        typ = getattr(mod, obj["__name__"])
        return getattr(typ, obj["name"])

    if "__Set__" in obj:
        return set(obj["as-list"])

    if "__Serialized__" in obj:
        # Notice, the data here is marked a Serialized rather than deserialized. This
        # is because deserialization requires Pickle which the Scheduler cannot run
        # because of security reasons.
        # By marking it Serialized, the data is passed through to the workers that
        # eventually will deserialize it.
        return Serialized(*obj["data"])

    return obj


def msgpack_encode_default(obj):
    """
    Custom packer/unpacker for msgpack
    """

    if isinstance(obj, Serialize):
        return {"__Serialized__": True, "data": serialize(obj.data)}

    if isinstance(obj, Enum):
        return {
            "__Enum__": True,
            "name": obj.name,
            "__module__": obj.__module__,
            "__name__": type(obj).__name__,
        }

    if isinstance(obj, set):
        return {"__Set__": True, "as-list": list(obj)}

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
    msg = "\n".join([codecs.decode(frame, "utf8") for frame in frames])
    raise TypeError(msg)


families = {}


def register_serialization_family(name, dumps, loads):
    families[name] = (dumps, loads, dumps and has_keyword(dumps, "context"))


register_serialization_family("dask", dask_dumps, dask_loads)
register_serialization_family("pickle", pickle_dumps, pickle_loads)
register_serialization_family("msgpack", msgpack_dumps, msgpack_loads)
register_serialization_family("error", None, serialization_error_loads)


def check_dask_serializable(x):
    try:
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
    except RecursionError:
        return False


def serialize(  # type: ignore[no-untyped-def]
    x: object,
    serializers=None,
    on_error: Literal["message" | "raise"] = "message",
    context=None,
    iterate_collection: bool | None = None,
) -> tuple[dict[str, Any], list[bytes | memoryview]]:
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

    Notes on the ``iterate_collection`` argument (only relevant when
    ``x`` is a collection):
    - ``iterate_collection=True``: Serialize collection elements separately.
    - ``iterate_collection=False``: Serialize collection elements together.
    - ``iterate_collection=None`` (default): Infer the best setting.

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
    deserialize : Convert header and frames back to object
    to_serialize : Mark that data in a message should be serialized
    register_serialization : Register custom serialization functions
    """
    if serializers is None:
        serializers = ("dask", "pickle")  # TODO: get from configuration

    # Handle objects that are marked as `Serialize`, or that are
    # already `Serialized` objects (don't want to serialize them twice)
    if isinstance(x, Serialized):
        return x.header, x.frames
    if isinstance(x, Serialize):
        return serialize(
            x.data,
            serializers=serializers,
            on_error=on_error,
            context=context,
            iterate_collection=True,
        )

    # Note: don't use isinstance(), as it would match subclasses
    # (e.g. namedtuple, defaultdict) which however would revert to the base class on a
    # round-trip through msgpack
    if iterate_collection is None and type(x) in (list, set, tuple, dict):
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
            assert isinstance(x, (list, set, tuple))
            headers_frames = [
                serialize(
                    obj, serializers=serializers, on_error=on_error, context=context
                )
                for obj in x
            ]

        frames = []
        lengths = []
        compressions: list[str | None] = []
        for _header, _frames in headers_frames:
            frames.extend(_frames)
            length = len(_frames)
            lengths.append(length)
            compressions.extend(_header.get("compression") or [None] * len(_frames))

        headers = {
            "sub-headers": [obj[0] for obj in headers_frames],
            "is-collection": True,
            "frame-lengths": lengths,
            "type-serialized": type(x).__name__,
        }
        if any(compression is not None for compression in compressions):
            headers["compression"] = compressions
        return headers, frames

    tb = ""
    exc = None

    for name in serializers:
        dumps, _, wants_context = families[name]
        try:
            header, frames = dumps(x, context=context) if wants_context else dumps(x)
            header["serializer"] = name
            return header, frames
        except NotImplementedError:
            continue
        except Exception as e:
            exc = e
            tb = traceback.format_exc()
            break
    type_x = type(x)
    if isinstance(x, (ToPickle, Serialize)):
        type_x = type(x.data)
    msg = f"Could not serialize object of type {type_x.__name__}"
    if on_error == "message":
        txt_frames = [msg]
        if tb:
            txt_frames.append(tb[:100000])

        frames = [frame.encode() for frame in txt_frames]

        return {"serializer": "error"}, frames
    elif on_error == "raise":
        try:
            str_x = str(x)[:10000]
        except Exception:
            raise TypeError(msg) from exc
        raise TypeError(msg, str_x) from exc
    else:  # pragma: nocover
        raise ValueError(f"{on_error=}; expected 'message' or 'raise'")


def deserialize(header, frames, deserializers=None):
    """
    Convert serialized header and list of bytestrings back to a Python object

    Parameters
    ----------
    header : dict
    frames : list of bytes
    deserializers : dict[str, tuple[Callable, Callable, bool]] | None
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


@context_meter.meter("serialize")
def serialize_and_split(
    x, serializers=None, on_error="message", context=None, size=None
):
    """Serialize and split compressible frames

    This function is a drop-in replacement of `serialize()` that calls `serialize()`
    followed by `frame_split_size()` on frames that should be compressed.

    Use `merge_and_deserialize()` to merge and deserialize the frames back.

    See Also
    --------
    serialize
    merge_and_deserialize
    """
    header, frames = serialize(x, serializers, on_error, context)
    num_sub_frames = []
    offsets = []
    out_frames = []
    out_compression = []
    for frame, compression in zip(
        frames, header.get("compression") or [None] * len(frames)
    ):
        if compression is None:  # default behavior
            sub_frames = frame_split_size(frame, n=size)
            num_sub_frames.append(len(sub_frames))
            offsets.append(len(out_frames))
            out_frames.extend(sub_frames)
            out_compression.extend([None] * len(sub_frames))
        else:
            num_sub_frames.append(1)
            offsets.append(len(out_frames))
            out_frames.append(frame)
            out_compression.append(compression)
    assert len(out_compression) == len(out_frames)

    # Notice, in order to match msgpack's implicit conversion to tuples,
    # we convert to tuples here as well.
    header["split-num-sub-frames"] = tuple(num_sub_frames)
    header["split-offsets"] = tuple(offsets)
    header["compression"] = tuple(out_compression)
    return header, out_frames


@context_meter.meter("deserialize")
def merge_and_deserialize(header, frames, deserializers=None):
    """Merge and deserialize frames

    This function is a drop-in replacement of `deserialize()` that merges
    frames that were split by `serialize_and_split()`

    See Also
    --------
    deserialize
    serialize_and_split
    """
    if "split-num-sub-frames" not in header:
        merged_frames = frames
    else:
        merged_frames = []
        for n, offset in zip(header["split-num-sub-frames"], header["split-offsets"]):
            subframes = frames[offset : offset + n]
            try:
                merged = merge_memoryviews(subframes)
            except (ValueError, TypeError):
                merged = host_array_from_buffers(subframes)

            merged_frames.append(merged)

    return deserialize(header, merged_frames, deserializers=deserializers)


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
        return f"<Serialize: {self.data}>"

    def __eq__(self, other):
        return isinstance(other, Serialize) and other.data == self.data

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.data)


to_serialize = Serialize


class Serialized:
    """An object that is already serialized into header and frames

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


class ToPickle(Generic[T]):
    """Mark an object that should be pickled

    Both the scheduler and workers with automatically unpickle this
    object on arrival.
    """

    data: T

    def __init__(self, data: T):
        self.data = data

    def __repr__(self) -> str:
        return "<ToPickle: %s>" % str(self.data)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and other.data == self.data

    def __hash__(self) -> int:
        return hash(self.data)


class Pickled:
    """An object that is already pickled into header and frames

    Normal pickled objects are unpickled by the scheduler.
    """

    def __init__(self, header, frames):
        self.header = header
        self.frames = frames

    def __eq__(self, other):
        return (
            isinstance(other, type(self))
            and other.header == self.header
            and other.frames == self.frames
        )

    def __ne__(self, other):
        return not (self == other)


def nested_deserialize(x):
    warnings.warn(
        "nested_deserialize is deprecated and will be removed in a future release.",
        DeprecationWarning,
    )
    return _nested_deserialize(x, emulate_deserialize=True)


def _nested_deserialize(x, emulate_deserialize=True):
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
                if emulate_deserialize:
                    if typ is Serialize:
                        x[k] = v.data
                    elif typ is Serialized:
                        x[k] = deserialize(v.header, v.frames)
                if typ is ToPickle:
                    x[k] = v.data

        elif type(x) is list:
            x = list(x)
            for k, v in enumerate(x):
                typ = type(v)
                if typ is dict or typ is list:
                    x[k] = replace_inner(v)
                if emulate_deserialize:
                    if typ is Serialize:
                        x[k] = v.data
                    elif typ is Serialized:
                        x[k] = deserialize(v.header, v.frames)
                if typ is ToPickle:
                    x[k] = v.data

        return x

    return replace_inner(x)


@sizeof.register(ToPickle)
@sizeof.register(Serialize)
def sizeof_serialize(obj):
    return sizeof(obj.data)


@sizeof.register(Pickled)
@sizeof.register(Serialized)
def sizeof_serialized(obj):
    return sizeof(obj.header) + sizeof(obj.frames)


def serialize_bytelist(
    x: object, compression: str | None | Literal[False] = "auto", **kwargs: Any
) -> list[bytes | bytearray | memoryview]:
    header, frames = serialize_and_split(x, **kwargs)
    if frames:
        header["compression"], frames = zip(
            *(maybe_compress(frame, compression=compression) for frame in frames)
        )
    header["count"] = len(frames)

    header = msgpack.dumps(header, use_bin_type=True)
    frames2 = [header, *frames]
    frames2.insert(0, pack_frames_prelude(frames2))
    return frames2


def serialize_bytes(x: object, **kwargs: Any) -> bytes:
    L = serialize_bytelist(x, **kwargs)
    return b"".join(L)


def deserialize_bytes(b: bytes | bytearray | memoryview) -> Any:
    """Deserialize the output of :func:`serialize_bytes`"""
    frames = unpack_frames(b)
    bin_header, frames = frames[0], frames[1:]
    if bin_header:
        header = msgpack.loads(bin_header, raw=False, use_list=False)
    else:
        header = {}
    frames2 = decompress(header, frames)
    return merge_and_deserialize(header, frames2)


################################
# Class specific serialization #
################################


def register_serialization(cls, serialize, deserialize):
    """Register a new class for dask-custom serialization

    Parameters
    ----------
    cls : type
    serialize : callable(cls) -> Tuple[Dict, List[bytes]]
    deserialize : callable(header: Dict, frames: List[bytes]) -> cls

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
    return [o.header] + o.frames  # for dask.tokenize.tokenize


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
        return b"".join(frames)


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
    nframes = len(frames)
    if nframes == 1:
        a.frombytes(ensure_memoryview(frames[0]))
    elif nframes > 1:
        a.frombytes(b"".join(map(ensure_memoryview, frames)))
    return a


@dask_serialize.register(memoryview)
def _serialize_memoryview(obj):
    if obj.format == "O":
        raise ValueError("Cannot serialize `memoryview` containing Python objects")
    if not obj and obj.ndim > 1:
        raise ValueError("Cannot serialize empty non-1-D `memoryview`")
    header = {"format": obj.format, "shape": obj.shape}
    frames = [obj]
    return header, frames


@dask_deserialize.register(memoryview)
def _deserialize_memoryview(header, frames):
    if len(frames) == 1:
        out = ensure_memoryview(frames[0])
    else:
        out = memoryview(b"".join(frames))

    # handle empty `memoryview`s
    if out:
        out = out.cast(header["format"], header["shape"])
    else:
        out = out.cast(header["format"])
        assert out.shape == header["shape"]

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
        or typ is bytes
        or typ is int
        or typ is float
        or isinstance(v, dict)
        and all(map(_is_msgpack_serializable, v.values()))
        and all(type(x) is str for x in v.keys())
        or isinstance(v, (list, tuple))
        and all(map(_is_msgpack_serializable, v))
    )


def _is_dumpable(v):
    typ = type(v)
    return (
        v is None
        or typ is str
        or typ is bool
        or typ is bytes
        or typ is int
        or typ is float
        or typ is Pickled
        or typ is Serialize
        or typ is Serialized
        or typ is ToPickle
        or isinstance(v, dict)
        and all(map(_is_dumpable, v.values()))
        and all(type(x) is str for x in v.keys())
        or isinstance(v, (list, tuple))
        and all(map(_is_dumpable, v))
    )


class ObjectDictSerializer:
    def __init__(self, serializer):
        self.serializer = serializer

    def serialize(self, est):
        header = {
            "serializer": self.serializer,
            "type-serialized": pickle.dumps(type(est)),
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
                    h = {"nested-dict": h}
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
            nested_dict = h.get("nested-dict")
            if nested_dict:
                v = self.deserialize(nested_dict, f)
            else:
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
