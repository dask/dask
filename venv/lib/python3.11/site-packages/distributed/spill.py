from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterator, Mapping, MutableMapping, Sized
from contextlib import contextmanager
from functools import partial
from typing import Literal, NamedTuple, Protocol, cast

import zict
from dask.typing import Key

from distributed.metrics import context_meter
from distributed.protocol import deserialize_bytes, serialize_bytelist
from distributed.protocol.compression import get_compression_settings
from distributed.sizeof import safe_sizeof
from distributed.utils import RateLimiterFilter, nbytes

logger = logging.getLogger(__name__)
logger.addFilter(RateLimiterFilter("Spill file on disk reached capacity"))
logger.addFilter(RateLimiterFilter("Spill to disk failed"))


class SpilledSize(NamedTuple):
    """Size of a key/value pair when spilled to disk, in bytes"""

    # output of sizeof()
    memory: int
    # pickled size
    disk: int

    def __add__(self, other: SpilledSize) -> SpilledSize:  # type: ignore
        return SpilledSize(self.memory + other.memory, self.disk + other.disk)

    def __sub__(self, other: SpilledSize) -> SpilledSize:
        return SpilledSize(self.memory - other.memory, self.disk - other.disk)


class ManualEvictProto(Protocol):
    """Duck-type API that a third-party alternative to SpillBuffer must respect (in
    addition to MutableMapping) if it wishes to support spilling when the
    ``distributed.worker.memory.spill`` threshold is surpassed.

    This is public API. At the moment of writing, Dask-CUDA implements this protocol in
    the ProxifyHostFile class.
    """

    @property
    def fast(self) -> Sized | bool:
        """Access to fast memory. This is normally a MutableMapping, but for the purpose
        of the manual eviction API it is just tested for emptiness to know if there is
        anything to evict.
        """
        ...  # pragma: nocover

    def evict(self) -> int:
        """Manually evict a key/value pair from fast to slow memory.
        Return size of the evicted value in fast memory.

        If the eviction failed for whatever reason, return -1. This method must
        guarantee that the key/value pair that caused the issue has been retained in
        fast memory and that the problem has been logged internally.

        This method never raises.
        """
        ...  # pragma: nocover


class SpillBuffer(zict.Buffer[Key, object]):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target. If max_spill is provided the
    key/value pairs won't be spilled once this threshold has been reached.

    Parameters
    ----------
    spill_directory: str
        Location on disk to write the spill files to
    target: int
        Managed memory, in bytes, to start spilling at
    max_spill: int | False, optional
        Limit of number of bytes to be spilled on disk. Set to False to disable.
    """

    logged_pickle_errors: set[Key]
    #: (label, unit) -> ever-increasing cumulative value
    cumulative_metrics: defaultdict[tuple[str, str], float]

    def __init__(
        self,
        spill_directory: str,
        target: int,
        max_spill: int | Literal[False] = False,
    ):
        # If a value is still in use somewhere on the worker since the last time it was
        # unspilled, don't duplicate it
        slow = Slow(spill_directory, max_spill)
        slow_cached = zict.Cache(slow, zict.WeakValueMapping())

        super().__init__(fast={}, slow=slow_cached, n=target, weight=_in_memory_weight)
        self.logged_pickle_errors = set()  # keys logged with pickle error
        self.cumulative_metrics = defaultdict(float)

    @contextmanager
    def _capture_metrics(self) -> Iterator[None]:
        """Capture metrics re. disk read/write, serialize/deserialize, and
        compress/decompress.

        Note that this duplicates capturing from gather_dep, get_data, and execute. It
        is repeated here to make it possible to split serialize/deserialize and
        compress/decompress triggered by spill/unspill from those triggered by network
        comms.
        """

        def metrics_callback(label: Hashable, value: float, unit: str) -> None:
            assert isinstance(label, str)
            self.cumulative_metrics[label, unit] += value

        with context_meter.add_callback(metrics_callback):
            yield

    @contextmanager
    def _handle_errors(self, key: Key | None) -> Iterator[None]:
        try:
            yield
        except MaxSpillExceeded as e:
            # key is in self.fast; no keys have been lost on eviction
            (key_e,) = e.args
            assert key_e in self.fast
            assert key_e not in self.slow
            logger.warning(
                "Spill file on disk reached capacity; keeping data in memory"
            )
            raise HandledError()
        except OSError:
            # Typically, this is a disk full error
            logger.error("Spill to disk failed; keeping data in memory", exc_info=True)
            raise HandledError()
        except PickleError as e:
            assert e.key in self.fast
            assert e.key not in self.slow
            if e.key == key:
                assert key is not None
                # The key we just inserted failed to serialize.
                # This happens only when the key is individually larger than target.
                # The exception will be caught by Worker and logged; the status of
                # the task will be set to error.
                del self[key]
                raise
            else:
                # The key we just inserted is smaller than target, but it caused
                # another, unrelated key to be spilled out of the LRU, and that key
                # failed to serialize. There's nothing wrong with the new key. The older
                # key is still in memory.
                if e.key not in self.logged_pickle_errors:
                    logger.error("Failed to pickle %r", e.key, exc_info=True)
                    self.logged_pickle_errors.add(e.key)
                raise HandledError()

    def __setitem__(self, key: Key, value: object) -> None:
        """If sizeof(value) < target, write key/value pair to self.fast; this may in
        turn cause older keys to be spilled from fast to slow.
        If sizeof(value) >= target, write key/value pair directly to self.slow instead.

        Raises
        ------
        Exception
            sizeof(value) >= target, and value failed to pickle.
            The key/value pair has been forgotten.

        In all other cases:

        - an older value was evicted and failed to pickle,
        - this value or an older one caused the disk to fill and raise OSError,
        - this value or an older one caused the max_spill threshold to be exceeded,

        this method does not raise and guarantees that the key/value that caused the
        issue remained in fast.
        """
        try:
            with self._capture_metrics(), self._handle_errors(key):
                super().__setitem__(key, value)
                self.logged_pickle_errors.discard(key)
        except HandledError:
            assert key in self.fast
            assert key not in self.slow

    def evict(self) -> int:
        """Implementation of :meth:`ManualEvictProto.evict`.

        Manually evict the oldest key/value pair, even if target has not been
        reached. Returns sizeof(value).
        If the eviction failed (value failed to pickle, disk full, or max_spill
        exceeded), return -1; the key/value pair that caused the issue will remain in
        fast. The exception has been logged internally.
        This method never raises.
        """
        try:
            with self._capture_metrics(), self._handle_errors(None):
                _, _, weight = self.fast.evict()
                return cast(int, weight)
        except HandledError:
            return -1

    def __getitem__(self, key: Key) -> object:
        with self._capture_metrics():
            if key in self.fast:
                # Note: don't log from self.fast.__getitem__, because that's called
                # every time a key is evicted, and we don't want to count those events
                # here.
                memory_size = cast(int, self.fast.weights[key])
                # This is logged not only by the internal metrics callback but also by
                # those installed by gather_dep, get_data, and execute
                context_meter.digest_metric("memory-read", 1, "count")
                context_meter.digest_metric("memory-read", memory_size, "bytes")

            return super().__getitem__(key)

    def __delitem__(self, key: Key) -> None:
        super().__delitem__(key)
        self.logged_pickle_errors.discard(key)

    def pop(self, key: Key, default: object = None) -> object:
        raise NotImplementedError(
            "Are you calling .pop(key, None) as a way to discard a key if it exists?"
            "It may cause data to be read back from disk! Please use `del` instead."
        )

    @property
    def memory(self) -> Mapping[Key, object]:
        """Key/value pairs stored in RAM. Alias of zict.Buffer.fast.
        For inspection only - do not modify directly!
        """
        return self.fast

    @property
    def disk(self) -> Mapping[Key, object]:
        """Key/value pairs spilled out to disk. Alias of zict.Buffer.slow.
        For inspection only - do not modify directly!
        """
        return self.slow

    @property
    def _slow_uncached(self) -> Slow:
        cache = cast(zict.Cache, self.slow)
        return cast(Slow, cache.data)

    @property
    def spilled_total(self) -> SpilledSize:
        """Number of bytes spilled to disk. Tuple of

        - output of sizeof()
        - pickled size

        The two may differ substantially, e.g. if sizeof() is inaccurate or in case of
        compression.
        """
        return self._slow_uncached.total_weight


def _in_memory_weight(key: Key, value: object) -> int:
    return safe_sizeof(value)


# Internal exceptions. These are never raised by SpillBuffer.
class MaxSpillExceeded(Exception):
    pass


class PickleError(TypeError):
    def __str__(self) -> str:
        return f"Failed to pickle {self.key!r}"

    @property
    def key(self) -> Key:
        return self.args[0]


class HandledError(Exception):
    pass


class AnyKeyFile(zict.File):
    def _safe_key(self, key: Key) -> str:
        # We don't need _proper_ stringification, just a unique mapping
        return super()._safe_key(str(key))


class Slow(zict.Func[Key, object, bytes]):
    max_weight: int | Literal[False]
    weight_by_key: dict[Key, SpilledSize]
    total_weight: SpilledSize

    def __init__(self, spill_directory: str, max_weight: int | Literal[False] = False):
        compression = get_compression_settings(
            "distributed.worker.memory.spill-compression"
        )

        # File is MutableMapping[str, bytes], but serialize_bytelist returns
        # list[bytes | bytearray | memorymapping], which File.__setitem__ actually
        # accepts despite its signature; File.__getitem__ actually returns
        # bytearray. This headache is because MutableMapping doesn't allow for
        # asymmetric VT in __getitem__ and __setitem__.
        dump = cast(
            Callable[[object], bytes],
            partial(serialize_bytelist, compression=compression, on_error="raise"),
        )
        super().__init__(
            dump,
            deserialize_bytes,
            cast(MutableMapping[Key, bytes], AnyKeyFile(spill_directory)),
        )
        self.max_weight = max_weight
        self.weight_by_key = {}
        self.total_weight = SpilledSize(0, 0)

    def __getitem__(self, key: Key) -> object:
        with context_meter.meter("disk-read", "seconds"):
            pickled = self.d[key]
        context_meter.digest_metric("disk-read", 1, "count")
        context_meter.digest_metric("disk-read", len(pickled), "bytes")
        out = self.load(pickled)
        return out

    def __setitem__(self, key: Key, value: object) -> None:
        try:
            pickled = self.dump(value)
        except Exception as e:
            # zict.LRU ensures that the key remains in fast if we raise.
            # Wrap the exception so that it's recognizable by SpillBuffer,
            # which will then unwrap it.
            raise PickleError(key) from e

        # Thanks to Buffer.__setitem__, we never update existing
        # keys in slow, but always delete them and reinsert them.
        assert key not in self.d
        assert key not in self.weight_by_key

        pickled_size = sum(map(nbytes, pickled))

        if (
            self.max_weight is not False
            and self.total_weight.disk + pickled_size > self.max_weight
        ):
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            # To be caught by SpillBuffer.__setitem__
            raise MaxSpillExceeded(key)

        # Store to disk through File.
        # This may raise OSError, which is caught by SpillBuffer above.
        with context_meter.meter("disk-write", "seconds"):
            self.d[key] = pickled
        context_meter.digest_metric("disk-write", 1, "count")
        context_meter.digest_metric("disk-write", pickled_size, "bytes")

        weight = SpilledSize(safe_sizeof(value), pickled_size)
        self.weight_by_key[key] = weight
        self.total_weight += weight

    def __delitem__(self, key: Key) -> None:
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
