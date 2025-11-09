from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from collections.abc import Callable, Collection, Coroutine, Mapping
from functools import partial
from itertools import cycle
from typing import Any, TypeVar

from tlz import drop, groupby, merge

import dask.config
from dask._task_spec import TaskRef
from dask.typing import Key
from dask.utils import is_namedtuple_instance, parse_timedelta

from distributed.core import ConnectionPool, rpc
from distributed.utils import All

logger = logging.getLogger(__name__)


# Backwards compat
WrappedKey = TaskRef


async def gather_from_workers(
    who_has: Mapping[Key, Collection[str]],
    rpc: ConnectionPool,
    *,
    serializers: list[str] | None = None,
    who: str | None = None,
) -> tuple[dict[Key, object], list[Key], list[Key], list[str]]:
    """Gather data directly from peers

    Parameters
    ----------
    who_has:
        mapping from keys to worker addresses
    rpc:
        RPC channel to use

    Returns
    -------
    Tuple:

    - Successfully retrieved: ``{key: value, ...}``
    - Keys that were not available on any worker: ``[key, ...]``
    - Keys that raised exception; e.g. failed to deserialize: ``[key, ...]``
    - Workers that failed to respond: ``[address, ...]``

    See Also
    --------
    gather
    _gather
    Scheduler.get_who_has
    """
    from distributed.worker import get_data_from_worker

    to_gather = {k: set(v) for k, v in who_has.items()}
    data: dict[Key, object] = {}
    failed_keys: list[Key] = []
    missing_workers: set[str] = set()
    busy_workers: set[str] = set()

    while to_gather:
        d = defaultdict(list)
        for key, addresses in to_gather.items():
            addresses -= missing_workers
            ready_addresses = addresses - busy_workers
            if ready_addresses:
                d[random.choice(list(ready_addresses))].append(key)

        if not d:
            if busy_workers:
                await asyncio.sleep(0.15)
                busy_workers.clear()
                continue

            return data, list(to_gather), failed_keys, list(missing_workers)

        tasks = [
            asyncio.create_task(
                retry_operation(
                    partial(
                        get_data_from_worker,
                        rpc,
                        keys,
                        address,
                        who=who,
                        serializers=serializers,
                    ),
                    operation="get_data_from_worker",
                ),
                name=f"get-data-from-{address}",
            )
            for address, keys in d.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for address, r in zip(d, results):
            if isinstance(r, OSError):
                missing_workers.add(address)
            elif isinstance(r, Exception):
                # For example, deserialization error
                logger.error(
                    "Unexpected error while collecting tasks %s from %s",
                    d[address],
                    address,
                    exc_info=r,
                )
                for key in d[address]:
                    failed_keys.append(key)
                    del to_gather[key]
            elif isinstance(r, BaseException):  # pragma: nocover
                # for example, asyncio.CancelledError
                raise r
            else:
                assert isinstance(r, dict), r
                if r["status"] == "busy":
                    busy_workers.add(address)
                    continue

                assert r["status"] == "OK"
                for key in d[address]:
                    if key in r["data"]:
                        data[key] = r["data"][key]
                        del to_gather[key]
                    else:
                        to_gather[key].remove(address)

    return data, [], failed_keys, list(missing_workers)


_round_robin_counter = [0]


async def scatter_to_workers(workers, data, rpc=rpc):
    """Scatter data directly to workers

    This distributes data in a round-robin fashion to a set of workers.

    See scatter for parameter docstring
    """
    assert isinstance(data, dict)

    workers = sorted(workers)
    names, data = list(zip(*data.items()))

    worker_iter = drop(_round_robin_counter[0] % len(workers), cycle(workers))
    _round_robin_counter[0] += len(data)

    L = list(zip(worker_iter, names, data))
    d = groupby(0, L)
    d = {worker: {key: value for _, key, value in v} for worker, v in d.items()}

    rpcs = {addr: rpc(addr) for addr in d}
    try:
        out = await All([rpcs[address].update_data(data=v) for address, v in d.items()])
    finally:
        for r in rpcs.values():
            await r.close_rpc()

    nbytes = merge(o["nbytes"] for o in out)

    who_has = {k: [w for w, _, _ in v] for k, v in groupby(1, L).items()}

    return (names, who_has, nbytes)


collection_types = (tuple, list, set, frozenset)


def _namedtuple_packing(o: Any, handler: Callable[..., Any]) -> Any:
    """Special pack/unpack dispatcher for namedtuples respecting their potential constructors."""
    assert is_namedtuple_instance(o)
    typ = type(o)
    if hasattr(o, "__getnewargs_ex__"):
        args, kwargs = o.__getnewargs_ex__()
        handled_args = [handler(item) for item in args]
        handled_kwargs = {k: handler(v) for k, v in kwargs.items()}
        return typ(*handled_args, **handled_kwargs)
    args = o.__getnewargs__() if hasattr(typ, "__getnewargs__") else o
    handled_args = [handler(item) for item in args]
    return typ(*handled_args)


def _unpack_remotedata_inner(
    o: Any, byte_keys: bool, found_futures: set[TaskRef]
) -> Any:
    """Inner implementation of `unpack_remotedata` that adds found wrapped keys to `found_futures`"""

    typ = type(o)
    if typ is tuple:
        if not o:
            return o
        else:
            return tuple(
                _unpack_remotedata_inner(item, byte_keys, found_futures) for item in o
            )
    elif is_namedtuple_instance(o):
        return _namedtuple_packing(
            o,
            partial(
                _unpack_remotedata_inner,
                byte_keys=byte_keys,
                found_futures=found_futures,
            ),
        )

    if typ in collection_types:
        if not o:
            return o
        outs = [_unpack_remotedata_inner(item, byte_keys, found_futures) for item in o]
        return typ(outs)
    elif typ is dict:
        if o:
            return {
                k: _unpack_remotedata_inner(v, byte_keys, found_futures)
                for k, v in o.items()
            }
        else:
            return o
    elif issubclass(typ, TaskRef):  # TODO use type is Future
        k = o.key
        found_futures.add(o)
        return k
    else:
        return o


def unpack_remotedata(o: Any, byte_keys: bool = False) -> tuple[Any, set]:
    """Unpack TaskRef objects from collection

    Returns original collection and set of all found TaskRef objects

    Examples
    --------
    >>> rd = TaskRef('mykey')
    >>> unpack_remotedata(1)
    (1, set())
    >>> unpack_remotedata(())
    ((), set())
    >>> unpack_remotedata(rd)
    ('mykey', {TaskRef('mykey')})
    >>> unpack_remotedata([1, rd])
    ([1, 'mykey'], {TaskRef('mykey')})
    >>> unpack_remotedata({1: rd})
    ({1: 'mykey'}, {TaskRef('mykey')})
    >>> unpack_remotedata({1: [rd]})
    ({1: ['mykey']}, {TaskRef('mykey')})

    Use the ``byte_keys=True`` keyword to force string keys

    >>> rd = TaskRef(('x', 1))
    >>> unpack_remotedata(rd, byte_keys=True)
    ("('x', 1)", {TaskRef('('x', 1)')})
    """
    found_futures: set[TaskRef] = set()
    return _unpack_remotedata_inner(o, byte_keys, found_futures), found_futures


def pack_data(o, d, key_types=object):
    """Merge known data into tuple or dict

    Parameters
    ----------
    o
        core data structures containing literals and keys
    d : dict
        mapping of keys to data

    Examples
    --------
    >>> data = {'x': 1}
    >>> pack_data(('x', 'y'), data)
    (1, 'y')
    >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
    {'a': 1, 'b': 'y'}
    >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
    {'a': [1], 'b': 'y'}
    """
    typ = type(o)
    try:
        if isinstance(o, key_types) and o in d:
            return d[o]
    except TypeError:
        pass

    if typ in collection_types:
        return typ([pack_data(x, d, key_types=key_types) for x in o])
    elif typ is dict:
        return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
    elif is_namedtuple_instance(o):
        return _namedtuple_packing(o, partial(pack_data, d=d, key_types=key_types))
    else:
        return o


def subs_multiple(o, d):
    """Perform substitutions on a tasks

    Parameters
    ----------
    o
        Core data structures containing literals and keys
    d : dict
        Mapping of keys to values

    Examples
    --------
    >>> dsk = {"a": (sum, ["x", 2])}
    >>> data = {"x": 1}
    >>> subs_multiple(dsk, data)  # doctest: +SKIP
    {'a': (sum, [1, 2])}

    """
    typ = type(o)
    if typ is tuple and o and callable(o[0]):  # istask(o)
        return (o[0],) + tuple(subs_multiple(i, d) for i in o[1:])
    elif typ is list:
        return [subs_multiple(i, d) for i in o]
    elif typ is dict:
        return {k: subs_multiple(v, d) for (k, v) in o.items()}
    else:
        try:
            return d.get(o, o)
        except TypeError:
            return o


T = TypeVar("T")


async def retry(
    coro: Callable[[], Coroutine[Any, Any, T]],
    count: int,
    delay_min: float,
    delay_max: float,
    jitter_fraction: float = 0.1,
    retry_on_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (
        EnvironmentError,
        IOError,
    ),
    operation: str | None = None,
) -> T:
    """
    Return the result of ``await coro()``, re-trying in case of exceptions

    The delay between attempts is ``delay_min * (2 ** i - 1)`` where ``i`` enumerates the attempt that just failed
    (starting at 0), but never larger than ``delay_max``.
    This yields no delay between the first and second attempt, then ``delay_min``, ``3 * delay_min``, etc.
    (The reason to re-try with no delay is that in most cases this is sufficient and will thus recover faster
    from a communication failure).

    Parameters
    ----------
    coro
        The coroutine function to call and await
    count
        The maximum number of re-tries before giving up. 0 means no re-try; must be >= 0.
    delay_min
        The base factor for the delay (in seconds); this is the first non-zero delay between re-tries.
    delay_max
        The maximum delay (in seconds) between consecutive re-tries (without jitter)
    jitter_fraction
        The maximum jitter to add to the delay, as fraction of the total delay. No jitter is added if this
        value is <= 0.
        Using a non-zero value here avoids "herd effects" of many operations re-tried at the same time
    retry_on_exceptions
        A tuple of exception classes to retry. Other exceptions are not caught and re-tried, but propagate immediately.
    operation
        A human-readable description of the operation attempted; used only for logging failures

    Returns
    -------
    Any
        Whatever `await coro()` returned
    """
    # this loop is a no-op in case max_retries<=0
    for i_try in range(count):
        try:
            return await coro()
        except retry_on_exceptions as ex:
            operation = operation or str(coro)
            logger.info(
                f"Retrying {operation} after exception in attempt {i_try}/{count}: {ex}"
            )
            delay = min(delay_min * (2**i_try - 1), delay_max)
            if jitter_fraction > 0:
                delay *= 1 + random.random() * jitter_fraction
            await asyncio.sleep(delay)
    return await coro()


# ParamSpec is not supported here due to the additional "operation" kwarg
async def retry_operation(
    coro: Callable[..., Coroutine[Any, Any, T]],
    *args: object,
    operation: str | None = None,
    **kwargs: object,
) -> T:
    """
    Retry an operation using the configuration values for the retry parameters
    """

    retry_count = dask.config.get("distributed.comm.retry.count")
    retry_delay_min = parse_timedelta(
        dask.config.get("distributed.comm.retry.delay.min"), default="s"
    )
    retry_delay_max = parse_timedelta(
        dask.config.get("distributed.comm.retry.delay.max"), default="s"
    )
    return await retry(
        partial(coro, *args, **kwargs),
        count=retry_count,
        delay_min=retry_delay_min,
        delay_max=retry_delay_max,
        operation=operation,
    )
