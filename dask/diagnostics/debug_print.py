from __future__ import annotations

import builtins
import threading
from collections import namedtuple
from typing import Callable

from dask.typing import DaskCollection

QueueItem = namedtuple("QueueItem", ["key", "handler"])

_queue: list[QueueItem] = []
_queue_lock = threading.Lock()


def _make_key(collection: DaskCollection):
    """Make a tuple of keys in this collection."""
    from dask.base import flatten

    key = flatten(collection.__dask_keys__())
    if isinstance(key, str):
        key = [key]
    return set(key)


def dequeue(collections: list[DaskCollection]) -> list[Callable]:
    """Dequeue print handlers for collections that are being computed.

    Parameters
    ----------
    collections : list[DaskCollection]
        List of Dask collections being computed.

    Returns
    -------
    list[Delayed]
        List of delayed print handlers.
    """
    global _queue

    computed_keys = set()
    for collection in collections:
        computed_keys.update(_make_key(collection))

    result = []
    with _queue_lock:
        indices = []
        for i, item in enumerate(_queue):
            if item.key.issubset(computed_keys):
                result.append(item.handler)
                indices.append(i)
        _queue = [item for i, item in enumerate(_queue) if i not in indices]
    return result


def enqueue(msg: str, collection: DaskCollection) -> None:
    """Enqueue a delayed print statement for a value. After the value is
    computed, the result will be printed.

    Parameters
    ----------
    msg : str
        Format string for the message.
    collection : DaskCollection
        Collection to print

    Returns
    -------

    """
    from dask import delayed

    @delayed  # type: ignore
    def print_future(result) -> None:
        try:
            from distributed.worker import print
        except ImportError:
            print = builtins.print
        if "{result" in msg:
            print(msg.format(result=result))
        elif "%" in msg:
            print(msg % result)
        else:
            print(msg.format(result))

    key = _make_key(collection)
    with _queue_lock:
        _queue.append(QueueItem(key=key, handler=print_future(collection)))
