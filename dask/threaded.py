"""
A threaded shared-memory scheduler

See local.py
"""
import multiprocessing.pool
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from . import config
from .local import MultiprocessingPoolExecutor, get_async
from .system import CPU_COUNT
from .utils_test import add, inc  # noqa: F401

_EXECUTORS = threading.local()


class _ExecutorPool:
    """A thread-local pool of executors. When the thread closes, this pool will
    be GC'd and all contained executors shutdown"""

    def __init__(self):
        self.executors = {}

    def __del__(self):
        for executor in self.executors.values():
            executor.shutdown()

    def get(self, num_workers):
        if num_workers not in self.executors:
            self.executors[num_workers] = ThreadPoolExecutor(num_workers)
        return self.executors[num_workers]


def get_executor(num_workers=None):
    """Get a ThreadPoolExecutor given a specified number of workers.

    Executors are local to their calling thread, and are cached between
    subsequent calls. If the calling thread is closed, all executors will be
    automatically cleaned up.
    """
    if not hasattr(_EXECUTORS, "pool"):
        _EXECUTORS.pool = _ExecutorPool()
    if not num_workers:  # treat both 0 and None the same
        # TODO: if num_workers is 1 should we still use more threads?
        num_workers = CPU_COUNT
    return _EXECUTORS.pool.get(num_workers)


def _thread_get_id():
    return threading.current_thread().ident


def pack_exception(e, dumps):
    return e, sys.exc_info()[2]


def get(dsk, result, cache=None, num_workers=None, pool=None, **kwargs):
    """Threaded cached implementation of dask.get

    Parameters
    ----------

    dsk: dict
        A dask dictionary specifying a workflow
    result: key or list of keys
        Keys corresponding to desired data
    num_workers: integer of thread count
        The number of threads to use in the ThreadPool that will actually execute tasks
    cache: dict-like (optional)
        Temporary storage of results

    Examples
    --------

    >>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    >>> get(dsk, 'w')
    4
    >>> get(dsk, ['w', 'y'])
    (4, 2)
    """
    # Older versions of dask supported configuring a ThreadPool to use across
    # invocations, we preserve that here (even though we use
    # concurrent.futures.Executor` instances now).
    pool = pool or config.get("pool", None)
    num_workers = num_workers or config.get("num_workers", None)

    if pool is None:
        executor = get_executor(num_workers)
    elif isinstance(pool, multiprocessing.pool.Pool):
        executor = MultiprocessingPoolExecutor(pool)
    else:
        executor = pool

    return get_async(
        executor.submit,
        executor._max_workers,
        dsk,
        result,
        cache=cache,
        get_id=_thread_get_id,
        pack_exception=pack_exception,
        **kwargs
    )
