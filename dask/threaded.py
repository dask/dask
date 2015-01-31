"""
A threaded shared-memory scheduler

See async.py
"""
from __future__ import absolute_import, division, print_function

from multiprocessing.pool import ThreadPool
import psutil
from .async import get_async, inc, add
from .compatibility import Queue


NUM_CPUS = psutil.cpu_count()


def get(dsk, result, nthreads=NUM_CPUS, cache=None, debug_counts=None, **kwargs):
    """ Threaded cached implementation of dask.get

    Parameters
    ----------

    dsk: dict
        A dask dictionary specifying a workflow
    result: key or list of keys
        Keys corresponding to desired data
    nthreads: integer of thread count
        The number of threads to use in the ThreadPool that will actually execute tasks
    cache: dict-like (optional)
        Temporary storage of results
    debug_counts: integer or None
        This integer tells how often the scheduler should dump debugging info

    Examples
    --------

    >>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    >>> get(dsk, 'w')
    4
    >>> get(dsk, ['w', 'y'])
    (4, 2)
    """
    pool = ThreadPool(nthreads)
    queue = Queue()
    try:
        results = get_async(pool.apply_async, nthreads, dsk, result,
                            cache=cache, debug_counts=debug_counts,
                            queue=queue, **kwargs)
    finally:
        pool.close()
        pool.join()

    return results
