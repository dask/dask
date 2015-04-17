"""
A threaded shared-memory scheduler

See async.py
"""
from __future__ import absolute_import, division, print_function

from multiprocessing.pool import ThreadPool
from .async import get_async, inc, add
from .compatibility import Queue
from .context import _globals


default_pool = ThreadPool()


def get(dsk, result, cache=None, debug_counts=None, **kwargs):
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
    pool = _globals['pool']

    if pool is None:
        pool = default_pool

    queue = Queue()
    results = get_async(pool.apply_async, len(pool._pool), dsk, result,
                        cache=cache, debug_counts=debug_counts,
                        queue=queue, **kwargs)

    return results
