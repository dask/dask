"""
A threaded shared-memory scheduler

See async.py
"""
from __future__ import absolute_import, division, print_function

from multiprocessing.pool import ThreadPool
from threading import current_thread
from .async import get_async, inc, add
from .compatibility import Queue
from .context import _globals


default_pool = ThreadPool()


def _thread_get_id():
    return current_thread().ident


def get(dsk, result, cache=None, num_workers=None, **kwargs):
    """ Threaded cached implementation of dask.get

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
    pool = _globals['pool']

    if pool is None:
        if num_workers:
            pool = ThreadPool(num_workers)
        else:
            pool = default_pool

    queue = Queue()
    results = get_async(pool.apply_async, len(pool._pool), dsk, result,
                        cache=cache, queue=queue, get_id=_thread_get_id,
                        **kwargs)

    return results
