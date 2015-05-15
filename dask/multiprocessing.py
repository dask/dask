from __future__ import absolute_import, division, print_function

from toolz import curry, pipe, partial
from .optimize import fuse, cull
import multiprocessing
import dill
import pickle
from .async import get_async # TODO: get better get
from .context import _globals


def get(dsk, keys, optimizations=[fuse], num_workers=None,
        loads=None, dumps=None):
    """ Multiprocessed get function appropriate for Bags """
    pool = _globals['pool']
    if pool is None:
        pool = multiprocessing.Pool(num_workers)
        cleanup = True
    else:
        cleanup = False

    manager = multiprocessing.Manager()
    queue = manager.Queue()

    apply_async = dill_apply_async(pool.apply_async, dumps=dumps, loads=loads)

    # Optimize Dask
    dsk2 = pipe(dsk, partial(cull, keys=keys), *optimizations)

    try:
        # Run
        result = get_async(apply_async, len(pool._pool), dsk2, keys,
                           queue=queue)
    finally:
        if cleanup:
            pool.close()
    return result


def apply_func(sfunc, sargs, skwds, loads=None):
    loads = loads or _globals.get('loads') or dill.loads
    func = loads(sfunc)
    args = loads(sargs)
    kwds = loads(skwds)
    return func(*args, **kwds)

@curry
def dill_apply_async(apply_async, func, args=(), kwds={},
                     loads=None, dumps=None):
    dumps = dumps or _globals.get('dumps') or dill.dumps
    sfunc = dumps(func)
    sargs = dumps(args)
    skwds = dumps(kwds)
    return apply_async(curry(apply_func, loads=loads),
                       args=[sfunc, sargs, skwds])
