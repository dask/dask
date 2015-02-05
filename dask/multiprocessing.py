from __future__ import absolute_import, division, print_function

from toolz import curry, pipe, partial
from .optimize import fuse, cull
import multiprocessing
import psutil
import dill
import pickle
from .async import get_async # TODO: get better get


def get(dsk, keys, optimizations=[fuse]):
    """ Multiprocessed get function appropriate for Bags """
    pool = multiprocessing.Pool(psutil.cpu_count())
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    apply_async = dill_apply_async(pool.apply_async)

    # Optimize Dask
    dsk2 = pipe(dsk, partial(cull, keys=keys), *optimizations)

    try:
        # Run
        result = get_async(apply_async, psutil.cpu_count(), dsk2, keys,
                           queue=queue)
    finally:
        pool.close()
    return result


def dill_apply_func(sfunc, sargs, skwds):
    func = dill.loads(sfunc)
    args = dill.loads(sargs)
    kwds = dill.loads(skwds)
    return func(*args, **kwds)

@curry
def dill_apply_async(apply_async, func, args=(), kwds={}):
    sfunc = dill.dumps(func)
    sargs = dill.dumps(args)
    skwds = dill.dumps(kwds)
    return apply_async(dill_apply_func, args=[sfunc, sargs, skwds])
