from __future__ import absolute_import, division, print_function

from toolz import curry, pipe, partial
from .optimize import fuse, cull
import multiprocessing
import dill
import pickle
from .async import get_async # TODO: get better get
from .context import _globals


def get(dsk, keys, optimizations=[fuse], num_workers=None,
        func_loads=None, func_dumps=None):
    """ Multiprocessed get function appropriate for Bags

    Parameters
    ----------

    dsk: dict
        dask graph
    keys: object or list
        Desired results from graph
    optimizations: list of functions
        optimizations to perform on graph before execution
    num_workers: int
        Number of worker processes (defaults to number of cores)
    func_dumps: function
        Function to use for function serialization (defaults to dill.dumps)
    func_loads: function
        Function to use for function deserialization (defaults to dill.loads)
    """
    pool = _globals['pool']
    if pool is None:
        pool = multiprocessing.Pool(num_workers)
        cleanup = True
    else:
        cleanup = False

    manager = multiprocessing.Manager()
    queue = manager.Queue()

    apply_async = dill_apply_async(pool.apply_async,
                                   func_dumps=func_dumps, func_loads=func_loads)

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
                     func_loads=None, func_dumps=None):
    dumps = func_dumps or _globals.get('func_dumps') or dill.dumps
    sfunc = dumps(func)
    sargs = dumps(args)
    skwds = dumps(kwds)
    return apply_async(curry(apply_func, loads=func_loads),
                       args=[sfunc, sargs, skwds])
