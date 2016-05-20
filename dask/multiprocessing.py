from __future__ import absolute_import, division, print_function

from toolz import curry, pipe, partial
from .optimize import fuse, cull
import multiprocessing
from .async import get_async # TODO: get better get
from .context import _globals
from sys import version

if version < '3':
    import copy_reg as copyreg
else:
    import copyreg

def _reduce_method_descriptor(m):
    return getattr, (m.__objclass__, m.__name__)

# type(set.union) is used as a proxy to <class 'method_descriptor'>
copyreg.pickle(type(set.union), _reduce_method_descriptor)

import pickle
import cloudpickle

def _dumps(x):
    return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)

_loads = pickle.loads


def _process_get_id():
    return multiprocessing.current_process().ident


def get(dsk, keys, optimizations=[], num_workers=None,
        func_loads=None, func_dumps=None, **kwargs):
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
        Function to use for function serialization
        (defaults to cloudpickle.dumps)
    func_loads: function
        Function to use for function deserialization
        (defaults to cloudpickle.loads)
    """
    pool = _globals['pool']
    if pool is None:
        pool = multiprocessing.Pool(num_workers)
        cleanup = True
    else:
        cleanup = False

    manager = multiprocessing.Manager()
    queue = manager.Queue()

    apply_async = pickle_apply_async(pool.apply_async,
                                          func_dumps=func_dumps,
                                          func_loads=func_loads)

    # Optimize Dask
    dsk2, dependencies = cull(dsk, keys)
    dsk3, dependencies = fuse(dsk2, keys, dependencies)
    dsk4 = pipe(dsk3, *optimizations)

    try:
        # Run
        result = get_async(apply_async, len(pool._pool), dsk3, keys,
                           queue=queue, get_id=_process_get_id, **kwargs)
    finally:
        if cleanup:
            pool.close()
    return result


def apply_func(sfunc, sargs, skwds, loads=None):
    loads = loads or _globals.get('loads') or _loads
    func = loads(sfunc)
    args = loads(sargs)
    kwds = loads(skwds)
    return func(*args, **kwds)


@curry
def pickle_apply_async(apply_async, func, args=(), kwds={},
                            func_loads=None, func_dumps=None):
    dumps = func_dumps or _globals.get('func_dumps') or _dumps
    sfunc = dumps(func)
    sargs = dumps(args)
    skwds = dumps(kwds)
    return apply_async(curry(apply_func, loads=func_loads),
                       args=[sfunc, sargs, skwds])
