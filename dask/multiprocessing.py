from __future__ import absolute_import, division, print_function

import multiprocessing
import pickle
import sys

from .async import get_async  # TODO: get better get
from .context import _globals
from .optimize import fuse, cull

import cloudpickle


if sys.version_info.major < 3:
    import copy_reg as copyreg
else:
    import copyreg


def _reduce_method_descriptor(m):
    return getattr, (m.__objclass__, m.__name__)


# type(set.union) is used as a proxy to <class 'method_descriptor'>
copyreg.pickle(type(set.union), _reduce_method_descriptor)


def _dumps(x):
    return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)


_loads = pickle.loads


def _process_get_id():
    return multiprocessing.current_process().ident


def get(dsk, keys, num_workers=None, func_loads=None, func_dumps=None,
        optimize_graph=True, **kwargs):
    """ Multiprocessed get function appropriate for Bags

    Parameters
    ----------
    dsk : dict
        dask graph
    keys : object or list
        Desired results from graph
    num_workers : int
        Number of worker processes (defaults to number of cores)
    func_dumps : function
        Function to use for function serialization
        (defaults to cloudpickle.dumps)
    func_loads : function
        Function to use for function deserialization
        (defaults to cloudpickle.loads)
    optimize_graph : bool
        If True [default], `fuse` is applied to the graph before computation.
    """
    pool = _globals['pool']
    if pool is None:
        pool = multiprocessing.Pool(num_workers)
        cleanup = True
    else:
        cleanup = False

    # Optimize Dask
    dsk2, dependencies = cull(dsk, keys)
    if optimize_graph:
        dsk3, dependencies = fuse(dsk2, keys, dependencies)
    else:
        dsk3 = dsk2

    # We specify marshalling functions in order to catch serialization
    # errors and report them to the user.
    loads = func_loads or _globals.get('func_loads') or _loads
    dumps = func_dumps or _globals.get('func_dumps') or _dumps

    # Note former versions used a multiprocessing Manager to share
    # a Queue between parent and workers, but this is fragile on Windows
    # (issue #1652).
    try:
        # Run
        result = get_async(pool.apply_async, len(pool._pool), dsk3, keys,
                           get_id=_process_get_id,
                           dumps=dumps, loads=loads, **kwargs)
    finally:
        if cleanup:
            pool.close()
    return result
