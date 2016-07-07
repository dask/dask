from __future__ import absolute_import, division, print_function

import multiprocessing
import pickle
import sys

from .async import get_async  # TODO: get better get
from .context import _globals
from .optimize import fuse, cull

import cloudpickle
from toolz import curry


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

    manager = multiprocessing.Manager()
    queue = manager.Queue()

    apply_async = pickle_apply_async(pool.apply_async,
                                     func_dumps=func_dumps,
                                     func_loads=func_loads)

    # Optimize Dask
    dsk2, dependencies = cull(dsk, keys)
    if optimize_graph:
        dsk3, dependencies = fuse(dsk2, keys, dependencies)
    else:
        dsk3 = dsk2

    try:
        # Run
        result = get_async(apply_async, len(pool._pool), dsk3, keys,
                           queue=queue, get_id=_process_get_id, **kwargs)
    finally:
        if cleanup:
            pool.close()
    return result


def apply_func(sfunc, may_fail, wont_fail, loads=None):
    loads = loads or _globals.get('loads') or _loads
    func = loads(sfunc)
    key, queue, get_id, raise_on_exception = loads(wont_fail)
    try:
        task, data = loads(may_fail)
    except Exception as e:
        # Need a new reference for the exception, as `e` falls out of scope in
        # python 3
        exception = e
        def serialization_failure():
            raise exception
        task = (serialization_failure,)
        data = {}
    return func(key, task, data, queue, get_id,
                raise_on_exception=raise_on_exception)


@curry
def pickle_apply_async(apply_async, func, args=(),
                       func_loads=None, func_dumps=None):
    # XXX: To deal with deserialization errors of tasks, this version of
    # apply_async doesn't actually match that of `pool.apply_async`. It's
    # customized to fit the signature of `dask.async.execute_task`, which is
    # the only function ever actually passed as `func`. This is a bit of a
    # hack, but it works pretty well. If the signature of `execute_task`
    # changes, then this will need to be changed as well.
    dumps = func_dumps or _globals.get('func_dumps') or _dumps
    key, task, data, queue, get_id, raise_on_exception = args
    sfunc = dumps(func)
    may_fail = dumps((task, data))
    wont_fail = dumps((key, queue, get_id, raise_on_exception))
    return apply_async(curry(apply_func, loads=func_loads),
                       args=[sfunc, may_fail, wont_fail])
