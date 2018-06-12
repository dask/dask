from .. import optimization as oz
from .. import optimization_compile as ozc
from ..utils import ensure_dict


def cull(x):
    dsk = ensure_dict(x.__dask_graph__())
    dsk, dependencies = oz.cull(dsk, x.__dask_keys__())
    func, args = x.__dask_postpersist__()
    return func(dsk, *args), dependencies


def compiled(x):
    dsk = ozc.compiled(x.__dask_graph__(), x.__dask_keys__())
    func, args = x.__dask_postpersist__()
    return func(dsk, *args)


def preserve_keys(x):
    dsk = ozc.preserve_keys(x.__dask_graph__(), x.__dask_keys__())
    func, args = x.__dask_postpersist__()
    return func(dsk, *args)


def preserve_deps(x):
    dsk = ozc.preserve_deps(x.__dask_graph__(), x.__dask_keys__())
    func, args = x.__dask_postpersist__()
    return func(dsk, *args)
