from .. import optimization as oz
from .. import optimization_compile as ozc
from ..base import tokenize
from ..core import flatten
from ..sharedict import ShareDict
from ..utils import ensure_dict


def get_name_from_keys(keys):
    names = set()
    for name in flatten(keys):
        if type(name) is tuple:
            name = name[0]
        names.add(name)
    if len(names) == 1:
        return names.pop()
    else:
        # e.g. xarray.Dataset
        return 'multiname-' + tokenize(names)


def postpersist(x, dsk):
    if dsk is x.__dask_graph__():
        return x
    if isinstance(dsk, ShareDict):
        sharedsk = dsk
    else:
        sharedsk = ShareDict()
        name = get_name_from_keys(x.__dask_keys__())
        sharedsk.update_with_key(dsk, name)
    func, args = x.__dask_postpersist__()
    return func(sharedsk, *args)


def cull(x):
    dsk = x.__dask_graph__()
    if not dsk:
        return x, {}
    dsk = ensure_dict(dsk)
    dsk, dependencies = oz.cull(dsk, x.__dask_keys__())
    return postpersist(x, dsk), dependencies


def apply_oz(oz_func):
    def wrapper(x, **kwargs):
        dsk = x.__dask_graph__()
        if not dsk:
            return x
        dsk = oz_func(dsk, x.__dask_keys__(), **kwargs)
        return postpersist(x, dsk)
    return wrapper


compiled = apply_oz(ozc.compiled)
preserve_keys = apply_oz(ozc.preserve_keys)
preserve_deps = apply_oz(ozc.preserve_deps)
