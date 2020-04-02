import functools


def annotate_func(func: callable, annotation: dict) -> callable:
    ret = functools.partial(func)
    if not hasattr(ret, "_dask_annotation_dict"):
        ret._dask_annotation_dict = {}
        if hasattr(func, "__name__"):
            ret.__name__ = func
        if hasattr(func, "__repr__"):
            ret.__repr__ = func
        if hasattr(func, "__str__"):
            ret.__str__ = func
    ret._dask_annotation_dict.update(annotation)
    return ret


def get_annotation(obj: object) -> dict:
    if hasattr(obj, "_dask_annotation_dict"):
        return obj._dask_annotation_dict
    else:
        return {}
