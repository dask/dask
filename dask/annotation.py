import functools


def annotate_func(func: callable, annotation: dict) -> callable:
    ret = functools.partial(func)
    if not hasattr(ret, "_dask_annotation_dict"):
        ret._dask_annotation_dict = {}
        functools.update_wrapper(
            ret, func, functools.WRAPPER_ASSIGNMENTS + ("__repr__",)
        )
    ret._dask_annotation_dict.update(annotation)
    return ret


def get_annotation(obj: object) -> dict:
    if hasattr(obj, "_dask_annotation_dict"):
        return obj._dask_annotation_dict
    else:
        return {}
