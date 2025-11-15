from __future__ import annotations

import inspect
import io
import logging
import pickle

import cloudpickle

from distributed.protocol.serialize import dask_deserialize, dask_serialize

HIGHEST_PROTOCOL = pickle.HIGHEST_PROTOCOL

logger = logging.getLogger(__name__)


class _DaskPickler(pickle.Pickler):
    def reducer_override(self, obj):
        # For some objects this causes segfaults otherwise, see
        # https://github.com/dask/distributed/pull/7564#issuecomment-1438727339
        if _always_use_pickle_for(obj):
            return NotImplemented
        try:
            serialize = dask_serialize.dispatch(type(obj))
            deserialize = dask_deserialize.dispatch(type(obj))
            return deserialize, serialize(obj)
        except TypeError:
            return NotImplemented


def _always_use_pickle_for(x):
    try:
        mod, _, _ = x.__class__.__module__.partition(".")
    except Exception:
        return False
    if mod == "numpy":
        import numpy as np

        return isinstance(x, np.ndarray)
    elif mod == "pandas":
        import pandas as pd

        return isinstance(x, pd.core.generic.NDFrame)
    elif mod == "builtins":
        return isinstance(x, (str, bytes))
    else:
        return False


def dumps(x, *, buffer_callback=None, protocol=HIGHEST_PROTOCOL):
    """Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    buffers = []
    dump_kwargs = {"protocol": protocol or HIGHEST_PROTOCOL}
    if dump_kwargs["protocol"] >= 5 and buffer_callback is not None:
        dump_kwargs["buffer_callback"] = buffers.append
    try:
        try:
            result = pickle.dumps(x, **dump_kwargs)
        except Exception:
            f = io.BytesIO()
            pickler = _DaskPickler(f, **dump_kwargs)
            buffers.clear()
            pickler.dump(x)
            result = f.getvalue()
        if b"__main__" in result or (
            getattr(inspect.getmodule(x), "__name__", None)
            in cloudpickle.list_registry_pickle_by_value()
        ):
            if len(result) < 1000 or not _always_use_pickle_for(x):
                buffers.clear()
                result = cloudpickle.dumps(x, **dump_kwargs)
    except Exception:
        try:
            buffers.clear()
            result = cloudpickle.dumps(x, **dump_kwargs)
        except Exception:
            logger.exception("Failed to serialize %s.", x)
            raise
    if buffer_callback is not None:
        for b in buffers:
            buffer_callback(b)
    return result


def loads(x, *, buffers=()):
    try:
        if buffers:
            return pickle.loads(x, buffers=buffers)
        else:
            return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise
