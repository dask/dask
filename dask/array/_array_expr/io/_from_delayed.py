from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import numpy as np

from dask.array._array_expr.io._base import IO
from dask.array.utils import meta_from_array

if TYPE_CHECKING:
    pass


class FromDelayed(IO):
    """Expression for creating an array from a delayed value."""

    _parameters = ["value", "shape", "dtype", "_meta", "_name_prefix"]
    _defaults = {"dtype": None, "_meta": None, "_name_prefix": None}

    @functools.cached_property
    def _meta(self):
        meta = self.operand("_meta")
        dtype = self.operand("dtype")
        shape = self.operand("shape")
        if meta is not None:
            if dtype is None:
                dtype = getattr(meta, "dtype", None)
            return meta_from_array(meta, dtype=dtype)
        if dtype is not None:
            return np.empty((0,) * len(shape), dtype=dtype)
        return np.empty((0,) * len(shape))

    @functools.cached_property
    def chunks(self):
        return tuple((d,) for d in self.operand("shape"))

    @functools.cached_property
    def _name(self):
        prefix = self.operand("_name_prefix")
        if prefix:
            return prefix
        return "from-delayed-" + self.deterministic_token

    def _layer(self):
        from dask._task_spec import Alias
        from dask.base import is_dask_collection

        value = self.operand("value")
        shape = self.operand("shape")
        key = (self._name,) + (0,) * len(shape)
        task = Alias(key=key, target=value.key)
        result = {key: task}
        # Include the delayed value's graph
        if is_dask_collection(value):
            result.update(value.__dask_graph__())
        return result


def from_delayed(value, shape, dtype=None, meta=None, name=None):
    """Create a dask array from a dask delayed value

    This routine is useful for constructing dask arrays in an ad-hoc fashion
    using dask delayed, particularly when combined with stack and concatenate.

    The dask array will consist of a single chunk.

    Examples
    --------
    >>> import dask
    >>> import dask.array as da
    >>> import numpy as np
    >>> value = dask.delayed(np.ones)(5)
    >>> array = da.from_delayed(value, (5,), dtype=float)
    >>> array
    dask.array<from-value, shape=(5,), dtype=float64, chunksize=(5,), chunktype=numpy.ndarray>
    >>> array.compute()
    array([1., 1., 1., 1., 1.])
    """
    from dask.array._array_expr._collection import new_collection
    from dask.delayed import Delayed, delayed

    # Convert to Delayed if it has a key but isn't a Delayed
    if not isinstance(value, Delayed) and hasattr(value, "key"):
        value = delayed(value)

    return new_collection(
        FromDelayed(
            value=value,
            shape=shape,
            dtype=dtype,
            _meta=meta,
            _name_prefix=name,
        )
    )
