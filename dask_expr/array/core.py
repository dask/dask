import functools
import operator
from itertools import product
from typing import Iterable, Union

import numpy as np
from dask import istask
from dask.array.core import (
    _should_delegate,
    finalize,
    getter,
    getter_inline,
    getter_nofancy,
    graph_from_arraylike,
    normalize_chunks,
    slices_from_chunks,
)
from dask.array.utils import meta_from_array
from dask.base import DaskMethodsMixin, named_schedulers
from dask.core import flatten
from dask.utils import SerializableLock, cached_cumsum, cached_property, key_split
from toolz import reduce

from dask_expr import _core as core
from dask_expr._util import _tokenize_deterministic

T_IntOrNaN = Union[int, float]  # Should be Union[int, Literal[np.nan]]


class Array(core.Expr, DaskMethodsMixin):
    _cached_keys = None

    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)

    def __dask_postcompute__(self):
        return finalize, ()

    def __dask_postpersist__(self):
        state = self.lower_completely()
        return FromGraph, (
            state._meta,
            state.chunks,
            list(flatten(state.__dask_keys__())),
            key_split(state._name),
        )

    def __dask_graph__(self):
        expr = self.lower_completely()
        if expr._name == self._name:
            return super().__dask_graph__()
        return expr.__dask_graph__()

    def compute(self, **kwargs):
        return DaskMethodsMixin.compute(self.simplify(), **kwargs)

    def persist(self, **kwargs):
        return DaskMethodsMixin.persist(self.simplify(), **kwargs)

    def __array_ufunc__(self, numpy_ufunc, method, *inputs, **kwargs):
        raise NotImplementedError()

    def __array_function__(self, func, types, args, kwargs):
        # TODO: look at dask.array implementation to find lots of other cases
        import dask_expr.array as module

        for submodule in func.__module__.split(".")[1:]:
            try:
                module = getattr(module, submodule)
            except AttributeError:
                # TODO
                # return handle_nonmatching_names(func, args, kwargs)
                raise

        da_func = getattr(module, func.__name__)
        return da_func(*args, **kwargs)

    def __array__(self):
        return self.compute()

    def __getitem__(self, index):
        from dask.array.slicing import normalize_index

        from dask_expr.array.slicing import Slice

        if not isinstance(index, tuple):
            index = (index,)

        index2 = normalize_index(index, self.shape)

        # TODO: handle slicing with dask array

        return Slice(self, index2)

    @cached_property
    def shape(self) -> tuple[T_IntOrNaN, ...]:
        return tuple(cached_cumsum(c, initial_zero=True)[-1] for c in self.chunks)

    @property
    def ndim(self):
        return len(self.shape)

    @property
    def chunksize(self) -> tuple[T_IntOrNaN, ...]:
        return tuple(max(c) for c in self.chunks)

    @property
    def dtype(self):
        if isinstance(self._meta, tuple):
            dtype = self._meta[0].dtype
        else:
            dtype = self._meta.dtype
        return dtype

    def __dask_keys__(self):
        out = self.lower_completely()
        if self._cached_keys is not None:
            return self._cached_keys

        name, chunks, numblocks = out.name, out.chunks, out.numblocks

        def keys(*args):
            if not chunks:
                return [(name,)]
            ind = len(args)
            if ind + 1 == len(numblocks):
                result = [(name,) + args + (i,) for i in range(numblocks[ind])]
            else:
                result = [keys(*(args + (i,))) for i in range(numblocks[ind])]
            return result

        self._cached_keys = result = keys()
        return result

    @cached_property
    def numblocks(self):
        return tuple(map(len, self.chunks))

    @cached_property
    def npartitions(self):
        return reduce(operator.mul, self.numblocks, 1)

    @property
    def name(self):
        return self._name

    def __hash__(self):
        return hash(self._name)

    def optimize(self):
        return self.simplify()

    def rechunk(
        self,
        chunks="auto",
        threshold=None,
        block_size_limit=None,
        balance=False,
        method=None,
    ):
        from dask_expr.array.rechunk import Rechunk

        if isinstance(chunks, tuple):
            assert len(chunks) == self.ndim

        return Rechunk(self, chunks, threshold, block_size_limit, balance, method)

    def transpose(self, axes=None):
        if axes:
            if len(axes) != self.ndim:
                raise ValueError("axes don't match array")
            axes = tuple(d + self.ndim if d < 0 else d for d in axes)
        else:
            axes = tuple(range(self.ndim))[::-1]

        return Transpose(self, axes)

    @property
    def T(self):
        return self.transpose()

    def __add__(self, other):
        return elemwise(operator.add, self, other)

    def __radd__(self, other):
        return elemwise(operator.add, other, self)

    def __mul__(self, other):
        return elemwise(operator.add, self, other)

    def __rmul__(self, other):
        return elemwise(operator.mul, other, self)

    def __sub__(self, other):
        return elemwise(operator.sub, self, other)

    def __rsub__(self, other):
        return elemwise(operator.sub, other, self)

    def __pow__(self, other):
        return elemwise(operator.pow, self, other)

    def __rpow__(self, other):
        return elemwise(operator.pow, other, self)

    def __truediv__(self, other):
        return elemwise(operator.truediv, self, other)

    def __rtruediv__(self, other):
        return elemwise(operator.truediv, other, self)

    def __floordiv__(self, other):
        return elemwise(operator.floordiv, self, other)

    def __rfloordiv__(self, other):
        return elemwise(operator.floordiv, other, self)

    def __array_ufunc__(self, numpy_ufunc, method, *inputs, **kwargs):
        out = kwargs.get("out", ())
        for x in inputs + out:
            if _should_delegate(self, x):
                return NotImplemented

        if method == "__call__":
            if numpy_ufunc is np.matmul:
                return NotImplemented
            if numpy_ufunc.signature is not None:
                return NotImplemented
            if numpy_ufunc.nout > 1:
                return NotImplemented
            else:
                return elemwise(numpy_ufunc, *inputs, **kwargs)
        elif method == "outer":
            return NotImplemented
        else:
            return NotImplemented

    @cached_property
    def size(self):
        """Number of elements in array"""
        return reduce(operator.mul, self.shape, 1)

    def any(self, axis=None, keepdims=False, split_every=None, out=None):
        """Returns True if any of the elements evaluate to True.

        Refer to :func:`dask.array.any` for full documentation.

        See Also
        --------
        dask.array.any : equivalent function
        """
        from dask_expr.array.reductions import any

        return any(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def all(self, axis=None, keepdims=False, split_every=None, out=None):
        """Returns True if all elements evaluate to True.

        Refer to :func:`dask.array.all` for full documentation.

        See Also
        --------
        dask.array.all : equivalent function
        """
        from dask_expr.array.reductions import all

        return all(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def min(self, axis=None, keepdims=False, split_every=None, out=None):
        """Return the minimum along a given axis.

        Refer to :func:`dask.array.min` for full documentation.

        See Also
        --------
        dask.array.min : equivalent function
        """
        from dask_expr.array.reductions import min

        return min(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def max(self, axis=None, keepdims=False, split_every=None, out=None):
        """Return the maximum along a given axis.

        Refer to :func:`dask.array.max` for full documentation.

        See Also
        --------
        dask.array.max : equivalent function
        """
        from dask_expr.array.reductions import max

        return max(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def argmin(self, axis=None, *, keepdims=False, split_every=None, out=None):
        """Return indices of the minimum values along the given axis.

        Refer to :func:`dask.array.argmin` for full documentation.

        See Also
        --------
        dask.array.argmin : equivalent function
        """
        from dask_expr.array.reductions import argmin

        return argmin(
            self, axis=axis, keepdims=keepdims, split_every=split_every, out=out
        )

    def argmax(self, axis=None, *, keepdims=False, split_every=None, out=None):
        """Return indices of the maximum values along the given axis.

        Refer to :func:`dask.array.argmax` for full documentation.

        See Also
        --------
        dask.array.argmax : equivalent function
        """
        from dask_expr.array.reductions import argmax

        return argmax(
            self, axis=axis, keepdims=keepdims, split_every=split_every, out=out
        )

    def sum(self, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
        """
        Return the sum of the array elements over the given axis.

        Refer to :func:`dask.array.sum` for full documentation.

        See Also
        --------
        dask.array.sum : equivalent function
        """
        from dask_expr.array.reductions import sum

        return sum(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            split_every=split_every,
            out=out,
        )

    def mean(self, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
        """Returns the average of the array elements along given axis.

        Refer to :func:`dask.array.mean` for full documentation.

        See Also
        --------
        dask.array.mean : equivalent function
        """
        from dask_expr.array.reductions import mean

        return mean(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            split_every=split_every,
            out=out,
        )

    def std(
        self, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None
    ):
        """Returns the standard deviation of the array elements along given axis.

        Refer to :func:`dask.array.std` for full documentation.

        See Also
        --------
        dask.array.std : equivalent function
        """
        from dask_expr.array.reductions import std

        return std(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            ddof=ddof,
            split_every=split_every,
            out=out,
        )

    def var(
        self, axis=None, dtype=None, keepdims=False, ddof=0, split_every=None, out=None
    ):
        """Returns the variance of the array elements, along given axis.

        Refer to :func:`dask.array.var` for full documentation.

        See Also
        --------
        dask.array.var : equivalent function
        """
        from dask_expr.array.reductions import var

        return var(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            ddof=ddof,
            split_every=split_every,
            out=out,
        )

    def moment(
        self,
        order,
        axis=None,
        dtype=None,
        keepdims=False,
        ddof=0,
        split_every=None,
        out=None,
    ):
        """Calculate the nth centralized moment.

        Refer to :func:`dask.array.moment` for the full documentation.

        See Also
        --------
        dask.array.moment : equivalent function
        """
        from dask_expr.array.reductions import moment

        return moment(
            self,
            order,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            ddof=ddof,
            split_every=split_every,
            out=out,
        )

    def prod(self, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
        """Return the product of the array elements over the given axis

        Refer to :func:`dask.array.prod` for full documentation.

        See Also
        --------
        dask.array.prod : equivalent function
        """
        from dask_expr.array.reductions import prod

        return prod(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            split_every=split_every,
            out=out,
        )


class IO(Array):
    pass


class FromArray(IO):
    _parameters = [
        "array",
        "chunks",
        "lock",
        "getitem",
        "inline_array",
        "meta",
        "asarray",
        "fancy",
    ]
    _defaults = {
        "getitem": None,
        "inline_array": False,
        "meta": None,
        "asarray": None,
        "fancy": True,
    }

    @property
    def chunks(self):
        return normalize_chunks(
            self.operand("chunks"), self.array.shape, dtype=self.array.dtype
        )

    @functools.cached_property
    def _meta(self):
        if self.operand("meta") is not None:
            return meta_from_array(self.operand("meta"), dtype=self.array.dtype)
        return self.array[tuple(slice(0, 0) for _ in range(self.array.ndim))]

    @functools.cached_property
    def asarray_arg(self):
        if self.operand("asarray") is None:
            return not hasattr(self.array, "__array_function__")
        else:
            return self.operand("asarray")

    def _layer(self):
        lock = self.operand("lock")
        if lock is True:
            lock = SerializableLock()

        is_ndarray = type(self.array) in (np.ndarray, np.ma.core.MaskedArray)
        is_single_block = all(len(c) == 1 for c in self.chunks)
        # Always use the getter for h5py etc. Not using isinstance(x, np.ndarray)
        # because np.matrix is a subclass of np.ndarray.
        if is_ndarray and not is_single_block and not lock:
            # eagerly slice numpy arrays to prevent memory blowup
            # GH5367, GH5601
            slices = slices_from_chunks(self.chunks)
            keys = product([self._name], *(range(len(bds)) for bds in self.chunks))
            values = [self.array[slc] for slc in slices]
            dsk = dict(zip(keys, values))
        elif is_ndarray and is_single_block:
            # No slicing needed
            dsk = {(self._name,) + (0,) * self.array.ndim: self.array}
        else:
            getitem = self.operand("getitem")
            if getitem is None:
                if self.operand("fancy"):
                    getitem = getter
                else:
                    getitem = getter_nofancy

            dsk = graph_from_arraylike(
                self.array,
                chunks=self.chunks,
                shape=self.array.shape,
                name=self._name,
                lock=lock,
                getitem=getitem,
                asarray=self.asarray_arg,
                inline_array=self.inline_array,
                dtype=self.array.dtype,
            )
        return dict(dsk)  # this comes as a legacy HLG for now

    def __str__(self):
        return "FromArray(...)"


class FromGraph(Array):
    _parameters = ["layer", "_meta", "chunks", "keys", "name_prefix"]

    @property
    def _meta(self):
        return self.operand("_meta")

    @functools.cached_property
    def _name(self):
        return (
            self.operand("name_prefix") + "-" + _tokenize_deterministic(*self.operands)
        )

    def _layer(self):
        dsk = dict(self.operand("layer"))
        # The name may not actually match the layers name therefore rewrite this
        # using an alias
        for k in self.operand("keys"):
            if not isinstance(k, tuple):
                raise TypeError(f"Expected tuple, got {type(k)}")
            orig = dsk[k]
            if not istask(orig):
                del dsk[k]
                dsk[(self._name, *k[1:])] = orig
            else:
                dsk[(self._name, *k[1:])] = k
        return dsk


def from_array(
    x,
    chunks="auto",
    lock=False,
    asarray=None,
    fancy=True,
    getitem=None,
    meta=None,
    inline_array=False,
):
    if isinstance(x, (list, tuple, memoryview) + np.ScalarType):
        x = np.array(x)

    return FromArray(
        x,
        chunks,
        lock=lock,
        asarray=asarray,
        fancy=fancy,
        getitem=getitem,
        meta=meta,
        inline_array=inline_array,
    )


def asarray(
    a, allow_unknown_chunksizes=False, dtype=None, order=None, *, like=None, **kwargs
):
    """Convert the input to a dask array.

    Parameters
    ----------
    a : array-like
        Input data, in any form that can be converted to a dask array. This
        includes lists, lists of tuples, tuples, tuples of tuples, tuples of
        lists and ndarrays.
    allow_unknown_chunksizes: bool
        Allow unknown chunksizes, such as come from converting from dask
        dataframes.  Dask.array is unable to verify that chunks line up.  If
        data comes from differently aligned sources then this can cause
        unexpected results.
    dtype : data-type, optional
        By default, the data-type is inferred from the input data.
    order : {‘C’, ‘F’, ‘A’, ‘K’}, optional
        Memory layout. ‘A’ and ‘K’ depend on the order of input array a.
        ‘C’ row-major (C-style), ‘F’ column-major (Fortran-style) memory
        representation. ‘A’ (any) means ‘F’ if a is Fortran contiguous, ‘C’
        otherwise ‘K’ (keep) preserve input order. Defaults to ‘C’.
    like: array-like
        Reference object to allow the creation of Dask arrays with chunks
        that are not NumPy arrays. If an array-like passed in as ``like``
        supports the ``__array_function__`` protocol, the chunk type of the
        resulting array will be defined by it. In this case, it ensures the
        creation of a Dask array compatible with that passed in via this
        argument. If ``like`` is a Dask array, the chunk type of the
        resulting array will be defined by the chunk type of ``like``.
        Requires NumPy 1.20.0 or higher.

    Returns
    -------
    out : dask array
        Dask array interpretation of a.

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> x = np.arange(3)
    >>> da.asarray(x)
    dask.array<array, shape=(3,), dtype=int64, chunksize=(3,), chunktype=numpy.ndarray>

    >>> y = [[1, 2, 3], [4, 5, 6]]
    >>> da.asarray(y)
    dask.array<array, shape=(2, 3), dtype=int64, chunksize=(2, 3), chunktype=numpy.ndarray>
    """
    if like is None:
        if isinstance(a, Array):
            return a
        elif hasattr(a, "to_dask_array"):
            return a.to_dask_array()
        elif type(a).__module__.split(".")[0] == "xarray" and hasattr(a, "data"):
            return asarray(a.data)
        elif isinstance(a, (list, tuple)) and any(isinstance(i, Array) for i in a):
            # TODO
            raise NotImplementedError
        elif not isinstance(getattr(a, "shape", None), Iterable):
            a = np.asarray(a, dtype=dtype, order=order)
    else:
        like_meta = meta_from_array(like)
        if isinstance(a, Array):
            # TODO
            raise NotImplementedError
        else:
            a = np.asarray(a, like=like_meta, dtype=dtype, order=order)
    return from_array(a, getitem=getter_inline, **kwargs)


from dask_expr.array.blockwise import Transpose, elemwise
