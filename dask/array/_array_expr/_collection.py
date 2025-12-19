from __future__ import annotations

import math
import operator

import numpy as np
import toolz

from dask._collections import new_collection
from dask.array import chunk
from dask.array.chunk_types import is_valid_chunk_type
from dask.array._array_expr.manipulation._transpose import Transpose
from dask.array._array_expr._expr import ArrayExpr
from dask.base import DaskMethodsMixin, is_dask_collection, named_schedulers
from dask.core import flatten
from dask.utils import derived_from, has_keyword, key_split

# Import core conversion functions from their module
from dask.array._array_expr.core._conversion import (
    array,
    asanyarray,
    asarray,
    from_array,
)
from dask.array._array_expr.core._from_graph import from_graph

# Import stacking functions from their module
from dask.array._array_expr._concatenate import concatenate
from dask.array._array_expr._stack import stack

# Import blockwise functions from their module
from dask.array._array_expr.core._blockwise_funcs import blockwise, elemwise


# Type imports
from dask.array.core import (
    T_IntOrNaN,
    _HANDLED_FUNCTIONS,
    _should_delegate,
    check_if_handled_given_other,
    finalize,
)
from dask.array.chunk_types import is_valid_chunk_type


class Array(DaskMethodsMixin):
    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)
    __array_priority__ = 11  # higher than numpy.ndarray and numpy.matrix

    def __init__(self, expr):
        self._expr = expr

    @property
    def expr(self) -> ArrayExpr:
        return self._expr

    @property
    def _name(self):
        return self.expr._name

    def __dask_postcompute__(self):
        return finalize, ()

    def __dask_postpersist__(self):
        state = self.expr.lower_completely()
        # Use original array's meta like legacy implementation
        meta = self._meta
        if meta is None:
            # Fallback to synthetic meta if original is also None
            meta = np.empty((0,) * state.ndim, dtype=state.dtype)
        # Use self.chunks to preserve nan chunks for unknown-sized operations
        return from_graph, (
            meta,
            self.chunks,
            # FIXME: This is using keys of the unoptimized graph
            list(flatten(state.__dask_keys__())),
            key_split(state._name),
        )

    @property
    def dask(self):
        return self.__dask_graph__()

    def __dask_graph__(self):
        out = self.expr.lower_completely()
        return out.__dask_graph__()

    def __dask_keys__(self):
        out = self.expr.lower_completely()
        return out.__dask_keys__()

    def __dask_tokenize__(self):
        return "Array", self.expr._name

    def compute(self, **kwargs):
        return DaskMethodsMixin.compute(self.optimize(), **kwargs)

    def persist(self, **kwargs):
        return DaskMethodsMixin.persist(self.optimize(), **kwargs)

    def optimize(self):
        return new_collection(self.expr.optimize())

    def simplify(self):
        return new_collection(self.expr.simplify())

    @property
    def _meta(self):
        return self.expr._meta

    @property
    def dtype(self):
        return self.expr.dtype

    @property
    def shape(self):
        return self.expr.shape

    @property
    def chunks(self):
        return self.expr.chunks

    @chunks.setter
    def chunks(self, chunks):
        raise TypeError(
            "Can not set chunks directly\n\n"
            "Please use the rechunk method instead:\n"
            f"  x.rechunk({chunks})\n\n"
            "Documentation\n"
            "-------------\n"
            "https://docs.dask.org/en/latest/generated/dask.array.rechunk.html"
        )

    @property
    def chunksize(self) -> tuple:
        from dask.array.core import cached_max
        return tuple(cached_max(c) for c in self.chunks)

    @property
    def ndim(self):
        return self.expr.ndim

    @property
    def numblocks(self):
        return self.expr.numblocks

    @property
    def npartitions(self):
        from math import prod

        return prod(self.numblocks)

    @property
    def _key_array(self):
        return np.array(self.__dask_keys__(), dtype=object)

    @property
    def blocks(self):
        from dask.array.core import BlockView

        return BlockView(self)

    @property
    def size(self) -> T_IntOrNaN:
        return self.expr.size

    @property
    def nbytes(self) -> T_IntOrNaN:
        """Number of bytes in array"""
        return self.size * self.dtype.itemsize

    @property
    def itemsize(self) -> int:
        """Length of one array element in bytes"""
        return self.dtype.itemsize

    @property
    def name(self):
        return self.expr.name

    def __len__(self):
        return self.expr.__len__()

    def __repr__(self):
        name = self.name.rsplit("-", 1)[0]
        return (
            "dask.array<{}, shape={}, dtype={}, chunksize={}, chunktype={}.{}>".format(
                name,
                self.shape,
                self.dtype,
                self.chunksize,
                type(self._meta).__module__.split(".")[0],
                type(self._meta).__name__,
            )
        )

    def __bool__(self):
        if self.size > 1:
            raise ValueError(
                f"The truth value of a {self.__class__.__name__} is ambiguous. "
                "Use a.any() or a.all()."
            )
        return bool(self.compute())

    def _scalarfunc(self, cast_type):
        if self.size > 1:
            raise TypeError("Only length-1 arrays can be converted to Python scalars")
        else:
            return cast_type(self.compute().item())

    def __int__(self):
        return self._scalarfunc(int)

    def __float__(self):
        return self._scalarfunc(float)

    def __complex__(self):
        return self._scalarfunc(complex)

    def __index__(self):
        return self._scalarfunc(operator.index)

    def __array__(self, dtype=None, copy=None, **kwargs):
        import warnings

        if kwargs:
            warnings.warn(
                f"Extra keyword arguments {kwargs} are ignored and won't be "
                "accepted in the future",
                FutureWarning,
            )
        if copy is False:
            warnings.warn(
                "Can't acquire a memory view of a Dask array. "
                "This will raise in the future.",
                FutureWarning,
            )
        x = self.compute()
        return np.asarray(x, dtype=dtype)

    def __getitem__(self, index):
        # Field access, e.g. x['a'] or x[['a', 'b']]
        if isinstance(index, str) or (
            isinstance(index, list) and index and all(isinstance(i, str) for i in index)
        ):
            from dask.array.chunk import getitem

            if isinstance(index, str):
                dt = self.dtype[index]
            else:
                dt = np.dtype(
                    {
                        "names": index,
                        "formats": [self.dtype.fields[name][0] for name in index],
                        "offsets": [self.dtype.fields[name][1] for name in index],
                        "itemsize": self.dtype.itemsize,
                    }
                )

            if dt.shape:
                new_axis = list(range(self.ndim, self.ndim + len(dt.shape)))
                chunks = self.chunks + tuple((i,) for i in dt.shape)
                return self.map_blocks(
                    getitem, index, dtype=dt.base, chunks=chunks, new_axis=new_axis
                )
            else:
                return self.map_blocks(getitem, index, dtype=dt)

        if not isinstance(index, tuple):
            index = (index,)

        from dask.array._array_expr._slicing import (
            slice_array,
            slice_with_int_dask_array,
        )
        from dask.array.slicing import normalize_index

        index2 = normalize_index(index, self.shape)
        dependencies = {self.name}
        for i in index2:
            if isinstance(i, Array):
                dependencies.add(i.name)

        if any(isinstance(i, Array) and i.dtype.kind in "iu" for i in index2):
            self, index2 = slice_with_int_dask_array(self, index2)
        if any(isinstance(i, Array) and i.dtype == bool for i in index2):
            from dask.array._array_expr._slicing import slice_with_bool_dask_array

            self, index2 = slice_with_bool_dask_array(self, index2)

        if all(isinstance(i, slice) and i == slice(None) for i in index2):
            return self

        result = slice_array(self.expr, index2)
        return new_collection(result)

    def __setitem__(self, key, value):
        from dask.array.core import unknown_chunk_message

        # Handle np.ma.masked assignment
        if value is np.ma.masked:
            value = np.ma.masked_all((), dtype=self.dtype)

        # Check for NaN/inf in integer arrays
        if not is_dask_collection(value) and self.dtype.kind in "iu":
            if np.isnan(value).any():
                raise ValueError("cannot convert float NaN to integer")
            if np.isinf(value).any():
                raise ValueError("cannot convert float infinity to integer")

        # Suppress dtype broadcasting; __setitem__ can't change the dtype.
        value = asanyarray(value, dtype=self.dtype)

        # Handle 1D integer array index case
        if isinstance(key, Array) and (
            key.dtype.kind in "iu"
            or (key.dtype == bool and key.ndim == 1 and self.ndim > 1)
        ):
            key = (key,)

        # Use "where" method for boolean mask case
        if isinstance(key, Array) and key.dtype == bool:
            from dask.array._array_expr._broadcast import broadcast_to
            from dask.array._array_expr.routines import where

            left_shape = np.array(key.shape)
            right_shape = np.array(self.shape)

            # Treat unknown shapes as matching
            match = left_shape == right_shape
            match |= np.isnan(left_shape) | np.isnan(right_shape)

            if not match.all():
                raise IndexError(
                    f"boolean index shape {key.shape} must match indexed array's "
                    f"{self.shape}."
                )

            # If value has ndim > 0, they must be broadcastable to self.shape[idx].
            if value.ndim:
                value = broadcast_to(value, self[key].shape)

            from dask.array._array_expr.routines._where import where

            y = where(key, value, self)
            self._expr = y.expr
            return

        # Check for unknown chunks
        if np.isnan(self.shape).any():
            raise ValueError(f"Arrays chunk sizes are unknown. {unknown_chunk_message}")

        # Use SetItem expression for other index types
        from dask.array._array_expr._slicing import SetItem

        value_expr = value.expr if isinstance(value, Array) else value
        y = new_collection(SetItem(self.expr, key, value_expr))
        self._expr = y.expr

    def __add__(self, other):
        return elemwise(operator.add, self, other)

    def __radd__(self, other):
        return elemwise(operator.add, other, self)

    def __mul__(self, other):
        return elemwise(operator.mul, self, other)

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

    def __abs__(self):
        return elemwise(operator.abs, self)

    @check_if_handled_given_other
    def __and__(self, other):
        return elemwise(operator.and_, self, other)

    @check_if_handled_given_other
    def __rand__(self, other):
        return elemwise(operator.and_, other, self)

    @check_if_handled_given_other
    def __div__(self, other):
        return elemwise(operator.div, self, other)

    @check_if_handled_given_other
    def __rdiv__(self, other):
        return elemwise(operator.div, other, self)

    @check_if_handled_given_other
    def __eq__(self, other):
        return elemwise(operator.eq, self, other)

    @check_if_handled_given_other
    def __gt__(self, other):
        return elemwise(operator.gt, self, other)

    @check_if_handled_given_other
    def __ge__(self, other):
        return elemwise(operator.ge, self, other)

    def __invert__(self):
        return elemwise(operator.invert, self)

    @check_if_handled_given_other
    def __lshift__(self, other):
        return elemwise(operator.lshift, self, other)

    @check_if_handled_given_other
    def __rlshift__(self, other):
        return elemwise(operator.lshift, other, self)

    @check_if_handled_given_other
    def __lt__(self, other):
        return elemwise(operator.lt, self, other)

    @check_if_handled_given_other
    def __le__(self, other):
        return elemwise(operator.le, self, other)

    @check_if_handled_given_other
    def __mod__(self, other):
        return elemwise(operator.mod, self, other)

    @check_if_handled_given_other
    def __rmod__(self, other):
        return elemwise(operator.mod, other, self)

    @check_if_handled_given_other
    def __ne__(self, other):
        return elemwise(operator.ne, self, other)

    def __neg__(self):
        return elemwise(operator.neg, self)

    @check_if_handled_given_other
    def __or__(self, other):
        return elemwise(operator.or_, self, other)

    def __pos__(self):
        return self

    @check_if_handled_given_other
    def __ror__(self, other):
        return elemwise(operator.or_, other, self)

    @check_if_handled_given_other
    def __rshift__(self, other):
        return elemwise(operator.rshift, self, other)

    @check_if_handled_given_other
    def __rrshift__(self, other):
        return elemwise(operator.rshift, other, self)

    @check_if_handled_given_other
    def __xor__(self, other):
        return elemwise(operator.xor, self, other)

    @check_if_handled_given_other
    def __rxor__(self, other):
        return elemwise(operator.xor, other, self)

    @check_if_handled_given_other
    def __matmul__(self, other):
        from dask.array._array_expr._linalg import matmul

        return matmul(self, other)

    @check_if_handled_given_other
    def __rmatmul__(self, other):
        from dask.array._array_expr._linalg import matmul

        return matmul(other, self)

    @check_if_handled_given_other
    def __divmod__(self, other):
        from dask.array._array_expr._ufunc import divmod

        return divmod(self, other)

    @check_if_handled_given_other
    def __rdivmod__(self, other):
        from dask.array._array_expr._ufunc import divmod

        return divmod(other, self)

    def __array_function__(self, func, types, args, kwargs):
        import dask.array as module

        from dask.base import compute

        def handle_nonmatching_names(func, args, kwargs):
            if func not in _HANDLED_FUNCTIONS:
                warnings.warn(
                    f"The `{func.__module__}.{func.__name__}` function "
                    "is not implemented by Dask array. "
                    "You may want to use the da.map_blocks function "
                    "or something similar to silence this warning. "
                    "Your code may stop working in a future release.",
                    FutureWarning,
                )
                # Need to convert to array object (e.g. numpy.ndarray or
                # cupy.ndarray) as needed, so we can call the NumPy function
                # again and it gets the chance to dispatch to the right
                # implementation.
                args, kwargs = compute(args, kwargs)
                return func(*args, **kwargs)

            return _HANDLED_FUNCTIONS[func](*args, **kwargs)

        # First, verify that all types are handled by Dask. Otherwise, return NotImplemented.
        if not all(
            # Accept our own superclasses as recommended by NEP-13
            # (https://numpy.org/neps/nep-0013-ufunc-overrides.html#subclass-hierarchies)
            issubclass(type(self), type_) or is_valid_chunk_type(type_)
            for type_ in types
        ):
            return NotImplemented

        # Now try to find a matching function name.  If that doesn't work, we may
        # be dealing with an alias or a function that's simply not in the Dask API.
        # Handle aliases via the _HANDLED_FUNCTIONS dict mapping, and warn otherwise.
        for submodule in func.__module__.split(".")[1:]:
            try:
                module = getattr(module, submodule)
            except AttributeError:
                return handle_nonmatching_names(func, args, kwargs)

        if not hasattr(module, func.__name__):
            return handle_nonmatching_names(func, args, kwargs)

        da_func = getattr(module, func.__name__)
        if da_func is func:
            return handle_nonmatching_names(func, args, kwargs)

        # If ``like`` is contained in ``da_func``'s signature, add ``like=self``
        # to the kwargs dictionary.
        if has_keyword(da_func, "like"):
            kwargs["like"] = self

        return da_func(*args, **kwargs)

    def transpose(self, *axes):
        from collections.abc import Iterable

        if not axes:
            axes = None
        elif len(axes) == 1 and isinstance(axes[0], Iterable):
            axes = axes[0]

        if axes:
            if len(axes) != self.ndim:
                raise ValueError("axes don't match array")
            axes = tuple(d + self.ndim if d < 0 else d for d in axes)
        else:
            axes = tuple(range(self.ndim))[::-1]

        # Identity transpose - return self
        if axes == tuple(range(self.ndim)):
            return self

        return new_collection(Transpose(self, axes))

    @property
    def T(self):
        return self.transpose()

    @property
    def A(self):
        return self

    def swapaxes(self, axis1, axis2):
        """Interchange two axes of an array.

        Refer to :func:`dask.array.swapaxes` for full documentation.
        """
        return swapaxes(self, axis1, axis2)

    def squeeze(self, axis=None):
        """Remove axes of length one from array.

        Refer to :func:`dask.array.squeeze` for full documentation.
        """
        return squeeze(self, axis=axis)

    def reshape(self, *shape, merge_chunks=True, limit=None):
        """Reshape the array to a new shape.

        Refer to :func:`dask.array.reshape` for full documentation.
        """
        if len(shape) == 1 and isinstance(shape[0], (tuple, list)):
            shape = shape[0]
        return reshape(self, shape, merge_chunks=merge_chunks, limit=limit)

    def flatten(self):
        """Return a copy of the array collapsed into one dimension.

        Returns
        -------
        dask Array
            A 1-D array with the same data as self.
        """
        return reshape(self, (-1,))

    def ravel(self):
        """Return a flattened array.

        Returns
        -------
        dask Array
            A 1-D array with the same data as self.
        """
        return reshape(self, (-1,))

    flatten = ravel

    def repeat(self, repeats, axis=None):
        """Repeat elements of an array.

        Refer to :func:`dask.array.repeat` for full documentation.
        """
        from dask.array._array_expr._creation import repeat

        return repeat(self, repeats, axis)

    def choose(self, choices):
        """Use an index array to construct a new array from a set of choices.

        Refer to :func:`dask.array.choose` for full documentation.

        See Also
        --------
        dask.array.choose : equivalent function
        """
        from dask.array._array_expr._routines import choose

        return choose(self, choices)

    def nonzero(self):
        """Return the indices of the elements that are non-zero.

        Refer to :func:`dask.array.nonzero` for full documentation.

        See Also
        --------
        dask.array.nonzero : equivalent function
        """
        from dask.array._array_expr._routines import nonzero

        return nonzero(self)

    def round(self, decimals=0):
        """Return array with each element rounded to the given number of decimals.

        Refer to :func:`dask.array.round` for full documentation.

        See Also
        --------
        dask.array.round : equivalent function
        """
        from dask.array._array_expr._routines import round

        return round(self, decimals=decimals)

    def rechunk(
        self,
        chunks="auto",
        threshold=None,
        block_size_limit=None,
        balance=False,
        method=None,
    ):
        return rechunk(self, chunks, threshold, block_size_limit, balance, method)

    def _vindex(self, key):
        from dask.array._array_expr._slicing import _numpy_vindex, _vindex
        from dask.base import is_dask_collection

        if not isinstance(key, tuple):
            key = (key,)
        if any(k is None for k in key):
            raise IndexError(
                f"vindex does not support indexing with None (np.newaxis), got {key}"
            )
        if all(isinstance(k, slice) for k in key):
            if all(
                k.indices(d) == slice(0, d).indices(d) for k, d in zip(key, self.shape)
            ):
                return self
            raise IndexError(
                "vindex requires at least one non-slice to vectorize over "
                "when the slices are not over the entire array (i.e, x[:]). "
                f"Use normal slicing instead when only using slices. Got: {key}"
            )
        elif any(is_dask_collection(k) for k in key):
            if math.prod(self.numblocks) == 1 and len(key) == 1 and self.ndim == 1:
                idxr = key[0]
                # we can broadcast in this case
                return idxr.map_blocks(
                    _numpy_vindex, self, dtype=self.dtype, chunks=idxr.chunks
                )
            else:
                raise IndexError(
                    "vindex does not support indexing with dask objects. Call compute "
                    f"on the indexer first to get an evalurated array. Got: {key}"
                )
        return _vindex(self, *key)

    @property
    def vindex(self):
        """Vectorized indexing with broadcasting.

        This is equivalent to numpy's advanced indexing, using arrays that are
        broadcast against each other. This allows for pointwise indexing:

        >>> import dask.array as da
        >>> x = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        >>> x = da.from_array(x, chunks=2)
        >>> x.vindex[[0, 1, 2], [0, 1, 2]].compute()
        array([1, 5, 9])

        Mixed basic/advanced indexing with slices/arrays is also supported. The
        order of dimensions in the result follows those proposed for
        `ndarray.vindex <https://github.com/numpy/numpy/pull/6256>`_:
        the subspace spanned by arrays is followed by all slices.

        Note: ``vindex`` provides more general functionality than standard
        indexing, but it also has fewer optimizations and can be significantly
        slower.
        """
        from dask.utils import IndexCallable

        return IndexCallable(self._vindex)

    def store(self, target, **kwargs):
        """Store array in array-like object.

        Refer to :func:`dask.array.store` for full documentation.
        """
        from dask.array._array_expr._io import store

        return store([self], [target], **kwargs)

    def to_svg(self, size=500):
        """Convert chunks from Dask Array into an SVG Image

        Parameters
        ----------
        size : int
            Rough size of the image

        Returns
        -------
        str
            An svg string depicting the array as a grid of chunks
        """
        from dask.array.svg import svg

        return svg(self.chunks, size=size)

    def copy(self):
        """Copy array. This is a no-op for dask arrays, which are immutable."""
        return Array(self._expr)

    def __deepcopy__(self, memo):
        c = self.copy()
        memo[id(self)] = c
        return c

    def to_delayed(self, optimize_graph=True):
        """Convert into an array of :class:`dask.delayed.Delayed` objects, one per chunk.

        Parameters
        ----------
        optimize_graph : bool, optional
            If True [default], the graph is optimized before converting into
            :class:`dask.delayed.Delayed` objects.

        See Also
        --------
        dask.array.from_delayed
        """
        from dask.delayed import Delayed
        from dask.utils import ndeepmap

        keys = self.__dask_keys__()
        graph = self.__dask_graph__()
        if optimize_graph:
            graph = self.__dask_optimize__(graph, keys)
        L = ndeepmap(self.ndim, lambda k: Delayed(k, graph), keys)
        return np.array(L, dtype=object)

    def sum(self, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
        """
        Return the sum of the array elements over the given axis.

        Refer to :func:`dask.array.sum` for full documentation.

        See Also
        --------
        dask.array.sum : equivalent function
        """
        from dask.array.reductions import sum

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
        from dask.array.reductions import mean

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
        from dask.array.reductions import std

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
        from dask.array.reductions import var

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
        from dask.array.reductions import moment

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
        from dask.array.reductions import prod

        return prod(
            self,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
            split_every=split_every,
            out=out,
        )

    def any(self, axis=None, keepdims=False, split_every=None, out=None):
        """Returns True if any of the elements evaluate to True.

        Refer to :func:`dask.array.any` for full documentation.

        See Also
        --------
        dask.array.any : equivalent function
        """
        from dask.array.reductions import any

        return any(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def all(self, axis=None, keepdims=False, split_every=None, out=None):
        """Returns True if all elements evaluate to True.

        Refer to :func:`dask.array.all` for full documentation.

        See Also
        --------
        dask.array.all : equivalent function
        """
        from dask.array.reductions import all

        return all(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def min(self, axis=None, keepdims=False, split_every=None, out=None):
        """Return the minimum along a given axis.

        Refer to :func:`dask.array.min` for full documentation.

        See Also
        --------
        dask.array.min : equivalent function
        """
        from dask.array.reductions import min

        return min(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def max(self, axis=None, keepdims=False, split_every=None, out=None):
        """Return the maximum along a given axis.

        Refer to :func:`dask.array.max` for full documentation.

        See Also
        --------
        dask.array.max : equivalent function
        """
        from dask.array.reductions import max

        return max(self, axis=axis, keepdims=keepdims, split_every=split_every, out=out)

    def argmin(self, axis=None, *, keepdims=False, split_every=None, out=None):
        """Return indices of the minimum values along the given axis.

        Refer to :func:`dask.array.argmin` for full documentation.

        See Also
        --------
        dask.array.argmin : equivalent function
        """
        from dask.array.reductions import argmin

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
        from dask.array.reductions import argmax

        return argmax(
            self, axis=axis, keepdims=keepdims, split_every=split_every, out=out
        )

    def topk(self, k, axis=-1, split_every=None):
        """The top k elements of an array.

        Refer to :func:`dask.array.topk` for full documentation.

        See Also
        --------
        dask.array.topk : equivalent function
        """
        from dask.array._array_expr._routines import topk

        return topk(self, k, axis=axis, split_every=split_every)

    def argtopk(self, k, axis=-1, split_every=None):
        """The indices of the top k elements of an array.

        Refer to :func:`dask.array.argtopk` for full documentation.

        See Also
        --------
        dask.array.argtopk : equivalent function
        """
        from dask.array._array_expr._routines import argtopk

        return argtopk(self, k, axis=axis, split_every=split_every)

    def cumsum(self, axis, dtype=None, out=None, *, method="sequential"):
        """Return the cumulative sum of the elements along the given axis.

        Refer to :func:`dask.array.cumsum` for full documentation.

        See Also
        --------
        dask.array.cumsum : equivalent function
        """
        from dask.array.reductions import cumsum

        return cumsum(self, axis=axis, dtype=dtype, out=out, method=method)

    def cumprod(self, axis, dtype=None, out=None, *, method="sequential"):
        """Return the cumulative product of the elements along the given axis.

        Refer to :func:`dask.array.cumprod` for full documentation.

        See Also
        --------
        dask.array.cumprod : equivalent function
        """
        from dask.array.reductions import cumprod

        return cumprod(self, axis=axis, dtype=dtype, out=out, method=method)

    def trace(self, offset=0, axis1=0, axis2=1, dtype=None):
        """Return the sum along diagonals of the array.

        Refer to :func:`dask.array.trace` for full documentation.

        See Also
        --------
        dask.array.trace : equivalent function
        """
        from dask.array._array_expr._reductions import trace

        return trace(self, offset=offset, axis1=axis1, axis2=axis2, dtype=dtype)

    def dot(self, other):
        """Dot product of self and other.

        Refer to :func:`dask.array.tensordot` for full documentation.

        See Also
        --------
        dask.array.dot : equivalent function
        """
        from dask.array._array_expr._linalg import tensordot

        return tensordot(self, other, axes=((self.ndim - 1,), (other.ndim - 2,)))

    def astype(self, dtype, **kwargs):
        """Copy of the array, cast to a specified type.

        Parameters
        ----------
        dtype : str or dtype
            Typecode or data-type to which the array is cast.
        casting : {'no', 'equiv', 'safe', 'same_kind', 'unsafe'}, optional
            Controls what kind of data casting may occur. Defaults to 'unsafe'
            for backwards compatibility.

            * 'no' means the data types should not be cast at all.
            * 'equiv' means only byte-order changes are allowed.
            * 'safe' means only casts which can preserve values are allowed.
            * 'same_kind' means only safe casts or casts within a kind,
                like float64 to float32, are allowed.
            * 'unsafe' means any data conversions may be done.
        copy : bool, optional
            By default, astype always returns a newly allocated array. If this
            is set to False and the `dtype` requirement is satisfied, the input
            array is returned instead of a copy.

            .. note::

                Dask does not respect the contiguous memory layout of the array,
                and will ignore the ``order`` keyword argument.
                The default order is 'C' contiguous.
        """
        kwargs.pop("order", None)  # `order` is not respected, so we remove this kwarg
        # Scalars don't take `casting` or `copy` kwargs - as such we only pass
        # them to `map_blocks` if specified by user (different than defaults).
        extra = set(kwargs) - {"casting", "copy"}
        if extra:
            raise TypeError(
                f"astype does not take the following keyword arguments: {list(extra)}"
            )
        casting = kwargs.get("casting", "unsafe")
        dtype = np.dtype(dtype)
        if self.dtype == dtype:
            return self
        elif not np.can_cast(self.dtype, dtype, casting=casting):
            raise TypeError(
                f"Cannot cast array from {self.dtype!r} to {dtype!r} "
                f"according to the rule {casting!r}"
            )
        return elemwise(chunk.astype, self, dtype=dtype, astype_dtype=dtype, **kwargs)

    def map_blocks(self, func, *args, **kwargs):
        from dask.array._array_expr._map_blocks import map_blocks

        return map_blocks(func, self, *args, **kwargs)

    @property
    def _elemwise(self):
        return elemwise

    @property
    def real(self):
        from dask.array._array_expr._ufunc import real

        return real(self)

    @property
    def imag(self):
        from dask.array._array_expr._ufunc import imag

        return imag(self)

    def conj(self):
        """Complex-conjugate all elements.

        Refer to :func:`dask.array.conj` for full documentation.

        See Also
        --------
        dask.array.conj : equivalent function
        """
        from dask.array._array_expr._ufunc import conj

        return conj(self)

    def clip(self, min=None, max=None):
        """Return an array whose values are limited to ``[min, max]``.
        One of max or min must be given.

        Refer to :func:`dask.array.clip` for full documentation.

        See Also
        --------
        dask.array.clip : equivalent function
        """
        from dask.array._array_expr._ufunc import clip

        return clip(self, min, max)

    def view(self, dtype=None, order="C"):
        """Get a view of the array as a new data type

        Parameters
        ----------
        dtype:
            The dtype by which to view the array.
            The default, None, results in the view having the same data-type
            as the original array.
        order: string
            'C' or 'F' (Fortran) ordering

        This reinterprets the bytes of the array under a new dtype.  If that
        dtype does not have the same size as the original array then the shape
        will change.

        Beware that both numpy and dask.array can behave oddly when taking
        shape-changing views of arrays under Fortran ordering.  Under some
        versions of NumPy this function will fail when taking shape-changing
        views of Fortran ordered arrays if the first dimension has chunks of
        size one.
        """
        if dtype is None:
            dtype = self.dtype
        else:
            dtype = np.dtype(dtype)
        mult = self.dtype.itemsize / dtype.itemsize

        def _ensure_int(f):
            i = int(f)
            if i != f:
                raise ValueError(f"Could not coerce {f:f} to integer")
            return i

        if order == "C":
            chunks = self.chunks[:-1] + (
                tuple(_ensure_int(c * mult) for c in self.chunks[-1]),
            )
        elif order == "F":
            chunks = (
                tuple(_ensure_int(c * mult) for c in self.chunks[0]),
            ) + self.chunks[1:]
        else:
            raise ValueError("Order must be one of 'C' or 'F'")

        return self.map_blocks(
            chunk.view, dtype, order=order, dtype=dtype, chunks=chunks
        )

    def __array_ufunc__(self, numpy_ufunc, method, *inputs, **kwargs):
        out = kwargs.get("out", ())
        for x in inputs + out:
            if _should_delegate(self, x):
                return NotImplemented

        if method == "__call__":
            if numpy_ufunc is np.matmul:
                from dask.array._array_expr._linalg import matmul

                # special case until apply_gufunc handles optional dimensions
                return matmul(*inputs, **kwargs)
            if numpy_ufunc.signature is not None:
                from dask.array._array_expr._gufunc import apply_gufunc

                return apply_gufunc(
                    numpy_ufunc, numpy_ufunc.signature, *inputs, **kwargs
                )
            if numpy_ufunc.nout > 1:
                from dask.array._array_expr import _ufunc as ufunc

                try:
                    da_ufunc = getattr(ufunc, numpy_ufunc.__name__)
                except AttributeError:
                    return NotImplemented
                return da_ufunc(*inputs, **kwargs)
            else:
                return elemwise(numpy_ufunc, *inputs, **kwargs)
        elif method == "outer":
            from dask.array._array_expr import _ufunc as ufunc

            try:
                da_ufunc = getattr(ufunc, numpy_ufunc.__name__)
            except AttributeError:
                return NotImplemented
            return da_ufunc.outer(*inputs, **kwargs)
        else:
            return NotImplemented

    def map_overlap(self, func, depth, boundary=None, trim=True, **kwargs):
        """Map a function over blocks of the array with some overlap

        Refer to :func:`dask.array.map_overlap` for full documentation.

        See Also
        --------
        dask.array.map_overlap : equivalent function
        """
        from dask.array._array_expr._overlap import map_overlap

        return map_overlap(
            func, self, depth=depth, boundary=boundary, trim=trim, **kwargs
        )


# Import rechunk and ravel from their modules (they return Arrays directly)
from dask.array._array_expr._rechunk import rechunk
from dask.array._array_expr._reshape import ravel, reshape

# Import squeeze from its module (it returns an Array directly)
from dask.array._array_expr._slicing import squeeze


# Import expand functions from their module
from dask.array._array_expr.manipulation._expand import (
    atleast_1d,
    atleast_2d,
    atleast_3d,
)


# Import stacking functions from their module
from dask.array._array_expr.stacking._simple import vstack, hstack, dstack
from dask.array._array_expr.stacking._block import block


# Import manipulation functions from their module
from dask.array._array_expr.manipulation._flip import flip, flipud, fliplr, rot90
from dask.array._array_expr.manipulation._transpose import (
    swapaxes,
    moveaxis,
    rollaxis,
    transpose,
)
from dask.array._array_expr.manipulation._roll import roll

# Import broadcast_to directly (it returns an Array)
from dask.array._array_expr._broadcast import broadcast_to

# Import expand_dims from manipulation module
from dask.array._array_expr.manipulation._expand import expand_dims
