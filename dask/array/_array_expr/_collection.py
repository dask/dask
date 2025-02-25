from __future__ import annotations

import operator

import numpy as np
import toolz

from dask._collections import new_collection
from dask.array._array_expr._blockwise import Blockwise, Elemwise
from dask.array._array_expr._expr import ArrayExpr
from dask.array._array_expr._io import FromGraph
from dask.array.core import T_IntOrNaN, finalize
from dask.base import DaskMethodsMixin, named_schedulers
from dask.core import flatten
from dask.utils import key_split


class Array(DaskMethodsMixin):
    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)

    def __init__(self, expr):
        self._expr = expr

    @property
    def expr(self) -> ArrayExpr:
        return self._expr

    def __dask_postcompute__(self):
        return finalize, ()

    def __dask_postpersist__(self):
        state = self.expr.lower_completely()
        return from_graph, (
            state._meta,
            state.chunks,
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

    @property
    def ndim(self):
        return self.expr.ndim

    @property
    def numblocks(self):
        return self.expr.numblocks

    @property
    def size(self) -> T_IntOrNaN:
        return self.expr.size

    @property
    def name(self):
        return self.expr.name

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

    def __array_function__(self, func, types, args, kwargs):
        # TODO(expr-soon): Not done yet, but needed for assert_eq to identify us as an Array
        raise NotImplementedError

    def rechunk(
        self,
        chunks="auto",
        threshold=None,
        block_size_limit=None,
        balance=False,
        method=None,
    ):
        return rechunk(self, chunks, threshold, block_size_limit, balance, method)

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


def from_graph(layer, _meta, chunks, keys, name_prefix):
    return new_collection(
        FromGraph(
            layer=layer,
            _meta=_meta,
            chunks=chunks,
            keys=keys,
            name_prefix=name_prefix,
        )
    )


def blockwise(
    func,
    out_ind,
    *args,
    name=None,
    token=None,
    dtype=None,
    adjust_chunks=None,
    new_axes=None,
    align_arrays=True,
    concatenate=None,
    meta=None,
    **kwargs,
):
    """Tensor operation: Generalized inner and outer products

    A broad class of blocked algorithms and patterns can be specified with a
    concise multi-index notation.  The ``blockwise`` function applies an in-memory
    function across multiple blocks of multiple inputs in a variety of ways.
    Many dask.array operations are special cases of blockwise including
    elementwise, broadcasting, reductions, tensordot, and transpose.

    Parameters
    ----------
    func : callable
        Function to apply to individual tuples of blocks
    out_ind : iterable
        Block pattern of the output, something like 'ijk' or (1, 2, 3)
    *args : sequence of Array, index pairs
        You may also pass literal arguments, accompanied by None index
        e.g. (x, 'ij', y, 'jk', z, 'i', some_literal, None)
    **kwargs : dict
        Extra keyword arguments to pass to function
    dtype : np.dtype
        Datatype of resulting array.
    concatenate : bool, keyword only
        If true concatenate arrays along dummy indices, else provide lists
    adjust_chunks : dict
        Dictionary mapping index to function to be applied to chunk sizes
    new_axes : dict, keyword only
        New indexes and their dimension lengths
    align_arrays: bool
        Whether or not to align chunks along equally sized dimensions when
        multiple arrays are provided.  This allows for larger chunks in some
        arrays to be broken into smaller ones that match chunk sizes in other
        arrays such that they are compatible for block function mapping. If
        this is false, then an error will be thrown if arrays do not already
        have the same number of blocks in each dimension.

    Examples
    --------
    2D embarrassingly parallel operation from two arrays, x, and y.

    >>> import operator, numpy as np, dask.array as da
    >>> x = da.from_array([[1, 2],
    ...                    [3, 4]], chunks=(1, 2))
    >>> y = da.from_array([[10, 20],
    ...                    [0, 0]])
    >>> z = blockwise(operator.add, 'ij', x, 'ij', y, 'ij', dtype='f8')
    >>> z.compute()
    array([[11, 22],
           [ 3,  4]])

    Outer product multiplying a by b, two 1-d vectors

    >>> a = da.from_array([0, 1, 2], chunks=1)
    >>> b = da.from_array([10, 50, 100], chunks=1)
    >>> z = blockwise(np.outer, 'ij', a, 'i', b, 'j', dtype='f8')
    >>> z.compute()
    array([[  0,   0,   0],
           [ 10,  50, 100],
           [ 20, 100, 200]])

    z = x.T

    >>> z = blockwise(np.transpose, 'ji', x, 'ij', dtype=x.dtype)
    >>> z.compute()
    array([[1, 3],
           [2, 4]])

    The transpose case above is illustrative because it does transposition
    both on each in-memory block by calling ``np.transpose`` and on the order
    of the blocks themselves, by switching the order of the index ``ij -> ji``.

    We can compose these same patterns with more variables and more complex
    in-memory functions

    z = X + Y.T

    >>> z = blockwise(lambda x, y: x + y.T, 'ij', x, 'ij', y, 'ji', dtype='f8')
    >>> z.compute()
    array([[11,  2],
           [23,  4]])

    Any index, like ``i`` missing from the output index is interpreted as a
    contraction (note that this differs from Einstein convention; repeated
    indices do not imply contraction.)  In the case of a contraction the passed
    function should expect an iterable of blocks on any array that holds that
    index.  To receive arrays concatenated along contracted dimensions instead
    pass ``concatenate=True``.

    Inner product multiplying a by b, two 1-d vectors

    >>> def sequence_dot(a_blocks, b_blocks):
    ...     result = 0
    ...     for a, b in zip(a_blocks, b_blocks):
    ...         result += a.dot(b)
    ...     return result

    >>> z = blockwise(sequence_dot, '', a, 'i', b, 'i', dtype='f8')
    >>> z.compute()
    np.int64(250)

    Add new single-chunk dimensions with the ``new_axes=`` keyword, including
    the length of the new dimension.  New dimensions will always be in a single
    chunk.

    >>> def f(a):
    ...     return a[:, None] * np.ones((1, 5))

    >>> z = blockwise(f, 'az', a, 'a', new_axes={'z': 5}, dtype=a.dtype)

    New dimensions can also be multi-chunk by specifying a tuple of chunk
    sizes.  This has limited utility as is (because the chunks are all the
    same), but the resulting graph can be modified to achieve more useful
    results (see ``da.map_blocks``).

    >>> z = blockwise(f, 'az', a, 'a', new_axes={'z': (5, 5)}, dtype=x.dtype)
    >>> z.chunks
    ((1, 1, 1), (5, 5))

    If the applied function changes the size of each chunk you can specify this
    with a ``adjust_chunks={...}`` dictionary holding a function for each index
    that modifies the dimension size in that index.

    >>> def double(x):
    ...     return np.concatenate([x, x])

    >>> y = blockwise(double, 'ij', x, 'ij',
    ...               adjust_chunks={'i': lambda n: 2 * n}, dtype=x.dtype)
    >>> y.chunks
    ((2, 2), (2,))

    Include literals by indexing with None

    >>> z = blockwise(operator.add, 'ij', x, 'ij', 1234, None, dtype=x.dtype)
    >>> z.compute()
    array([[1235, 1236],
           [1237, 1238]])
    """
    new_axes = new_axes or {}

    # Input Validation
    if len(set(out_ind)) != len(out_ind):
        raise ValueError(
            "Repeated elements not allowed in output index",
            [k for k, v in toolz.frequencies(out_ind).items() if v > 1],
        )
    new = (
        set(out_ind)
        - {a for arg in args[1::2] if arg is not None for a in arg}
        - set(new_axes or ())
    )
    if new:
        raise ValueError("Unknown dimension", new)

    return new_collection(
        Blockwise(
            func,
            out_ind,
            name,
            token,
            dtype,
            adjust_chunks,
            new_axes,
            align_arrays,
            concatenate,
            meta,
            kwargs,
            *args,
        )
    )


def elemwise(op, *args, out=None, where=True, dtype=None, name=None, **kwargs):
    """Apply an elementwise ufunc-like function blockwise across arguments.

    Like numpy ufuncs, broadcasting rules are respected.

    Parameters
    ----------
    op : callable
        The function to apply. Should be numpy ufunc-like in the parameters
        that it accepts.
    *args : Any
        Arguments to pass to `op`. Non-dask array-like objects are first
        converted to dask arrays, then all arrays are broadcast together before
        applying the function blockwise across all arguments. Any scalar
        arguments are passed as-is following normal numpy ufunc behavior.
    out : dask array, optional
        If out is a dask.array then this overwrites the contents of that array
        with the result.
    where : array_like, optional
        An optional boolean mask marking locations where the ufunc should be
        applied. Can be a scalar, dask array, or any other array-like object.
        Mirrors the ``where`` argument to numpy ufuncs, see e.g. ``numpy.add``
        for more information.
    dtype : dtype, optional
        If provided, overrides the output array dtype.
    name : str, optional
        A unique key name to use when building the backing dask graph. If not
        provided, one will be automatically generated based on the input
        arguments.

    Examples
    --------
    >>> elemwise(add, x, y)  # doctest: +SKIP
    >>> elemwise(sin, x)  # doctest: +SKIP
    >>> elemwise(sin, x, out=dask_array)  # doctest: +SKIP

    See Also
    --------
    blockwise
    """
    if where is not True:
        # TODO(expr-soon): Need asarray for this
        where = True

    if out is not None:
        raise NotImplementedError("elemwise does not support out=")

    args = [np.asarray(a) if isinstance(a, (list, tuple)) else a for a in args]

    return new_collection(Elemwise(op, dtype, name, where, *args))


def rechunk(
    x,
    chunks="auto",
    threshold=None,
    block_size_limit=None,
    balance=False,
    method=None,
):
    """
    Convert blocks in dask array x for new chunks.

    Parameters
    ----------
    x: dask array
        Array to be rechunked.
    chunks:  int, tuple, dict or str, optional
        The new block dimensions to create. -1 indicates the full size of the
        corresponding dimension. Default is "auto" which automatically
        determines chunk sizes.
    threshold: int, optional
        The graph growth factor under which we don't bother introducing an
        intermediate step.
    block_size_limit: int, optional
        The maximum block size (in bytes) we want to produce
        Defaults to the configuration value ``array.chunk-size``
    balance : bool, default False
        If True, try to make each chunk to be the same size.

        This means ``balance=True`` will remove any small leftover chunks, so
        using ``x.rechunk(chunks=len(x) // N, balance=True)``
        will almost certainly result in ``N`` chunks.
    method: {'tasks', 'p2p'}, optional.
        Rechunking method to use.


    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones((1000, 1000), chunks=(100, 100))

    Specify uniform chunk sizes with a tuple

    >>> y = x.rechunk((1000, 10))

    Or chunk only specific dimensions with a dictionary

    >>> y = x.rechunk({0: 1000})

    Use the value ``-1`` to specify that you want a single chunk along a
    dimension or the value ``"auto"`` to specify that dask can freely rechunk a
    dimension to attain blocks of a uniform block size

    >>> y = x.rechunk({0: -1, 1: 'auto'}, block_size_limit=1e8)

    If a chunk size does not divide the dimension then rechunk will leave any
    unevenness to the last chunk.

    >>> x.rechunk(chunks=(400, -1)).chunks
    ((400, 400, 200), (1000,))

    However if you want more balanced chunks, and don't mind Dask choosing a
    different chunksize for you then you can use the ``balance=True`` option.

    >>> x.rechunk(chunks=(400, -1), balance=True).chunks
    ((500, 500), (1000,))
    """

    return new_collection(
        x.expr.rechunk(chunks, threshold, block_size_limit, balance, method)
    )
