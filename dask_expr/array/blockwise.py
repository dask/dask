import functools
import itertools
import math
import numbers
import warnings
from collections.abc import Iterable

import numpy as np
import toolz
from dask.array.core import (
    PerformanceWarning,
    _enforce_dtype,
    apply_infer_dtype,
    broadcast_dimensions,
    common_blockdim,
    normalize_arg,
)
from dask.array.utils import compute_meta
from dask.base import is_dask_collection, tokenize
from dask.blockwise import blockwise as core_blockwise
from dask.delayed import unpack_collections
from dask.utils import cached_property, funcname

from dask_expr.array.core import Array, asarray


class Blockwise(Array):
    _parameters = [
        "func",
        "out_ind",
        "name",
        "token",
        "dtype",
        "adjust_chunks",
        "new_axes",
        "align_arrays",
        "concatenate",
        "_meta_provided",
        "kwargs",
    ]
    _defaults = {
        "name": None,
        "token": None,
        "dtype": None,
        "adjust_chunks": None,
        "new_axes": None,
        "align_arrays": True,
        "concatenate": None,
        "_meta_provided": None,
        "kwargs": None,
    }

    @functools.cached_property
    def args(self):
        return self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta_provided(self):
        # We catch recursion errors if key starts with _meta, so define
        # explicitly here
        return self.operand("_meta_provided")

    @functools.cached_property
    def _meta(self):
        if self._meta_provided is not None:
            return self._meta_provided
        else:
            return compute_meta(self.func, self.dtype, *self.args[::2], **self.kwargs)

    @functools.cached_property
    def chunks(self):
        if self.align_arrays:
            chunkss, arrays, _ = unify_chunks(*self.args)
        else:
            arginds = [
                (a, i) for (a, i) in toolz.partition(2, self.args) if i is not None
            ]
            chunkss = {}
            # For each dimension, use the input chunking that has the most blocks;
            # this will ensure that broadcasting works as expected, and in
            # particular the number of blocks should be correct if the inputs are
            # consistent.
            for arg, ind in arginds:
                for c, i in zip(arg.chunks, ind):
                    if i not in chunkss or len(c) > len(chunkss[i]):
                        chunkss[i] = c

        for k, v in self.new_axes.items():
            if not isinstance(v, tuple):
                v = (v,)
            chunkss[k] = v

        chunks = [chunkss[i] for i in self.out_ind]
        if self.adjust_chunks:
            for i, ind in enumerate(self.out_ind):
                if ind in self.adjust_chunks:
                    if callable(self.adjust_chunks[ind]):
                        chunks[i] = tuple(map(self.adjust_chunks[ind], chunks[i]))
                    elif isinstance(self.adjust_chunks[ind], numbers.Integral):
                        chunks[i] = tuple(self.adjust_chunks[ind] for _ in chunks[i])
                    elif isinstance(self.adjust_chunks[ind], (tuple, list)):
                        if len(self.adjust_chunks[ind]) != len(chunks[i]):
                            raise ValueError(
                                f"Dimension {i} has {len(chunks[i])} blocks, adjust_chunks "
                                f"specified with {len(self.adjust_chunks[ind])} blocks"
                            )
                        chunks[i] = tuple(self.adjust_chunks[ind])
                    else:
                        raise NotImplementedError(
                            "adjust_chunks values must be callable, int, or tuple"
                        )
        chunks = tuple(chunks)
        return chunks

    @functools.cached_property
    def dtype(self):
        return self.operand("dtype")

    @functools.cached_property
    def _name(self):
        if "name" in self._parameters and self.operand("name"):
            return self.operand("name")
        else:
            return "{}-{}".format(
                self.token or funcname(self.func).strip("_"),
                tokenize(
                    self.func, self.out_ind, self.dtype, *self.args, **self.kwargs
                ),
            )

    def _layer(self):
        arginds = [(a, i) for (a, i) in toolz.partition(2, self.args)]

        numblocks = {}
        dependencies = []
        arrays = []

        # Normalize arguments
        argindsstr = []

        for arg, ind in arginds:
            if ind is None:
                arg = normalize_arg(arg)
                arg, collections = unpack_collections(arg)
                dependencies.extend(collections)
            else:
                if (
                    hasattr(arg, "ndim")
                    and hasattr(ind, "__len__")
                    and arg.ndim != len(ind)
                ):
                    raise ValueError(
                        "Index string %s does not match array dimension %d"
                        % (ind, arg.ndim)
                    )
                numblocks[arg.name] = arg.numblocks
                arrays.append(arg)
                arg = arg.name
            argindsstr.extend((arg, ind))

        # Normalize keyword arguments
        kwargs2 = {}
        for k, v in self.kwargs.items():
            v = normalize_arg(v)
            v, collections = unpack_collections(v)
            dependencies.extend(collections)
            kwargs2[k] = v

        graph = core_blockwise(
            self.func,
            self._name,
            self.out_ind,
            *argindsstr,
            numblocks=numblocks,
            dependencies=dependencies,
            new_axes=self.new_axes,
            concatenate=self.concatenate,
            **kwargs2,
        )
        return dict(graph)

    def _lower(self):
        if self.align_arrays:
            _, arrays, changed = unify_chunks(*self.args)
            if changed:
                return type(self)(*self.operands[: len(self._parameters)], *arrays)


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
    cls=Blockwise,
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
    250

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

    return cls(
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


class Elemwise(Blockwise):
    _parameters = ["op", "dtype", "name"]
    _defaults = {
        "dtype": None,
        "name": None,
    }
    align_arrays = True
    new_axes = {}
    adjust_chunks = None
    token = None
    _meta_provided = None
    concatenate = None

    @property
    def elemwise_args(self):
        return self.operands[len(self._parameters) :]

    @property
    def out_ind(self):
        shapes = []
        for arg in self.elemwise_args:
            shape = getattr(arg, "shape", ())
            if any(is_dask_collection(x) for x in shape):
                # Want to exclude Delayed shapes and dd.Scalar
                shape = ()
            shapes.append(shape)
        # if isinstance(where, Array):
        #     shapes.append(where.shape)
        # if isinstance(out, Array):
        #     shapes.append(out.shape)

        shapes = [s if isinstance(s, Iterable) else () for s in shapes]
        out_ndim = len(
            broadcast_shapes(*shapes)
        )  # Raises ValueError if dimensions mismatch
        return tuple(range(out_ndim))[::-1]

    @cached_property
    def _info(self):
        if self.operand("dtype") is not None:
            need_enforce_dtype = True
            dtype = self.operand("dtype")
        else:
            # We follow NumPy's rules for dtype promotion, which special cases
            # scalars and 0d ndarrays (which it considers equivalent) by using
            # their values to compute the result dtype:
            # https://github.com/numpy/numpy/issues/6240
            # We don't inspect the values of 0d dask arrays, because these could
            # hold potentially very expensive calculations. Instead, we treat
            # them just like other arrays, and if necessary cast the result of op
            # to match.
            vals = [
                np.empty((1,) * max(1, a.ndim), dtype=a.dtype)
                if not is_scalar_for_elemwise(a)
                else a
                for a in self.elemwise_args
            ]
            try:
                dtype = apply_infer_dtype(
                    self.op, vals, {}, "elemwise", suggest_dtype=False
                )
            except Exception:
                return NotImplemented
            need_enforce_dtype = any(
                not is_scalar_for_elemwise(a) and a.ndim == 0
                for a in self.elemwise_args
            )

        # TODO: add back
        # if where is not True:
        #     blockwise_kwargs["elemwise_where_function"] = op
        #     op = _elemwise_handle_where
        #     args.extend([where, out])

        if need_enforce_dtype:
            blockwise_kwargs = {
                "enforce_dtype": dtype,
                "enforce_dtype_function": self.op,
            }
            op = _enforce_dtype
        else:
            blockwise_kwargs = {}
            op = self.op

        return op, dtype, blockwise_kwargs

    @property
    def func(self):
        return self._info[0]

    @property
    def dtype(self):
        return self._info[1]

    @property
    def kwargs(self):
        return self._info[2]

    @property
    def token(self):
        return funcname(self.op).strip("_")

    @property
    def args(self):
        # for Blockwise rather than Elemwise
        return tuple(
            toolz.concat(
                (
                    a,
                    tuple(range(a.ndim)[::-1])
                    if not is_scalar_for_elemwise(a)
                    else None,
                )
                for a in self.elemwise_args
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
    if kwargs:
        raise TypeError(
            f"{op.__name__} does not take the following keyword arguments "
            f"{sorted(kwargs)}"
        )

    if out is not None:
        raise NotImplementedError()
    if where is not True:
        raise NotImplementedError()

    args = [np.asarray(a) if isinstance(a, (list, tuple)) else a for a in args]

    return Elemwise(op, dtype, name, *args)


def broadcast_shapes(*shapes):
    """
    Determines output shape from broadcasting arrays.

    Parameters
    ----------
    shapes : tuples
        The shapes of the arguments.

    Returns
    -------
    output_shape : tuple

    Raises
    ------
    ValueError
        If the input shapes cannot be successfully broadcast together.
    """
    if len(shapes) == 1:
        return shapes[0]
    out = []
    for sizes in itertools.zip_longest(*map(reversed, shapes), fillvalue=-1):
        if np.isnan(sizes).any():
            dim = np.nan
        else:
            dim = 0 if 0 in sizes else np.max(sizes)
        if any(i not in [-1, 0, 1, dim] and not np.isnan(i) for i in sizes):
            raise ValueError(
                "operands could not be broadcast together with "
                "shapes {}".format(" ".join(map(str, shapes)))
            )
        out.append(dim)
    return tuple(reversed(out))


def is_scalar_for_elemwise(arg):
    """

    >>> is_scalar_for_elemwise(42)
    True
    >>> is_scalar_for_elemwise('foo')
    True
    >>> is_scalar_for_elemwise(True)
    True
    >>> is_scalar_for_elemwise(np.array(42))
    True
    >>> is_scalar_for_elemwise([1, 2, 3])
    True
    >>> is_scalar_for_elemwise(np.array([1, 2, 3]))
    False
    >>> is_scalar_for_elemwise(from_array(np.array(0), chunks=()))
    False
    >>> is_scalar_for_elemwise(np.dtype('i4'))
    True
    """
    # the second half of shape_condition is essentially just to ensure that
    # dask series / frame are treated as scalars in elemwise.
    maybe_shape = getattr(arg, "shape", None)
    shape_condition = not isinstance(maybe_shape, Iterable) or any(
        is_dask_collection(x) for x in maybe_shape
    )

    return (
        np.isscalar(arg)
        or shape_condition
        or isinstance(arg, np.dtype)
        or (isinstance(arg, np.ndarray) and arg.ndim == 0)
    )


class Transpose(Blockwise):
    _parameters = ["array", "axes"]
    func = staticmethod(np.transpose)
    align_arrays = False
    adjust_chunks = None
    concatenate = None
    token = "transpose"

    @property
    def new_axes(self):
        return {}

    @property
    def name(self):
        return self._name

    @property
    def _meta_provided(self):
        return self.array._meta

    @property
    def dtype(self):
        return self._meta.dtype

    @property
    def out_ind(self):
        return self.axes

    @property
    def kwargs(self):
        return {"axes": self.axes}

    @property
    def args(self):
        return (self.array, tuple(range(self.array.ndim)))

    def _simplify_down(self):
        if isinstance(self.array, Transpose):
            axes = tuple(self.array.axes[i] for i in self.axes)
            return Transpose(self.array.array, axes)
        if self.axes == tuple(range(self.ndim)):
            return self.array


def unify_chunks(*args, **kwargs):
    """
    Unify chunks across a sequence of arrays

    This utility function is used within other common operations like
    :func:`dask.array.core.map_blocks` and :func:`dask.array.core.blockwise`.
    It is not commonly used by end-users directly.

    Parameters
    ----------
    *args: sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')

    Examples
    --------
    >>> import dask.array as da
    >>> x = da.ones(10, chunks=((5, 2, 3),))
    >>> y = da.ones(10, chunks=((2, 3, 5),))
    >>> chunkss, arrays = unify_chunks(x, 'i', y, 'i')
    >>> chunkss
    {'i': (2, 3, 2, 3)}

    >>> x = da.ones((100, 10), chunks=(20, 5))
    >>> y = da.ones((10, 100), chunks=(4, 50))
    >>> chunkss, arrays = unify_chunks(x, 'ij', y, 'jk', 'constant', None)
    >>> chunkss  # doctest: +SKIP
    {'k': (50, 50), 'i': (20, 20, 20, 20, 20), 'j': (4, 1, 3, 2)}

    >>> unify_chunks(0, None)
    ({}, [0])

    Returns
    -------
    chunkss : dict
        Map like {index: chunks}.
    arrays : list
        List of rechunked arrays.

    See Also
    --------
    common_blockdim
    """
    if not args:
        return {}, []

    arginds = [
        # TODO
        # (asanyarray(a) if ind is not None else a, ind) for a, ind in partition(2, args)
        (asarray(a) if ind is not None else a, ind)
        for a, ind in toolz.partition(2, args)
    ]  # [x, ij, y, jk]
    warn = kwargs.get("warn", True)

    arrays, inds = zip(*arginds)
    if all(ind is None for ind in inds):
        return {}, list(arrays), False
    if all(ind == inds[0] for ind in inds) and all(
        a.chunks == arrays[0].chunks for a in arrays
    ):
        return dict(zip(inds[0], arrays[0].chunks)), arrays, False

    nameinds = []
    blockdim_dict = dict()
    max_parts = 0
    for a, ind in arginds:
        if ind is not None:
            nameinds.append((a.name, ind))
            blockdim_dict[a.name] = a.chunks
            max_parts = max(max_parts, a.npartitions)
        else:
            nameinds.append((a, ind))

    chunkss = broadcast_dimensions(nameinds, blockdim_dict, consolidate=common_blockdim)
    nparts = math.prod(map(len, chunkss.values()))

    if warn and nparts and nparts >= max_parts * 10:
        warnings.warn(
            "Increasing number of chunks by factor of %d" % (nparts / max_parts),
            PerformanceWarning,
            stacklevel=3,
        )

    arrays = []
    changed = False
    for a, i in arginds:
        if i is None:
            arrays.append(a)
        else:
            chunks = tuple(
                chunkss[j]
                if a.shape[n] > 1
                else (a.shape[n],)
                if not np.isnan(sum(chunkss[j]))
                else None
                for n, j in enumerate(i)
            )
            if chunks != a.chunks and all(a.chunks):
                arrays.append(a.rechunk(chunks))
                changed = True
            else:
                arrays.append(a)
    return chunkss, arrays, changed
