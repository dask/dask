from __future__ import annotations

import numbers
import warnings
from dataclasses import fields, is_dataclass, replace

import tlz as toolz

from dask import base, utils
from dask._task_spec import Dict, List, Task, TaskRef
from dask.blockwise import blockwise as core_blockwise
from dask.delayed import Delayed, finalize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import ArrayBlockwiseDep


def _unpack_collections(expr):
    # FIXME This is a copy of the delayed.unpack_collections function with the
    # addition of the TaskSpec class. Eventually this should all be consolidated
    # but to reduce the number of changes we'll vendor this here
    # FIXME: There is also a dask.base version of unpack_collections that looks
    # similar but is different. At the very least the names should be fixed
    if isinstance(expr, Delayed):
        return TaskRef(expr._key), (expr,)

    if base.is_dask_collection(expr):
        if hasattr(expr, "optimize"):
            # Optimize dask-expr collections
            expr = expr.optimize()

        finalized = finalize(expr)
        return finalized._key, (finalized,)

    if type(expr) is type(iter(list())):
        expr = list(expr)
    elif type(expr) is type(iter(tuple())):
        expr = tuple(expr)
    elif type(expr) is type(iter(set())):
        expr = set(expr)

    typ = type(expr)

    if typ in (list, tuple, set):
        args, collections = utils.unzip((_unpack_collections(e) for e in expr), 2)
        collections = tuple(toolz.unique(toolz.concat(collections), key=id))
        if not collections:
            return expr, ()
        args = List(*args)
        # Ensure output type matches input type
        if typ is not list:
            args = Task(None, typ, args)
        return args, collections

    if typ is dict:
        args, collections = _unpack_collections([[k, v] for k, v in expr.items()])
        if not collections:
            return expr, ()
        return Dict(args), collections

    if typ is slice:
        args, collections = _unpack_collections([expr.start, expr.stop, expr.step])
        if not collections:
            return expr, ()
        return Task(None, slice, *args), collections

    if is_dataclass(expr):
        args, collections = _unpack_collections(
            [
                [f.name, getattr(expr, f.name)]
                for f in fields(expr)
                if hasattr(expr, f.name)  # if init=False, field might not exist
            ]
        )
        if not collections:
            return expr, ()
        try:
            _fields = {
                f.name: getattr(expr, f.name)
                for f in fields(expr)
                if hasattr(expr, f.name)
            }
            replace(expr, **_fields)
        except (TypeError, ValueError) as e:
            if isinstance(e, ValueError) or "is declared with init=False" in str(e):
                raise ValueError(
                    f"Failed to unpack {typ} instance. "
                    "Note that using fields with `init=False` are not supported."
                ) from e
            else:
                raise TypeError(
                    f"Failed to unpack {typ} instance. "
                    "Note that using a custom __init__ is not supported."
                ) from e
        return Task(None, typ, **dict(args)), collections

    if utils.is_namedtuple_instance(expr):
        args, collections = _unpack_collections([v for v in expr])
        if not collections:
            return expr, ()
        return Task(None, typ, *args), collections

    return expr, ()


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
    out = name
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

    from dask.array.core import normalize_arg, unify_chunks

    if align_arrays:
        chunkss, arrays = unify_chunks(*args)
    else:
        arginds = [(a, i) for (a, i) in toolz.partition(2, args) if i is not None]
        chunkss = {}
        # For each dimension, use the input chunking that has the most blocks;
        # this will ensure that broadcasting works as expected, and in
        # particular the number of blocks should be correct if the inputs are
        # consistent.
        for arg, ind in arginds:
            for c, i in zip(arg.chunks, ind):
                if i not in chunkss or len(c) > len(chunkss[i]):
                    chunkss[i] = c
        arrays = args[::2]

    for k, v in new_axes.items():
        if not isinstance(v, tuple):
            v = (v,)
        chunkss[k] = v

    arginds = zip(arrays, args[1::2])
    numblocks = {}

    dependencies = []
    arrays = []

    # Normalize arguments
    argindsstr = []

    for arg, ind in arginds:
        if ind is None:
            arg = normalize_arg(arg)
            arg, collections = _unpack_collections(arg)

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
            if not isinstance(arg, ArrayBlockwiseDep):
                numblocks[arg.name] = arg.numblocks
                arrays.append(arg)
                arg = arg.name
        argindsstr.extend((arg, ind))

    # Normalize keyword arguments
    kwargs2 = {}
    for k, v in kwargs.items():
        v = normalize_arg(v)
        v, collections = _unpack_collections(v)
        dependencies.extend(collections)
        kwargs2[k] = v

    # Finish up the name
    if not out:
        out = "{}-{}".format(
            token or utils.funcname(func).strip("_"),
            base.tokenize(func, out_ind, argindsstr, dtype, **kwargs),
        )

    graph = core_blockwise(
        func,
        out,
        out_ind,
        *argindsstr,
        numblocks=numblocks,
        dependencies=dependencies,
        new_axes=new_axes,
        concatenate=concatenate,
        **kwargs2,
    )
    graph = HighLevelGraph.from_collections(
        out, graph, dependencies=arrays + dependencies
    )

    chunks = [chunkss[i] for i in out_ind]
    if adjust_chunks:
        for i, ind in enumerate(out_ind):
            if ind in adjust_chunks:
                if callable(adjust_chunks[ind]):
                    chunks[i] = tuple(map(adjust_chunks[ind], chunks[i]))
                elif isinstance(adjust_chunks[ind], numbers.Integral):
                    chunks[i] = tuple(adjust_chunks[ind] for _ in chunks[i])
                elif isinstance(adjust_chunks[ind], (tuple, list)):
                    if len(adjust_chunks[ind]) != len(chunks[i]):
                        raise ValueError(
                            f"Dimension {i} has {len(chunks[i])} blocks, adjust_chunks "
                            f"specified with {len(adjust_chunks[ind])} blocks"
                        )
                    chunks[i] = tuple(adjust_chunks[ind])
                else:
                    raise NotImplementedError(
                        "adjust_chunks values must be callable, int, or tuple"
                    )
    chunks = tuple(chunks)

    if meta is None:
        from dask.array.utils import compute_meta

        meta = compute_meta(func, dtype, *args[::2], **kwargs)
    return new_da_object(graph, out, chunks, meta=meta, dtype=dtype)


def atop(*args, **kwargs):
    warnings.warn("The da.atop function has moved to da.blockwise")
    return blockwise(*args, **kwargs)


from dask.array.core import new_da_object
