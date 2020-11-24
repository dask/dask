import numbers
import warnings

import tlz as toolz

from .. import base, utils
from ..delayed import unpack_collections
from ..highlevelgraph import HighLevelGraph
from ..blockwise import blockwise as core_blockwise


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
    **kwargs
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
        Sequence like (x, 'ij', y, 'jk', z, 'i')
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

    Examples
    --------
    2D embarrassingly parallel operation from two arrays, x, and y.

    >>> z = blockwise(operator.add, 'ij', x, 'ij', y, 'ij', dtype='f8')  # z = x + y  # doctest: +SKIP

    Outer product multiplying x by y, two 1-d vectors

    >>> z = blockwise(operator.mul, 'ij', x, 'i', y, 'j', dtype='f8')  # doctest: +SKIP

    z = x.T

    >>> z = blockwise(np.transpose, 'ji', x, 'ij', dtype=x.dtype)  # doctest: +SKIP

    The transpose case above is illustrative because it does same transposition
    both on each in-memory block by calling ``np.transpose`` and on the order
    of the blocks themselves, by switching the order of the index ``ij -> ji``.

    We can compose these same patterns with more variables and more complex
    in-memory functions

    z = X + Y.T

    >>> z = blockwise(lambda x, y: x + y.T, 'ij', x, 'ij', y, 'ji', dtype='f8')  # doctest: +SKIP

    Any index, like ``i`` missing from the output index is interpreted as a
    contraction (note that this differs from Einstein convention; repeated
    indices do not imply contraction.)  In the case of a contraction the passed
    function should expect an iterable of blocks on any array that holds that
    index.  To receive arrays concatenated along contracted dimensions instead
    pass ``concatenate=True``.

    Inner product multiplying x by y, two 1-d vectors

    >>> def sequence_dot(x_blocks, y_blocks):
    ...     result = 0
    ...     for x, y in zip(x_blocks, y_blocks):
    ...         result += x.dot(y)
    ...     return result

    >>> z = blockwise(sequence_dot, '', x, 'i', y, 'i', dtype='f8')  # doctest: +SKIP

    Add new single-chunk dimensions with the ``new_axes=`` keyword, including
    the length of the new dimension.  New dimensions will always be in a single
    chunk.

    >>> def f(x):
    ...     return x[:, None] * np.ones((1, 5))

    >>> z = blockwise(f, 'az', x, 'a', new_axes={'z': 5}, dtype=x.dtype)  # doctest: +SKIP

    New dimensions can also be multi-chunk by specifying a tuple of chunk
    sizes.  This has limited utility as is (because the chunks are all the
    same), but the resulting graph can be modified to achieve more useful
    results (see ``da.map_blocks``).

    >>> z = blockwise(f, 'az', x, 'a', new_axes={'z': (5, 5)}, dtype=x.dtype)  # doctest: +SKIP

    If the applied function changes the size of each chunk you can specify this
    with a ``adjust_chunks={...}`` dictionary holding a function for each index
    that modifies the dimension size in that index.

    >>> def double(x):
    ...     return np.concatenate([x, x])

    >>> y = blockwise(double, 'ij', x, 'ij',
    ...               adjust_chunks={'i': lambda n: 2 * n}, dtype=x.dtype)  # doctest: +SKIP

    Include literals by indexing with None

    >>> y = blockwise(add, 'ij', x, 'ij', 1234, None, dtype=x.dtype)  # doctest: +SKIP
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

    from .core import unify_chunks, normalize_arg

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
    for k, v in kwargs.items():
        v = normalize_arg(v)
        v, collections = unpack_collections(v)
        dependencies.extend(collections)
        kwargs2[k] = v

    # Finish up the name
    if not out:
        out = "%s-%s" % (
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
        **kwargs2
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
                            "Dimension {0} has {1} blocks, "
                            "adjust_chunks specified with "
                            "{2} blocks".format(
                                i, len(chunks[i]), len(adjust_chunks[ind])
                            )
                        )
                    chunks[i] = tuple(adjust_chunks[ind])
                else:
                    raise NotImplementedError(
                        "adjust_chunks values must be callable, int, or tuple"
                    )
    chunks = tuple(chunks)

    if meta is None:
        from .utils import compute_meta

        meta = compute_meta(func, dtype, *args[::2], **kwargs)
    return new_da_object(graph, out, chunks, meta=meta, dtype=dtype)


def atop(*args, **kwargs):
    warnings.warn("The da.atop function has moved to da.blockwise")
    return blockwise(*args, **kwargs)


from .core import new_da_object
