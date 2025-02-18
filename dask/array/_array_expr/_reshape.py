from __future__ import annotations

import functools
import math
from functools import reduce
from itertools import product
from operator import mul

from dask._collections import new_collection
from dask._task_spec import Task, TaskRef
from dask.array._array_expr import ArrayExpr
from dask.array.core import Array
from dask.array.reshape import (
    _convert_to_shape,
    _reshape_blockwise,
    _sanity_checks,
    reshape_rechunk,
)
from dask.array.utils import meta_from_array
from dask.base import tokenize
from dask.core import flatten
from dask.utils import M

_not_implemented_message = """
Dask's reshape only supports operations that merge or split existing dimensions
evenly. For example:

>>> x = da.ones((6, 5, 4), chunks=(3, 2, 2))
>>> x.reshape((3, 2, 5, 4))  # supported, splits 6 into 3 & 2
>>> x.reshape((30, 4))       # supported, merges 6 & 5 into 30
>>> x.reshape((4, 5, 6))     # unsupported, existing dimensions split unevenly

To work around this you may call reshape in multiple passes, or (if your data
is small enough) call ``compute`` first and handle reshaping in ``numpy``
directly.
"""


def reshape(x, shape, merge_chunks=True, limit=None):
    """Reshape array to new shape

    Parameters
    ----------
    shape : int or tuple of ints
        The new shape should be compatible with the original shape. If
        an integer, then the result will be a 1-D array of that length.
        One shape dimension can be -1. In this case, the value is
        inferred from the length of the array and remaining dimensions.
    merge_chunks : bool, default True
        Whether to merge chunks using the logic in :meth:`dask.array.rechunk`
        when communication is necessary given the input array chunking and
        the output shape. With ``merge_chunks==False``, the input array will
        be rechunked to a chunksize of 1, which can create very many tasks.
    limit: int (optional)
        The maximum block size to target in bytes. If no limit is provided,
        it defaults to using the ``array.chunk-size`` Dask config value.

    Notes
    -----
    This is a parallelized version of the ``np.reshape`` function with the
    following limitations:

    1.  It assumes that the array is stored in `row-major order`_
    2.  It only allows for reshapings that collapse or merge dimensions like
        ``(1, 2, 3, 4) -> (1, 6, 4)`` or ``(64,) -> (4, 4, 4)``

    .. _`row-major order`: https://en.wikipedia.org/wiki/Row-_and_column-major_order

    When communication is necessary this algorithm depends on the logic within
    rechunk.  It endeavors to keep chunk sizes roughly the same when possible.

    See :ref:`array-chunks.reshaping` for a discussion the tradeoffs of
    ``merge_chunks``.

    See Also
    --------
    dask.array.rechunk
    numpy.reshape
    """
    # Sanitize inputs, look for -1 in shape
    from dask.array.slicing import sanitize_index

    shape = tuple(map(sanitize_index, shape))
    known_sizes = [s for s in shape if s != -1]
    if len(known_sizes) < len(shape):
        if len(shape) - len(known_sizes) > 1:
            raise ValueError("can only specify one unknown dimension")
        # Fastpath for x.reshape(-1) on 1D arrays, allows unknown shape in x
        # for this case only.
        if len(shape) == 1 and x.ndim == 1:
            return x
        missing_size = sanitize_index(x.size / reduce(mul, known_sizes, 1))
        shape = tuple(missing_size if s == -1 else s for s in shape)

    _sanity_checks(x, shape)

    if x.shape == shape:
        return x

    if x.npartitions == 1:
        return new_collection(TrivialReshape(x, shape))

    # Logic or how to rechunk
    din = len(x.shape)
    dout = len(shape)
    if not merge_chunks and din > dout:
        x = x.rechunk({i: 1 for i in range(din - dout)})

    chunking_structure = reshape_rechunk(x.shape, shape, x.chunks)
    x2 = x.rechunk(chunking_structure[0])

    return new_collection(Reshape(x2, shape, chunking_structure))


class ReshapeAbstract(ArrayExpr):
    _parameters = ["array", "shape"]

    @functools.cached_property
    def _meta(self):
        return meta_from_array(self.array, len(self.shape))


class Reshape(ReshapeAbstract):
    _parameters = ["array", "shape", "chunking_structure"]

    @functools.cached_property
    def _chunking_structure(self):
        return self.chunking_structure

    @functools.cached_property
    def chunks(self):
        return self._chunking_structure[1]

    def _layer(self) -> dict:
        in_keys = list(
            product(
                [self.array._name],
                *[range(len(c)) for c in self._chunking_structure[0]],
            )
        )
        out_keys = list(
            product([self._name], *[range(len(c)) for c in self._chunking_structure[1]])
        )
        shapes = list(product(*self._chunking_structure[1]))
        return {
            a: Task(a, M.reshape, TaskRef(b), shape)
            for a, b, shape in zip(out_keys, in_keys, shapes)
        }


class TrivialReshape(ReshapeAbstract):
    @functools.cached_property
    def chunks(self):
        return tuple((d,) for d in self.operand("shape"))

    def _layer(self) -> dict:
        shape = self.operand("shape")
        key = next(flatten(self.array.__dask_keys__()))
        new_key = (self._name,) + (0,) * len(shape)
        dsk = {new_key: Task(new_key, M.reshape, TaskRef(key), shape)}
        return dsk


def reshape_blockwise(
    x: Array,
    shape: int | tuple[int, ...],
    chunks: tuple[tuple[int, ...], ...] | None = None,
) -> Array:
    """Blockwise-reshape into a new shape.

    The regular reshape operation in Dask preserves C-ordering in the array
    which requires a rechunking for most reshaping operations, making the
    computation relatively expensive.

    Blockwise-reshape reshapes every block into the new shape and concatenates
    the results. This is a trivial blockwise computation but will return the
    result in a different order than NumPy. This is a good solution for
    subsequent operations that don't rely on the order.

    Parameters
    ----------
    x: Array
        The input array to reshape.
    shape : int or tuple of ints
        The new shape should be compatible with the original shape. If
        an integer, then the result will be a 1-D array of that length.
        One shape dimension can be -1. In this case, the value is
        inferred from the length of the array and remaining dimensions.
    chunks: tuple of ints, default None
        The chunk sizes for every chunk in the output array. Dask will expand
        the chunks per dimension into the cross product of chunks for every
        chunk in the array.

        An error is raised if chunks is given and the number of dimensions
        decreases.

        .. note::
            This information is required if the number of dimensions is increased.
            Dask cannot infer the output chunks in this case. The keyword is ignored
            if the number of dimensions is reduced.

    Notes
    -----
    This is a parallelized version of the ``np.reshape`` function with the
    following limitations:

    1.  It does not return elements in the same order as NumPy would
    2.  It only allows for reshapings that collapse like
        ``(1, 2, 3, 4) -> (1, 6, 4)``

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> x = da.from_array(np.arange(0, 27).reshape(3, 3, 3), chunks=(3, 2, (2, 1)))
    >>> result = reshape_blockwise(x, (3, 9))
    >>> result.chunks
    ((3,), (4, 2, 2, 1))

    The resulting chunks are calculated automatically to match the new shape.

    >>> result.compute()
    array([[ 0,  1,  3,  4,  2,  5,  6,  7,  8],
           [ 9, 10, 12, 13, 11, 14, 15, 16, 17],
           [18, 19, 21, 22, 20, 23, 24, 25, 26]])

    >>> result = reshape_blockwise(result, (3, 3, 3), chunks=x.chunks)
    >>> result.chunks
    ((3,), (2, 1), (2, 1))

    The resulting chunks are taken from the input. Chaining the reshape operation
    together like this reverts the previous reshaping operation that reduces the
    number of dimensions.

    >>> result.compute()
    array([[[ 0,  1,  2],
            [ 3,  4,  5],
            [ 6,  7,  8]],
    <BLANKLINE>
           [[ 9, 10, 11],
            [12, 13, 14],
            [15, 16, 17]],
    <BLANKLINE>
           [[18, 19, 20],
            [21, 22, 23],
            [24, 25, 26]]])
    """
    if shape in [-1, (-1,)]:
        shape = (reduce(mul, x.shape),)

    if not isinstance(shape, tuple):
        shape = (shape,)

    _sanity_checks(x, shape)

    if len(shape) == x.ndim and shape == x.shape:
        return x

    if len(shape) > x.ndim:
        if chunks is None:
            raise TypeError("Need to specify chunks if expanding dimensions.")
        out_shapes = list(product(*(c for c in chunks)))
        in_shapes = list(product(*(c for c in x.chunks)))
        non_matching_chunks = [
            (i, in_c, out_c)
            for i, (in_c, out_c) in enumerate(zip(in_shapes, out_shapes))
            if math.prod(in_c) != math.prod(out_c)
        ]
        if non_matching_chunks:
            raise ValueError(
                f"Chunk sizes do not match for the following chunks: "
                f"{[x[0] for x in non_matching_chunks[:5]]}. \n"
                f"The corresponding chunksizes are: {[x[1:] for x in non_matching_chunks[:5]]}. "
                f"(restricted to first 5 entries)."
            )

        return new_collection(BlockwiseReshapeBlockwiseEnlarge(x, shape, chunks))

    if chunks is not None:
        raise ValueError(
            "Setting chunks is not allowed when reducing the number of dimensions."
        )

    return new_collection(BlockwiseReshapeBlockwiseReduce(x, shape, None))


class ReshapeBlockwiseAbstract(ReshapeAbstract):
    _parameters = ["array", "shape", "chunks"]

    @functools.cached_property
    def chunks(self):
        return self.operand("chunks")

    @functools.cached_property
    def _name(self):
        return "reshape-blockwise-" + tokenize(
            self.array, self.operand("shape"), self.operand("chunks")
        )


class BlockwiseReshapeBlockwiseEnlarge(ReshapeBlockwiseAbstract):

    def _layer(self) -> dict:
        chunks = self.operand("chunks")
        out_shapes = list(product(*(c for c in chunks)))
        out_chunk_tuples = list(product(*(range(len(c)) for c in chunks)))
        chunk_tuples = list(
            product(*(range(len(c)) for i, c in enumerate(self.array.chunks)))
        )
        return {
            (self._name,)
            + tuple(chunk_out): Task(
                (self._name,) + tuple(chunk_out),
                _reshape_blockwise,
                TaskRef((self.array._name,) + tuple(chunk_in)),
                shape,
            )
            for chunk_in, chunk_out, shape in zip(
                chunk_tuples, out_chunk_tuples, out_shapes
            )
        }


class BlockwiseReshapeBlockwiseReduce(ReshapeBlockwiseAbstract):
    @functools.cached_property
    def chunks(self):
        # Calculating the output chunks is a bit tricky. We have all the output chunk
        # tuples, but now we have to create the source of the cross product. We can
        # iterate over the tuples to extract all elements along a single dimension.

        output_chunks = []
        ctr = 1
        for i, nr_chunks_dim in enumerate(reversed(self.nr_out_chunks)):
            dimension_chunks = []
            for elem in range(nr_chunks_dim):
                dimension_chunks.append(
                    self.out_shapes[elem * ctr][len(self.nr_out_chunks) - i - 1]
                )
            output_chunks.append(tuple(dimension_chunks))
            ctr *= nr_chunks_dim
        return tuple(reversed(output_chunks))

    @functools.cached_property
    def nr_out_chunks(self):
        return _convert_to_shape(
            tuple(map(len, self.array.chunks)),
            self.chunking_structure[2],
            self.chunking_structure[3],
        )

    @functools.cached_property
    def out_shapes(self):
        # Convert input chunks to output chunks
        return [
            _convert_to_shape(c, self.chunking_structure[2], self.chunking_structure[3])
            for c in list(product(*(c for c in self.array.chunks)))
        ]

    @functools.cached_property
    def chunking_structure(self):
        return reshape_rechunk(
            self.array.shape,
            self.operand("shape"),
            self.array.chunks,
            disallow_dimension_expansion=True,
        )

    def _layer(self) -> dict:
        # Create output chunk tuples for the graph
        chunk_tuples = list(
            product(*(range(len(c)) for i, c in enumerate(self.array.chunks)))
        )
        out_chunk_tuples = list(product(*(range(c) for c in self.nr_out_chunks)))

        dsk = {
            (self._name,)
            + tuple(chunk_out): Task(
                (self._name,) + tuple(chunk_out),
                _reshape_blockwise,
                TaskRef((self.array.name,) + tuple(chunk_in)),
                shape,
            )
            for chunk_in, chunk_out, shape in zip(
                chunk_tuples, out_chunk_tuples, self.out_shapes
            )
        }
        return dsk
