"""
Utilities for rechunking arrays through p2p shuffles
====================================================

Tensors (or n-D arrays) in dask are split up across the workers as
regular n-D "chunks" or bricks. These bricks are stacked up to form
the global array.

A key algorithm for these tensors is to "rechunk" them. That is to
reassemble the same global representation using differently shaped n-D
bricks.

For example, to take an FFT of an n-D array, one uses a sequence of 1D
FFTs along each axis. The implementation in dask (and indeed almost
all distributed array frameworks) requires that 1D
axis along which the FFT is taken is local to a single brick. So to
perform the global FFT we need to arrange that each axis in turn is
local to bricks.

This can be achieved through all-to-all communication between the
workers to exchange sub-pieces of their individual bricks, given a
"rechunking" scheme.

To perform the redistribution, each input brick is cut up into some
number of smaller pieces, each of which contributes to one of the
output bricks. The mapping from input brick to output bricks
decomposes into the Cartesian product of axis-by-axis mappings. To
see this, consider first a 1D example.

Suppose our array is split up into three equally sized bricks::

    |----0----|----1----|----2----|

And the requested output chunks are::

    |--A--|--B--|----C----|---D---|

So brick 0 contributes to output bricks A and B; brick 1 contributes
to B and C; and brick 2 contributes to C and D.

Now consider a 2D example of the same problem::

    +----0----+----1----+----2----+
    |         |         |         |
    α         |         |         |
    |         |         |         |
    +---------+---------+---------+
    |         |         |         |
    β         |         |         |
    |         |         |         |
    +---------+---------+---------+
    |         |         |         |
    γ         |         |         |
    |         |         |         |
    +---------+---------+---------+

Each brick can be described as the ordered pair of row and column
1D bricks, (0, α), (0, β), ..., (2, γ). Since the rechunking does
not also reshape the array, axes do not "interfere" with one another
when determining output bricks::

    +--A--+--B--+----C----+---D---+
    |     |     |         |       |
    Σ     |     |         |       |
    |     |     |         |       |
    +-----+ ----+---------+-------+
    |     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    Π     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    +-----+-----+---------+-------+

Consider the output (B, Σ) brick. This is contributed to by the
input (0, α) and (1, α) bricks. Determination of the subslices is
just done by slicing the the axes separately and combining them.

The key thing to note here is that we never need to create, and
store, the dense 2D mapping, we can instead construct it on the fly
for each output brick in turn as necessary.

The implementation here uses :func:`split_axes` to construct these
1D rechunkings. The output partitioning in
:meth:`~.ArrayRechunkRun.add_partition` then lazily constructs the
subsection of the Cartesian product it needs to determine the slices
of the current input brick.

This approach relies on the generic p2p buffering machinery to
ensure that there are not too many small messages exchanged, since
no special effort is made to minimise messages between workers when
a worker might have two adjacent input bricks that are sliced into
the same output brick.
"""

from __future__ import annotations

import math
import mmap
import os
from collections import defaultdict
from collections.abc import (
    Callable,
    Collection,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import chain, product
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import toolz
from tornado.ioloop import IOLoop

import dask
import dask.config
from dask._task_spec import Task, TaskRef, parse_input
from dask.highlevelgraph import HighLevelGraph
from dask.layers import Layer
from dask.tokenize import tokenize
from dask.typing import Key
from dask.utils import parse_bytes

from distributed.core import PooledRPCCall
from distributed.metrics import context_meter
from distributed.shuffle._core import (
    NDIndex,
    P2PBarrierTask,
    ShuffleId,
    ShuffleRun,
    ShuffleSpec,
    barrier_key,
    get_worker_plugin,
    handle_transfer_errors,
    handle_unpack_errors,
    p2p_barrier,
)
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._pickle import unpickle_bytestream
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin
from distributed.sizeof import sizeof

if TYPE_CHECKING:
    import numpy as np
    from typing_extensions import TypeAlias

    import dask.array as da

_T_LowLevelGraph: TypeAlias = dict[Key, tuple]
ChunkedAxis: TypeAlias = tuple[float, ...]  # chunks must either be an int or NaN
ChunkedAxes: TypeAlias = tuple[ChunkedAxis, ...]
NDSlice: TypeAlias = tuple[slice, ...]
SlicedAxis: TypeAlias = tuple[slice, ...]
SlicedAxes: TypeAlias = tuple[SlicedAxis, ...]


def rechunk_transfer(
    input: np.ndarray,
    id: ShuffleId,
    input_chunk: NDIndex,
) -> int:
    with handle_transfer_errors(id):
        return get_worker_plugin().add_partition(
            input,
            partition_id=input_chunk,
            id=id,
        )


def rechunk_unpack(
    id: ShuffleId, output_chunk: NDIndex, barrier_run_id: int
) -> np.ndarray:
    with handle_unpack_errors(id):
        return get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_chunk
        )


class _Partial(NamedTuple):
    """Information used to perform a partial rechunk along a single axis"""

    #: Slice of the old chunks along this axis that belong to the partial
    old: slice
    #: Slice of the new chunks along this axis that belong to the partial
    new: slice


class _NDPartial(NamedTuple):
    """Information used to perform a partial rechunk along all axes"""

    #: n-dimensional slice of the old chunks along each axis that belong to the partial
    old: NDSlice
    #: n-dimensional slice of the new chunks along each axis that belong to the partial
    new: NDSlice
    ix: NDIndex


def rechunk_name(token: str) -> str:
    return f"rechunk-p2p-{token}"


def rechunk_p2p(
    x: da.Array,
    chunks: ChunkedAxes,
    *,
    threshold: int | None = None,
    block_size_limit: int | None = None,
    balance: bool = False,
) -> da.Array:
    import dask.array as da

    if x.size == 0:
        # Special case for empty array, as the algorithm below does not behave correctly
        return da.empty(x.shape, chunks=chunks, dtype=x.dtype)
    from dask.array.core import new_da_object

    prechunked = _calculate_prechunking(x.chunks, chunks, x.dtype, block_size_limit)
    if prechunked != x.chunks:
        x = cast(
            "da.Array",
            x.rechunk(
                chunks=prechunked,
                threshold=threshold,
                block_size_limit=block_size_limit,
                balance=balance,
                method="tasks",
            ),
        )

    token = tokenize(x, chunks)
    name = rechunk_name(token)
    disk: bool = dask.config.get("distributed.p2p.storage.disk")

    layer = P2PRechunkLayer(
        name=name,
        token=token,
        chunks=chunks,
        chunks_input=x.chunks,
        name_input=x.name,
        disk=disk,
    )
    return new_da_object(
        HighLevelGraph.from_collections(name, layer, [x]),
        name,
        chunks,
        meta=x._meta,
        dtype=x.dtype,
    )


class P2PRechunkLayer(Layer):
    name: str
    token: str
    chunks: ChunkedAxes
    chunks_input: ChunkedAxes
    name_input: str
    disk: bool
    keepmap: np.ndarray

    _cached_dict: _T_LowLevelGraph | None

    def __init__(
        self,
        name: str,
        token: str,
        chunks: ChunkedAxes,
        chunks_input: ChunkedAxes,
        name_input: str,
        disk: bool,
        keepmap: np.ndarray | None = None,
        annotations: Mapping[str, Any] | None = None,
    ):
        import numpy as np

        self.name = name
        self.token = token
        self.chunks = chunks
        self.chunks_input = chunks_input
        self.name_input = name_input
        self.disk = disk
        if keepmap is not None:
            self.keepmap = keepmap
        else:
            shape = tuple(len(axis) for axis in chunks)
            self.keepmap = np.ones(shape, dtype=bool)
        self._cached_dict = None
        super().__init__(annotations=annotations)

    def __repr__(self) -> str:
        return f"{type(self).__name__}<name='{self.name}', chunks={self.chunks}>"

    def get_output_keys(self) -> set[Key]:
        import numpy as np

        return {
            (self.name,) + nindex
            for nindex in np.ndindex(tuple(len(axis) for axis in self.chunks))
            if self.keepmap[nindex]
        }

    def is_materialized(self) -> bool:
        return self._cached_dict is not None

    @property
    def _dict(self) -> _T_LowLevelGraph:
        """Materialize full dict representation"""
        dsk: _T_LowLevelGraph
        if self._cached_dict is not None:
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key: Key) -> tuple:
        return self._dict[key]

    def __iter__(self) -> Iterator[Key]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def _cull(self, keepmap: np.ndarray) -> P2PRechunkLayer:
        return P2PRechunkLayer(
            name=self.name,
            token=self.token,
            chunks=self.chunks,
            chunks_input=self.chunks_input,
            name_input=self.name_input,
            disk=self.disk,
            keepmap=keepmap,
            annotations=self.annotations,
        )

    def _keys_to_indices(self, keys: Iterable[Key]) -> set[tuple[int, ...]]:
        """Simple utility to convert keys to chunk indices."""
        chunks = set()
        for key in keys:
            if not isinstance(key, tuple) or len(key) < 2 or key[0] != self.name:
                continue
            chunk = cast(tuple[int, ...], key[1:])
            assert all(isinstance(index, int) for index in chunk)
            chunks.add(chunk)
        return chunks

    def cull(
        self, keys: set[Key], all_keys: Collection[Key]
    ) -> tuple[P2PRechunkLayer, dict]:
        """Cull a P2PRechunkLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indices) included in `keepmap`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        import numpy as np

        from dask.array.rechunk import old_to_new

        keepmap = np.zeros_like(self.keepmap, dtype=bool)
        indices_to_keep = self._keys_to_indices(keys)
        _old_to_new = old_to_new(self.chunks_input, self.chunks)

        for ndindex in indices_to_keep:
            keepmap[ndindex] = True

        culled_deps = {}
        # Identify the individual partial rechunks
        for ndpartial in _split_partials(_old_to_new):
            # Cull partials for which we do not keep any output tasks
            if not np.any(keepmap[ndpartial.new]):
                continue

            # Within partials, we have all-to-all communication.
            # Thus, all output tasks share the same input tasks.
            deps = frozenset(
                (self.name_input,) + ndindex
                for ndindex in _ndindices_of_slice(ndpartial.old)
            )

            for ndindex in _ndindices_of_slice(ndpartial.new):
                culled_deps[(self.name,) + ndindex] = deps

        if np.array_equal(keepmap, self.keepmap):
            return self, culled_deps
        else:
            culled_layer = self._cull(keepmap)
            return culled_layer, culled_deps

    def _construct_graph(self) -> _T_LowLevelGraph:
        import numpy as np

        from dask.array.rechunk import old_to_new

        dsk: _T_LowLevelGraph = {}

        _old_to_new = old_to_new(self.chunks_input, self.chunks)

        for ndpartial in _split_partials(_old_to_new):
            partial_keepmap = self.keepmap[ndpartial.new]
            output_count = np.sum(partial_keepmap)
            if output_count == 0:
                continue
            elif output_count == 1:
                # Single output chunk
                dsk.update(
                    partial_concatenate(
                        input_name=self.name_input,
                        input_chunks=self.chunks_input,
                        ndpartial=ndpartial,
                        token=self.token,
                        keepmap=self.keepmap,
                        old_to_new=_old_to_new,
                    )
                )
            else:
                dsk.update(
                    partial_rechunk(
                        input_name=self.name_input,
                        input_chunks=self.chunks_input,
                        chunks=self.chunks,
                        ndpartial=ndpartial,
                        token=self.token,
                        disk=self.disk,
                        keepmap=self.keepmap,
                    )
                )
        return dsk


def _calculate_prechunking(
    old_chunks: ChunkedAxes,
    new_chunks: ChunkedAxes,
    dtype: np.dtype,
    block_size_limit: int | None,
) -> ChunkedAxes:
    """Calculate how to perform the pre-rechunking step

    During the pre-rechunking step, we
      1. Split input chunks along partial boundaries to make partials completely independent of one another
      2. Merge small chunks within partials to reduce the number of transfer tasks and corresponding overhead
    """
    split_axes = _split_chunks_along_partial_boundaries(old_chunks, new_chunks)

    # We can only determine how to concatenate chunks if we can calculate block sizes.
    has_nans = (any(math.isnan(y) for y in x) for x in old_chunks)

    if len(new_chunks) <= 1 or not all(new_chunks) or any(has_nans):
        return tuple(tuple(chain(*axis)) for axis in split_axes)

    if dtype is None or dtype.hasobject or dtype.itemsize == 0:
        return tuple(tuple(chain(*axis)) for axis in split_axes)

    # We made sure that there are no NaNs in split_axes above
    return _concatenate_small_chunks(
        split_axes, old_chunks, new_chunks, dtype, block_size_limit  # type: ignore[arg-type]
    )


def _concatenate_small_chunks(
    split_axes: list[list[list[int]]],
    old_chunks: ChunkedAxes,
    new_chunks: ChunkedAxes,
    dtype: np.dtype,
    block_size_limit: int | None,
) -> ChunkedAxes:
    """Concatenate small chunks within partials.

    By concatenating chunks within partials, we reduce the number of P2P transfer tasks and their
    corresponding overhead.

    The algorithm used in this function is very similar to :func:`dask.array.rechunk.find_merge_rechunk`,
    the main difference is that we have to make sure only to merge chunks within partials.
    """
    import numpy as np

    block_size_limit = block_size_limit or dask.config.get("array.chunk-size")

    if isinstance(block_size_limit, str):
        block_size_limit = parse_bytes(block_size_limit)

    # Make it a number of elements
    block_size_limit //= dtype.itemsize

    # We verified earlier that we do not have any NaNs
    largest_old_block = _largest_block_size(old_chunks)  # type: ignore[arg-type]
    largest_new_block = _largest_block_size(new_chunks)  # type: ignore[arg-type]
    block_size_limit = max([block_size_limit, largest_old_block, largest_new_block])

    old_largest_width = [max(chain(*axis)) for axis in split_axes]
    new_largest_width = [max(c) for c in new_chunks]

    # This represents how much each dimension increases (>1) or reduces (<1)
    # the graph size during rechunking
    graph_size_effect = {
        dim: len(new_axis) / sum(map(len, split_axis))
        for dim, (split_axis, new_axis) in enumerate(zip(split_axes, new_chunks))
    }

    ndim = len(old_chunks)

    # This represents how much each dimension increases (>1) or reduces (<1) the
    # largest block size during rechunking
    block_size_effect = {
        dim: new_largest_width[dim] / (old_largest_width[dim] or 1)
        for dim in range(ndim)
    }

    # Our goal is to reduce the number of nodes in the rechunk graph
    # by concatenating some adjacent chunks, so consider dimensions where we can
    # reduce the # of chunks
    candidates = [dim for dim in range(ndim) if graph_size_effect[dim] <= 1.0]

    # Concatenating along each dimension reduces the graph size by a certain factor
    # and increases memory largest block size by a certain factor.
    # We want to optimize the graph size while staying below the given
    # block_size_limit.  This is in effect a knapsack problem, except with
    # multiplicative values and weights.  Just use a greedy algorithm
    # by trying dimensions in decreasing value / weight order.
    def key(k: int) -> float:
        gse = graph_size_effect[k]
        bse = block_size_effect[k]
        if bse == 1:
            bse = 1 + 1e-9
        return (np.log(gse) / np.log(bse)) if bse > 0 else 0

    sorted_candidates = sorted(candidates, key=key)

    concatenated_axes: list[list[int]] = [[] for i in range(ndim)]

    # Sim all the axes that are no candidates
    for i in range(ndim):
        if i in candidates:
            continue
        concatenated_axes[i] = list(chain(*split_axes[i]))

    # We want to concatenate chunks
    for axis_index in sorted_candidates:
        concatenated_axis = concatenated_axes[axis_index]
        multiplier = math.prod(
            old_largest_width[:axis_index] + old_largest_width[axis_index + 1 :]
        )
        axis_limit = block_size_limit // multiplier

        for partial in split_axes[axis_index]:
            current = partial[0]
            for chunk in partial[1:]:
                if (current + chunk) > axis_limit:
                    concatenated_axis.append(current)
                    current = chunk
                else:
                    current += chunk
            concatenated_axis.append(current)
        old_largest_width[axis_index] = max(concatenated_axis)
    return tuple(tuple(axis) for axis in concatenated_axes)


def _split_chunks_along_partial_boundaries(
    old_chunks: ChunkedAxes, new_chunks: ChunkedAxes
) -> list[list[list[float]]]:
    """Split the old chunks along the boundaries of partials, i.e., groups of new chunks that share the same inputs.

    By splitting along the boundaries before rechunkin their input tasks become disjunct and each partial conceptually
    operates on an independent sub-array.
    """
    from dask.array.rechunk import old_to_new

    _old_to_new = old_to_new(old_chunks, new_chunks)

    partials = _slice_new_chunks_into_partials(_old_to_new)

    split_axes = []

    # Along each axis, we want to figure out how we have to split input chunks in order to make
    # partials disjunct. We then group the resulting input chunks per partial before returning.
    for axis_index, slices in enumerate(partials):
        old_to_new_axis = _old_to_new[axis_index]
        old_axis = old_chunks[axis_index]
        split_axis = []
        partial_chunks = []
        for slice_ in slices:
            first_new_chunk = slice_.start
            first_old_chunk, first_old_slice = old_to_new_axis[first_new_chunk][0]
            last_new_chunk = slice_.stop - 1
            last_old_chunk, last_old_slice = old_to_new_axis[last_new_chunk][-1]

            first_chunk_size = old_axis[first_old_chunk]
            last_chunk_size = old_axis[last_old_chunk]

            if first_old_chunk == last_old_chunk:
                chunk_size = first_chunk_size
                if (
                    last_old_slice.stop is not None
                    and last_old_slice.stop != last_chunk_size
                ):
                    chunk_size = last_old_slice.stop
                if first_old_slice.start != 0:
                    chunk_size -= first_old_slice.start
                partial_chunks.append(chunk_size)
            else:
                partial_chunks.append(first_chunk_size - first_old_slice.start)

                partial_chunks.extend(old_axis[first_old_chunk + 1 : last_old_chunk])

                if last_old_slice.stop is not None:
                    chunk_size = last_old_slice.stop
                else:
                    chunk_size = last_chunk_size

                partial_chunks.append(chunk_size)
            split_axis.append(partial_chunks)
            partial_chunks = []
        if partial_chunks:
            split_axis.append(partial_chunks)
        split_axes.append(split_axis)
    return split_axes


def _largest_block_size(chunks: tuple[tuple[int, ...], ...]) -> int:
    return math.prod(map(max, chunks))


def _split_partials(
    old_to_new: list[Any],
) -> Generator[_NDPartial]:
    """Split the rechunking into partials that can be performed separately"""
    partials_per_axis = _split_partials_per_axis(old_to_new)
    indices_per_axis = (range(len(partials)) for partials in partials_per_axis)
    for nindex, partial_per_axis in zip(
        product(*indices_per_axis), product(*partials_per_axis)
    ):
        old, new = zip(*partial_per_axis)
        yield _NDPartial(old, new, nindex)


def _split_partials_per_axis(old_to_new: list[Any]) -> tuple[tuple[_Partial, ...], ...]:
    """Split the rechunking into partials that can be performed separately
    on each axis"""
    sliced_axes = _slice_new_chunks_into_partials(old_to_new)

    partial_axes = []
    for axis_index, slices in enumerate(sliced_axes):
        partials = []
        for slice_ in slices:
            last_old_chunk: int
            first_old_chunk, _ = old_to_new[axis_index][slice_.start][0]
            last_old_chunk, _ = old_to_new[axis_index][slice_.stop - 1][-1]
            partials.append(
                _Partial(
                    old=slice(first_old_chunk, last_old_chunk + 1),
                    new=slice_,
                )
            )
        partial_axes.append(tuple(partials))
    return tuple(partial_axes)


def _slice_new_chunks_into_partials(
    old_to_new: list[list[list[tuple[int, slice]]]]
) -> SlicedAxes:
    """Slice the new chunks into partials that can be computed separately"""
    sliced_axes = []

    chunk_shape = tuple(len(axis) for axis in old_to_new)

    for axis_index, old_to_new_axis in enumerate(old_to_new):
        # Two consecutive output chunks A and B belong to the same partial rechunk
        # if A and B share the same input chunks, i.e., separating A and B would not
        # allow us to cull more input tasks.

        # Index of the last input chunk of this partial rechunk
        first_old_chunk: int | None = None
        partial_splits = [0]
        recipe: list[tuple[int, slice]]
        for new_chunk_index, recipe in enumerate(old_to_new_axis):
            if len(recipe) == 0:
                continue
            current_first_old_chunk, _ = recipe[0]
            current_last_old_chunk, _ = recipe[-1]
            if first_old_chunk is None:
                first_old_chunk = current_first_old_chunk
            elif first_old_chunk != current_last_old_chunk:
                partial_splits.append(new_chunk_index)
                first_old_chunk = current_first_old_chunk
        partial_splits.append(chunk_shape[axis_index])
        sliced_axes.append(
            tuple(slice(a, b) for a, b in toolz.sliding_window(2, partial_splits))
        )
    return tuple(sliced_axes)


def _ndindices_of_slice(ndslice: NDSlice) -> Iterator[NDIndex]:
    return product(*(range(slc.start, slc.stop) for slc in ndslice))


def _partial_index(global_index: NDIndex, partial_offset: NDIndex) -> NDIndex:
    return tuple(index - offset for index, offset in zip(global_index, partial_offset))


def partial_concatenate(
    input_name: str,
    input_chunks: ChunkedAxes,
    ndpartial: _NDPartial,
    token: str,
    keepmap: np.ndarray,
    old_to_new: list[Any],
) -> dict[Key, Any]:
    import numpy as np

    from dask.array.chunk import getitem
    from dask.array.core import concatenate3

    dsk: dict[Key, Any] = {}

    slice_group = f"rechunk-slice-{token}"

    partial_keepmap = keepmap[ndpartial.new]
    assert np.sum(partial_keepmap) == 1
    partial_new_index = np.argwhere(partial_keepmap)[0]

    global_new_index = tuple(
        int(ix) + slc.start for ix, slc in zip(partial_new_index, ndpartial.new)
    )

    inputs = tuple(
        old_to_new_axis[ix] for ix, old_to_new_axis in zip(global_new_index, old_to_new)
    )
    shape = tuple(len(axis) for axis in inputs)
    rec_cat_arg = np.empty(shape, dtype="O")

    for old_partial_index in np.ndindex(shape):
        old_global_index, old_slice = zip(
            *(input_axis[index] for index, input_axis in zip(old_partial_index, inputs))
        )
        original_shape = tuple(
            old_axis[index] for index, old_axis in zip(old_global_index, input_chunks)
        )
        if _slicing_is_necessary(old_slice, original_shape):
            key = (slice_group,) + ndpartial.ix + old_global_index
            dsk[key] = t = Task(
                key,
                getitem,
                TaskRef((input_name,) + old_global_index),
                old_slice,
            )
            rec_cat_arg[old_partial_index] = t.ref()
        else:
            rec_cat_arg[old_partial_index] = TaskRef((input_name,) + old_global_index)

    concat_task = Task(
        (rechunk_name(token),) + global_new_index,
        concatenate3,
        parse_input(rec_cat_arg.tolist()),
    )
    dsk[concat_task.key] = concat_task
    return dsk


def _slicing_is_necessary(slice: NDSlice, shape: tuple[int | None, ...]) -> bool:
    """Return True if applying the slice alters the shape, False otherwise."""
    return not all(
        slc.start == 0 and (size is None and slc.stop is None or slc.stop == size)
        for slc, size in zip(slice, shape)
    )


def partial_rechunk(
    input_name: str,
    input_chunks: ChunkedAxes,
    chunks: ChunkedAxes,
    ndpartial: _NDPartial,
    token: str,
    disk: bool,
    keepmap: np.ndarray,
) -> dict[Key, Any]:
    dsk: dict[Key, Any] = {}

    old_partial_offset = tuple(slice_.start for slice_ in ndpartial.old)

    partial_token = tokenize(token, ndpartial.ix)
    # Use `token` to generate a canonical group for the entire rechunk
    transfer_group = f"rechunk-transfer-{token}"
    unpack_group = rechunk_name(token)
    # We can use `partial_token` here because the barrier task share their
    # group across all P2P shuffle-like operations
    # FIXME: Make this group unique per individual P2P shuffle-like operation
    _barrier_key = barrier_key(ShuffleId(partial_token))

    ndim = len(input_chunks)

    partial_old = tuple(
        chunk_axis[partial_axis]
        for partial_axis, chunk_axis in zip(ndpartial.old, input_chunks)
    )
    partial_new: ChunkedAxes = tuple(
        chunks[axis_index][ndpartial.new[axis_index]] for axis_index in range(ndim)
    )

    transfer_keys = []
    for global_index in _ndindices_of_slice(ndpartial.old):
        partial_index = _partial_index(global_index, old_partial_offset)

        input_key = TaskRef((input_name,) + global_index)

        key = (transfer_group,) + ndpartial.ix + global_index
        dsk[key] = t = Task(
            key,
            rechunk_transfer,
            input_key,
            ShuffleId(partial_token),
            partial_index,
        )
        transfer_keys.append(t.ref())

    dsk[_barrier_key] = barrier = P2PBarrierTask(
        _barrier_key,
        p2p_barrier,
        partial_token,
        *transfer_keys,
        spec=ArrayRechunkSpec(
            id=ShuffleId(partial_token), new=partial_new, old=partial_old, disk=disk
        ),
    )

    new_partial_offset = tuple(axis.start for axis in ndpartial.new)
    for global_index in _ndindices_of_slice(ndpartial.new):
        partial_index = _partial_index(global_index, new_partial_offset)
        if keepmap[global_index]:
            k = (unpack_group,) + global_index
            dsk[k] = Task(
                k,
                rechunk_unpack,
                ShuffleId(partial_token),
                partial_index,
                barrier.ref(),
            )
    return dsk


class Split(NamedTuple):
    """Slice of a chunk that is concatenated with other splits to create a new chunk

    Splits define how to slice an input chunk on a single axis into small pieces
    that can be concatenated together with splits from other input chunks to create
    output chunks of a rechunk operation.
    """

    #: Index of the new output chunk to which this split belongs.
    chunk_index: int

    #: Index of the split within the list of splits that are concatenated
    #: to create the new chunk.
    split_index: int

    #: Slice of the input chunk.
    slice: slice


SplitChunk: TypeAlias = list[Split]
SplitAxis: TypeAlias = list[SplitChunk]
SplitAxes: TypeAlias = list[SplitAxis]


def split_axes(old: ChunkedAxes, new: ChunkedAxes) -> SplitAxes:
    """Calculate how to split the old chunks on each axis to create the new chunks

    Parameters
    ----------
    old : ChunkedAxes
        Chunks along each axis of the old array
    new : ChunkedAxes
        Chunks along each axis of the new array

    Returns
    -------
    SplitAxes
        Splits along each axis that determine how to slice the input chunks to create
        the new chunks by concatenating the resulting shards.
    """
    from dask.array.rechunk import old_to_new

    _old_to_new = old_to_new(old, new)

    axes = []
    for axis_index, new_axis in enumerate(_old_to_new):
        old_axis: SplitAxis = [[] for _ in old[axis_index]]
        for new_chunk_index, new_chunk in enumerate(new_axis):
            for split_index, (old_chunk_index, slice) in enumerate(new_chunk):
                old_axis[old_chunk_index].append(
                    Split(new_chunk_index, split_index, slice)
                )
        for old_chunk in old_axis:
            old_chunk.sort(key=lambda split: split.slice.start)
        axes.append(old_axis)
    return axes


def convert_chunk(shards: list[list[tuple[NDIndex, np.ndarray]]]) -> np.ndarray:
    import numpy as np

    from dask.array.core import concatenate3

    indexed: dict[NDIndex, np.ndarray] = {}
    for sublist in shards:
        for index, shard in sublist:
            indexed[index] = shard

    subshape = [max(dim) + 1 for dim in zip(*indexed.keys())]
    assert len(indexed) == np.prod(subshape)

    rec_cat_arg = np.empty(subshape, dtype="O")
    for index, shard in indexed.items():
        rec_cat_arg[tuple(index)] = shard
    arrs = rec_cat_arg.tolist()

    # This may block for several seconds, as it physically reads the memory-mapped
    # buffers from disk
    return concatenate3(arrs)


class ArrayRechunkRun(ShuffleRun[NDIndex, "np.ndarray"]):
    """State for a single active rechunk execution

    This object is responsible for splitting, sending, receiving and combining
    data shards.

    It is entirely agnostic to the distributed system and can perform a rechunk
    with other run instances using `rpc``.

    The user of this needs to guarantee that only `ArrayRechunkRun`s of the same unique
    `ShuffleID` and `run_id` interact.

    Parameters
    ----------
    worker_for:
        A mapping partition_id -> worker_address.
    old:
        Existing chunking of the array per dimension.
    new:
        Desired chunking of the array per dimension.
    id:
        A unique `ShuffleID` this belongs to.
    run_id:
        A unique identifier of the specific execution of the shuffle this belongs to.
    span_id:
        Span identifier; see :doc:`spans`
    local_address:
        The local address this Shuffle can be contacted by using `rpc`.
    directory:
        The scratch directory to buffer data in.
    executor:
        Thread pool to use for offloading compute.
    rpc:
        A callable returning a PooledRPCCall to contact other Shuffle instances.
        Typically a ConnectionPool.
    digest_metric:
        A callable to ingest a performance metric.
        Typically Server.digest_metric.
    scheduler:
        A PooledRPCCall to contact the scheduler.
    memory_limiter_disk:
    memory_limiter_comm:
        A ``ResourceLimiter`` limiting the total amount of memory used in either
        buffer.
    """

    def __init__(
        self,
        worker_for: dict[NDIndex, str],
        old: ChunkedAxes,
        new: ChunkedAxes,
        id: ShuffleId,
        run_id: int,
        span_id: str | None,
        local_address: str,
        directory: str,
        executor: ThreadPoolExecutor,
        rpc: Callable[[str], PooledRPCCall],
        digest_metric: Callable[[Hashable, float], None],
        scheduler: PooledRPCCall,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
        disk: bool,
        loop: IOLoop,
    ):
        super().__init__(
            id=id,
            run_id=run_id,
            span_id=span_id,
            local_address=local_address,
            directory=directory,
            executor=executor,
            rpc=rpc,
            digest_metric=digest_metric,
            scheduler=scheduler,
            memory_limiter_comms=memory_limiter_comms,
            memory_limiter_disk=memory_limiter_disk,
            disk=disk,
            loop=loop,
        )
        self.old = old
        self.new = new
        partitions_of = defaultdict(list)
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = worker_for
        self.split_axes = split_axes(old, new)

    async def _receive(
        self,
        data: list[tuple[NDIndex, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]]],
    ) -> None:
        self.raise_if_closed()

        # Repartition shards and filter out already received ones
        shards = defaultdict(list)
        for d in data:
            id1, payload = d
            if id1 in self.received:
                continue
            self.received.add(id1)
            for id2, shard in payload:
                shards[id2].append(shard)
            self.total_recvd += sizeof(d)
        del data
        if not shards:
            return

        try:
            await self._write_to_disk(shards)
        except Exception as e:
            self._exception = e
            raise

    def _shard_partition(
        self, data: np.ndarray, partition_id: NDIndex
    ) -> dict[str, tuple[NDIndex, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]]]:
        out: dict[str, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]] = defaultdict(
            list
        )
        shards_size = 0
        shards_count = 0

        ndsplits = product(*(axis[i] for axis, i in zip(self.split_axes, partition_id)))

        for ndsplit in ndsplits:
            chunk_index, shard_index, ndslice = zip(*ndsplit)

            shard = data[ndslice]
            # Don't wait until all shards have been transferred over the network
            # before data can be released
            if shard.base is not None:
                shard = shard.copy()

            shards_size += shard.nbytes
            shards_count += 1

            out[self.worker_for[chunk_index]].append(
                (chunk_index, (shard_index, shard))
            )

        context_meter.digest_metric("p2p-shards", shards_size, "bytes")
        context_meter.digest_metric("p2p-shards", shards_count, "count")
        return {k: (partition_id, v) for k, v in out.items()}

    def _get_output_partition(
        self, partition_id: NDIndex, key: Key, **kwargs: Any
    ) -> np.ndarray:
        # Quickly read metadata from disk.
        # This is a bunch of seek()'s interleaved with short reads.
        data = self._read_from_disk(partition_id)
        # Copy the memory-mapped buffers from disk into memory.
        # This is where we'll spend most time.
        return convert_chunk(data)

    def deserialize(self, buffer: Any) -> Any:
        return buffer

    def read(self, path: Path) -> tuple[list[list[tuple[NDIndex, np.ndarray]]], int]:
        """Open a memory-mapped file descriptor to disk, read all metadata, and unpickle
        all arrays. This is a fast sequence of short reads interleaved with seeks.
        Do not read in memory the actual data; the arrays' buffers will point to the
        memory-mapped area.

        The file descriptor will be automatically closed by the kernel when all the
        returned arrays are dereferenced, which will happen after the call to
        concatenate3.
        """
        with path.open(mode="r+b") as fh:
            buffer = memoryview(mmap.mmap(fh.fileno(), 0))

        # The file descriptor has *not* been closed!
        shards = list(unpickle_bytestream(buffer))
        return shards, buffer.nbytes

    def _get_assigned_worker(self, id: NDIndex) -> str:
        return self.worker_for[id]


@dataclass(frozen=True)
class ArrayRechunkSpec(ShuffleSpec[NDIndex]):
    new: ChunkedAxes
    old: ChunkedAxes

    @property
    def output_partitions(self) -> Generator[NDIndex]:
        yield from product(*(range(len(c)) for c in self.new))

    def pick_worker(self, partition: NDIndex, workers: Sequence[str]) -> str:
        npartitions = 1
        for c in self.new:
            npartitions *= len(c)
        ix = 0
        for dim, pos in enumerate(partition):
            if dim > 0:
                ix += len(self.new[dim - 1]) * pos
            else:
                ix += pos
        i = len(workers) * ix // npartitions
        return workers[i]

    def create_run_on_worker(
        self,
        run_id: int,
        span_id: str | None,
        worker_for: dict[NDIndex, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        return ArrayRechunkRun(
            worker_for=worker_for,
            old=self.old,
            new=self.new,
            id=self.id,
            run_id=run_id,
            span_id=span_id,
            directory=os.path.join(
                plugin.worker.local_directory,
                f"shuffle-{self.id}-{run_id}",
            ),
            executor=plugin._executor,
            local_address=plugin.worker.address,
            rpc=plugin.worker.rpc,
            digest_metric=plugin.worker.digest_metric,
            scheduler=plugin.worker.scheduler,
            memory_limiter_disk=plugin.memory_limiter_disk,
            memory_limiter_comms=plugin.memory_limiter_comms,
            disk=self.disk,
            loop=plugin.worker.loop,
        )
