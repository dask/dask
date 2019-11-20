"""
The rechunk module defines:
    intersect_chunks: a function for
        converting chunks to new dimensions
    rechunk: a function to convert the blocks
        of an existing dask array to new chunks or blockshape
"""
import math
import heapq

from itertools import product, chain, count
from operator import getitem, add, mul, itemgetter

import numpy as np
import toolz
from toolz import accumulate, reduce

from ..base import tokenize
from ..highlevelgraph import HighLevelGraph
from ..utils import parse_bytes
from .core import concatenate3, Array, normalize_chunks
from .utils import validate_axis
from .wrap import empty
from .. import config


def cumdims_label(chunks, const):
    """ Internal utility for cumulative sum with label.

    >>> cumdims_label(((5, 3, 3), (2, 2, 1)), 'n')  # doctest: +NORMALIZE_WHITESPACE
    [(('n', 0), ('n', 5), ('n', 8), ('n', 11)),
     (('n', 0), ('n', 2), ('n', 4), ('n', 5))]
    """
    return [
        tuple(zip((const,) * (1 + len(bds)), accumulate(add, (0,) + bds)))
        for bds in chunks
    ]


def _breakpoints(cumold, cumnew):
    """

    >>> new = cumdims_label(((2, 3), (2, 2, 1)), 'n')
    >>> old = cumdims_label(((2, 2, 1), (5,)), 'o')

    >>> _breakpoints(new[0], old[0])
    (('n', 0), ('o', 0), ('n', 2), ('o', 2), ('o', 4), ('n', 5), ('o', 5))
    >>> _breakpoints(new[1], old[1])
    (('n', 0), ('o', 0), ('n', 2), ('n', 4), ('n', 5), ('o', 5))
    """
    return tuple(sorted(cumold + cumnew, key=itemgetter(1)))


def _intersect_1d(breaks):
    """
    Internal utility to intersect chunks for 1d after preprocessing.

    >>> new = cumdims_label(((2, 3), (2, 2, 1)), 'n')
    >>> old = cumdims_label(((2, 2, 1), (5,)), 'o')

    >>> _intersect_1d(_breakpoints(old[0], new[0]))  # doctest: +NORMALIZE_WHITESPACE
    [[(0, slice(0, 2, None))],
     [(1, slice(0, 2, None)), (2, slice(0, 1, None))]]
    >>> _intersect_1d(_breakpoints(old[1], new[1]))  # doctest: +NORMALIZE_WHITESPACE
    [[(0, slice(0, 2, None))],
     [(0, slice(2, 4, None))],
     [(0, slice(4, 5, None))]]

    Parameters
    ----------

    breaks: list of tuples
        Each tuple is ('o', 8) or ('n', 8)
        These are pairs of 'o' old or new 'n'
        indicator with a corresponding cumulative sum.

    Uses 'o' and 'n' to make new tuples of slices for
    the new block crosswalk to old blocks.
    """
    start = 0
    last_end = 0
    old_idx = 0
    ret = []
    ret_next = []
    for idx in range(1, len(breaks)):
        label, br = breaks[idx]
        last_label, last_br = breaks[idx - 1]
        if last_label == "n":
            if ret_next:
                ret.append(ret_next)
                ret_next = []
        if last_label == "o":
            start = 0
        else:
            start = last_end
        end = br - last_br + start
        last_end = end
        if br == last_br:
            if label == "o":
                old_idx += 1
            continue
        ret_next.append((old_idx, slice(start, end)))
        if label == "o":
            old_idx += 1
            start = 0

    if ret_next:
        ret.append(ret_next)

    return ret


def _old_to_new(old_chunks, new_chunks):
    """ Helper to build old_chunks to new_chunks.

    Handles missing values, as long as the missing dimension
    is unchanged.

    Examples
    --------
    >>> old = ((10, 10, 10, 10, 10), )
    >>> new = ((25, 5, 20), )
    >>> _old_to_new(old, new)  # doctest: +NORMALIZE_WHITESPACE
    [[[(0, slice(0, 10, None)), (1, slice(0, 10, None)), (2, slice(0, 5, None))],
      [(2, slice(5, 10, None))],
      [(3, slice(0, 10, None)), (4, slice(0, 10, None))]]]
    """
    old_known = [x for x in old_chunks if not any(math.isnan(y) for y in x)]
    new_known = [x for x in new_chunks if not any(math.isnan(y) for y in x)]

    n_missing = [sum(math.isnan(y) for y in x) for x in old_chunks]
    n_missing2 = [sum(math.isnan(y) for y in x) for x in new_chunks]

    cmo = cumdims_label(old_known, "o")
    cmn = cumdims_label(new_known, "n")

    sums = [sum(o) for o in old_known]
    sums2 = [sum(n) for n in new_known]

    if not sums == sums2:
        raise ValueError("Cannot change dimensions from %r to %r" % (sums, sums2))
    if not n_missing == n_missing2:
        raise ValueError(
            "Chunks must be unchanging along unknown dimensions.\n\n"
            "A possible solution:\n  x.compute_chunk_sizes()"
        )

    old_to_new = [_intersect_1d(_breakpoints(cm[0], cm[1])) for cm in zip(cmo, cmn)]
    for idx, missing in enumerate(n_missing):
        if missing:
            # Missing dimensions are always unchanged, so old -> new is everything
            extra = [[(i, slice(0, None))] for i in range(missing)]
            old_to_new.insert(idx, extra)
    return old_to_new


def intersect_chunks(old_chunks, new_chunks):
    """
    Make dask.array slices as intersection of old and new chunks.

    >>> intersections = intersect_chunks(((4, 4), (2,)),
    ...                                  ((8,), (1, 1)))
    >>> list(intersections)  # doctest: +NORMALIZE_WHITESPACE
    [(((0, slice(0, 4, None)), (0, slice(0, 1, None))),
      ((1, slice(0, 4, None)), (0, slice(0, 1, None)))),
     (((0, slice(0, 4, None)), (0, slice(1, 2, None))),
      ((1, slice(0, 4, None)), (0, slice(1, 2, None))))]

    Parameters
    ----------

    old_chunks : iterable of tuples
        block sizes along each dimension (convert from old_chunks)
    new_chunks: iterable of tuples
        block sizes along each dimension (converts to new_chunks)
    """
    old_to_new = _old_to_new(old_chunks, new_chunks)

    cross1 = product(*old_to_new)
    cross = chain(tuple(product(*cr)) for cr in cross1)
    return cross


def rechunk(x, chunks="auto", threshold=None, block_size_limit=None):
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
    """
    if isinstance(chunks, dict):
        chunks = {validate_axis(c, x.ndim): v for c, v in chunks.items()}
        for i in range(x.ndim):
            if i not in chunks:
                chunks[i] = x.chunks[i]
    if isinstance(chunks, (tuple, list)):
        chunks = tuple(lc if lc is not None else rc for lc, rc in zip(chunks, x.chunks))
    chunks = normalize_chunks(
        chunks, x.shape, limit=block_size_limit, dtype=x.dtype, previous_chunks=x.chunks
    )

    if chunks == x.chunks:
        return x
    ndim = x.ndim
    if not len(chunks) == ndim:
        raise ValueError("Provided chunks are not consistent with shape")
    new_shapes = tuple(map(sum, chunks))

    for new, old in zip(new_shapes, x.shape):
        if new != old and not math.isnan(old) and not math.isnan(new):
            raise ValueError("Provided chunks are not consistent with shape")

    steps = plan_rechunk(
        x.chunks, chunks, x.dtype.itemsize, threshold, block_size_limit
    )
    for c in steps:
        x = _compute_rechunk(x, c)

    return x


def _number_of_blocks(chunks):
    return reduce(mul, map(len, chunks))


def _largest_block_size(chunks):
    return reduce(mul, map(max, chunks))


def estimate_graph_size(old_chunks, new_chunks):
    """ Estimate the graph size during a rechunk computation.
    """
    # Estimate the number of intermediate blocks that will be produced
    # (we don't use intersect_chunks() which is much more expensive)
    crossed_size = reduce(
        mul, (len(oc) + len(nc) for oc, nc in zip(old_chunks, new_chunks))
    )
    return crossed_size


def divide_to_width(desired_chunks, max_width):
    """ Minimally divide the given chunks so as to make the largest chunk
    width less or equal than *max_width*.
    """
    chunks = []
    for c in desired_chunks:
        nb_divides = int(np.ceil(c / max_width))
        for i in range(nb_divides):
            n = c // (nb_divides - i)
            chunks.append(n)
            c -= n
        assert c == 0
    return tuple(chunks)


def merge_to_number(desired_chunks, max_number):
    """ Minimally merge the given chunks so as to drop the number of
    chunks below *max_number*, while minimizing the largest width.
    """
    if len(desired_chunks) <= max_number:
        return desired_chunks

    distinct = set(desired_chunks)
    if len(distinct) == 1:
        # Fast path for homogeneous target, also ensuring a regular result
        w = distinct.pop()
        n = len(desired_chunks)
        total = n * w

        desired_width = total // max_number
        width = w * (desired_width // w)
        adjust = (total - max_number * width) // w

        return (width + w,) * adjust + (width,) * (max_number - adjust)

    desired_width = sum(desired_chunks) // max_number
    nmerges = len(desired_chunks) - max_number

    heap = [
        (desired_chunks[i] + desired_chunks[i + 1], i, i + 1)
        for i in range(len(desired_chunks) - 1)
    ]
    heapq.heapify(heap)

    chunks = list(desired_chunks)

    while nmerges > 0:
        # Find smallest interval to merge
        width, i, j = heapq.heappop(heap)
        # If interval was made invalid by another merge, recompute
        # it, re-insert it and retry.
        if chunks[j] == 0:
            j += 1
            while chunks[j] == 0:
                j += 1
            heapq.heappush(heap, (chunks[i] + chunks[j], i, j))
            continue
        elif chunks[i] + chunks[j] != width:
            heapq.heappush(heap, (chunks[i] + chunks[j], i, j))
            continue
        # Merge
        assert chunks[i] != 0
        chunks[i] = 0  # mark deleted
        chunks[j] = width
        nmerges -= 1

    return tuple(filter(None, chunks))


def find_merge_rechunk(old_chunks, new_chunks, block_size_limit):
    """
    Find an intermediate rechunk that would merge some adjacent blocks
    together in order to get us nearer the *new_chunks* target, without
    violating the *block_size_limit* (in number of elements).
    """
    ndim = len(old_chunks)

    old_largest_width = [max(c) for c in old_chunks]
    new_largest_width = [max(c) for c in new_chunks]

    graph_size_effect = {
        dim: len(nc) / len(oc)
        for dim, (oc, nc) in enumerate(zip(old_chunks, new_chunks))
    }

    block_size_effect = {
        dim: new_largest_width[dim] / (old_largest_width[dim] or 1)
        for dim in range(ndim)
    }

    # Our goal is to reduce the number of nodes in the rechunk graph
    # by merging some adjacent chunks, so consider dimensions where we can
    # reduce the # of chunks
    merge_candidates = [dim for dim in range(ndim) if graph_size_effect[dim] <= 1.0]

    # Merging along each dimension reduces the graph size by a certain factor
    # and increases memory largest block size by a certain factor.
    # We want to optimize the graph size while staying below the given
    # block_size_limit.  This is in effect a knapsack problem, except with
    # multiplicative values and weights.  Just use a greedy algorithm
    # by trying dimensions in decreasing value / weight order.
    def key(k):
        gse = graph_size_effect[k]
        bse = block_size_effect[k]
        if bse == 1:
            bse = 1 + 1e-9
        return (np.log(gse) / np.log(bse)) if bse > 0 else 0

    sorted_candidates = sorted(merge_candidates, key=key)

    largest_block_size = reduce(mul, old_largest_width)

    chunks = list(old_chunks)
    memory_limit_hit = False

    for dim in sorted_candidates:
        # Examine this dimension for possible graph reduction
        new_largest_block_size = (
            largest_block_size * new_largest_width[dim] // (old_largest_width[dim] or 1)
        )
        if new_largest_block_size <= block_size_limit:
            # Full replacement by new chunks is possible
            chunks[dim] = new_chunks[dim]
            largest_block_size = new_largest_block_size
        else:
            # Try a partial rechunk, dividing the new chunks into
            # smaller pieces
            largest_width = old_largest_width[dim]
            chunk_limit = int(block_size_limit * largest_width / largest_block_size)
            c = divide_to_width(new_chunks[dim], chunk_limit)
            if len(c) <= len(old_chunks[dim]):
                # We manage to reduce the number of blocks, so do it
                chunks[dim] = c
                largest_block_size = largest_block_size * max(c) // largest_width

            memory_limit_hit = True

    assert largest_block_size == _largest_block_size(chunks)
    assert largest_block_size <= block_size_limit
    return tuple(chunks), memory_limit_hit


def find_split_rechunk(old_chunks, new_chunks, graph_size_limit):
    """
    Find an intermediate rechunk that would split some chunks to
    get us nearer *new_chunks*, without violating the *graph_size_limit*.
    """
    ndim = len(old_chunks)

    chunks = list(old_chunks)

    for dim in range(ndim):
        graph_size = estimate_graph_size(chunks, new_chunks)
        if graph_size > graph_size_limit:
            break
        if len(old_chunks[dim]) > len(new_chunks[dim]):
            # It's not interesting to split
            continue
        # Merge the new chunks so as to stay within the graph size budget
        max_number = int(len(old_chunks[dim]) * graph_size_limit / graph_size)
        c = merge_to_number(new_chunks[dim], max_number)
        assert len(c) <= max_number
        # Consider the merge successful if its result has a greater length
        # and smaller max width than the old chunks
        if len(c) >= len(old_chunks[dim]) and max(c) <= max(old_chunks[dim]):
            chunks[dim] = c

    return tuple(chunks)


def plan_rechunk(
    old_chunks, new_chunks, itemsize, threshold=None, block_size_limit=None
):
    """ Plan an iterative rechunking from *old_chunks* to *new_chunks*.
    The plan aims to minimize the rechunk graph size.

    Parameters
    ----------
    itemsize: int
        The item size of the array
    threshold: int
        The graph growth factor under which we don't bother
        introducing an intermediate step
    block_size_limit: int
        The maximum block size (in bytes) we want to produce during an
        intermediate step

    Notes
    -----
    No intermediate steps will be planned if any dimension of ``old_chunks``
    is unknown.
    """
    threshold = threshold or config.get("array.rechunk-threshold")
    block_size_limit = block_size_limit or config.get("array.chunk-size")
    if isinstance(block_size_limit, str):
        block_size_limit = parse_bytes(block_size_limit)

    ndim = len(new_chunks)
    steps = []
    has_nans = [any(math.isnan(y) for y in x) for x in old_chunks]

    if ndim <= 1 or not all(new_chunks) or any(has_nans):
        # Trivial array / unknown dim => no need / ability for an intermediate
        return steps + [new_chunks]

    # Make it a number ef elements
    block_size_limit /= itemsize

    # Fix block_size_limit if too small for either old_chunks or new_chunks
    largest_old_block = _largest_block_size(old_chunks)
    largest_new_block = _largest_block_size(new_chunks)
    block_size_limit = max([block_size_limit, largest_old_block, largest_new_block])

    # The graph size above which to optimize
    graph_size_threshold = threshold * (
        _number_of_blocks(old_chunks) + _number_of_blocks(new_chunks)
    )

    current_chunks = old_chunks
    first_pass = True

    while True:
        graph_size = estimate_graph_size(current_chunks, new_chunks)
        if graph_size < graph_size_threshold:
            break

        if first_pass:
            chunks = current_chunks
        else:
            # We hit the block_size_limit in a previous merge pass =>
            # accept a significant increase in graph size in exchange for
            # 1) getting nearer the goal 2) reducing the largest block size
            # to make place for the following merge.
            # To see this pass in action, make the block_size_limit very small.
            chunks = find_split_rechunk(
                current_chunks, new_chunks, graph_size * threshold
            )
        chunks, memory_limit_hit = find_merge_rechunk(
            chunks, new_chunks, block_size_limit
        )
        if (chunks == current_chunks and not first_pass) or chunks == new_chunks:
            break
        steps.append(chunks)
        current_chunks = chunks
        if not memory_limit_hit:
            break
        first_pass = False

    return steps + [new_chunks]


def _compute_rechunk(x, chunks):
    """ Compute the rechunk of *x* to the given *chunks*.
    """
    if x.size == 0:
        # Special case for empty array, as the algorithm below does not behave correctly
        return empty(x.shape, chunks=chunks, dtype=x.dtype)

    ndim = x.ndim
    crossed = intersect_chunks(x.chunks, chunks)
    x2 = dict()
    intermediates = dict()
    token = tokenize(x, chunks)
    merge_name = "rechunk-merge-" + token
    split_name = "rechunk-split-" + token
    split_name_suffixes = count()

    # Pre-allocate old block references, to allow re-use and reduce the
    # graph's memory footprint a bit.
    old_blocks = np.empty([len(c) for c in x.chunks], dtype="O")
    for index in np.ndindex(old_blocks.shape):
        old_blocks[index] = (x.name,) + index

    # Iterate over all new blocks
    new_index = product(*(range(len(c)) for c in chunks))

    for new_idx, cross1 in zip(new_index, crossed):
        key = (merge_name,) + new_idx
        old_block_indices = [[cr[i][0] for cr in cross1] for i in range(ndim)]
        subdims1 = [len(set(old_block_indices[i])) for i in range(ndim)]

        rec_cat_arg = np.empty(subdims1, dtype="O")
        rec_cat_arg_flat = rec_cat_arg.flat

        # Iterate over the old blocks required to build the new block
        for rec_cat_index, ind_slices in enumerate(cross1):
            old_block_index, slices = zip(*ind_slices)
            name = (split_name, next(split_name_suffixes))
            old_index = old_blocks[old_block_index][1:]
            if all(
                slc.start == 0 and slc.stop == x.chunks[i][ind]
                for i, (slc, ind) in enumerate(zip(slices, old_index))
            ):
                rec_cat_arg_flat[rec_cat_index] = old_blocks[old_block_index]
            else:
                intermediates[name] = (getitem, old_blocks[old_block_index], slices)
                rec_cat_arg_flat[rec_cat_index] = name

        assert rec_cat_index == rec_cat_arg.size - 1

        # New block is formed by concatenation of sliced old blocks
        if all(d == 1 for d in rec_cat_arg.shape):
            x2[key] = rec_cat_arg.flat[0]
        else:
            x2[key] = (concatenate3, rec_cat_arg.tolist())

    del old_blocks, new_index

    layer = toolz.merge(x2, intermediates)
    graph = HighLevelGraph.from_collections(merge_name, layer, dependencies=[x])
    return Array(graph, merge_name, chunks, meta=x)


class _PrettyBlocks(object):
    def __init__(self, blocks):
        self.blocks = blocks

    def __str__(self):
        runs = []
        run = []
        repeats = 0
        for c in self.blocks:
            if run and run[-1] == c:
                if repeats == 0 and len(run) > 1:
                    runs.append((None, run[:-1]))
                    run = run[-1:]
                repeats += 1
            else:
                if repeats > 0:
                    assert len(run) == 1
                    runs.append((repeats + 1, run[-1]))
                    run = []
                    repeats = 0
                run.append(c)
        if run:
            if repeats == 0:
                runs.append((None, run))
            else:
                assert len(run) == 1
                runs.append((repeats + 1, run[-1]))

        parts = []
        for repeats, run in runs:
            if repeats is None:
                parts.append(str(run))
            else:
                parts.append("%d*[%s]" % (repeats, run))
        return " | ".join(parts)

    __repr__ = __str__


def format_blocks(blocks):
    """
    Pretty-format *blocks*.

    >>> format_blocks((10, 10, 10))
    3*[10]
    >>> format_blocks((2, 3, 4))
    [2, 3, 4]
    >>> format_blocks((10, 10, 5, 6, 2, 2, 2, 7))
    2*[10] | [5, 6] | 3*[2] | [7]
    """
    assert isinstance(blocks, tuple) and all(
        isinstance(x, int) or math.isnan(x) for x in blocks
    )
    return _PrettyBlocks(blocks)


def format_chunks(chunks):
    """
    >>> format_chunks((10 * (3,), 3 * (10,)))
    (10*[3], 3*[10])
    """
    assert isinstance(chunks, tuple)
    return tuple(format_blocks(c) for c in chunks)


def format_plan(plan):
    """
    >>> format_plan([((10, 10, 10), (15, 15)), ((30,), (10, 10, 10))])
    [(3*[10], 2*[15]), ([30], 3*[10])]
    """
    return [format_chunks(c) for c in plan]
