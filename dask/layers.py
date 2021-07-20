import operator
from collections import defaultdict
from functools import partial
from itertools import product
from typing import List, Optional, Tuple

import tlz as toolz
from tlz.curried import map

from .base import tokenize
from .blockwise import Blockwise, BlockwiseDep, BlockwiseDepDict, blockwise_token
from .core import flatten, keys_in_tasks
from .highlevelgraph import Layer
from .utils import apply, concrete, insert, stringify, stringify_collection_keys

#
##
###  General Utilities
##
#


class CallableLazyImport:
    """Function Wrapper for Lazy Importing.

    This Class should only be used when materializing a graph
    on a distributed scheduler.
    """

    def __init__(self, function_path):
        self.function_path = function_path

    def __call__(self, *args, **kwargs):
        from distributed.utils import import_term

        return import_term(self.function_path)(*args, **kwargs)


#
##
###  Array Layers & Utilities
##
#


class CreateArrayDeps(BlockwiseDep):
    """Index-chunk mapping for BlockwiseCreateArray"""

    def __init__(self, chunks: tuple):
        self.chunks = chunks
        self.numblocks = tuple(len(chunk) for chunk in chunks)
        self.produces_tasks = False

    def __getitem__(self, idx: tuple):
        return tuple(chunk[i] for i, chunk in zip(idx, self.chunks))

    def __dask_distributed_pack__(
        self, required_indices: Optional[List[Tuple[int, ...]]] = None
    ):
        return {"chunks": self.chunks}

    @classmethod
    def __dask_distributed_unpack__(cls, state):
        return cls(**state)


class BlockwiseCreateArray(Blockwise):
    """
    Specialized Blockwise Layer for array creation routines.

    Enables HighLevelGraph optimizations.

    Parameters
    ----------
    name: string
        The output name.
    func : callable
        Function to apply to populate individual blocks. This function should take
        an iterable containing the dimensions of the given block.
    shape: iterable
        Iterable containing the overall shape of the array.
    chunks: iterable
        Iterable containing the chunk sizes along each dimension of array.
    """

    def __init__(
        self,
        name,
        func,
        shape,
        chunks,
    ):
        # Define "blockwise" graph
        dsk = {name: (func, blockwise_token(0))}

        out_ind = tuple(range(len(shape)))
        super().__init__(
            output=name,
            output_indices=out_ind,
            dsk=dsk,
            indices=[(CreateArrayDeps(chunks), out_ind)],
            numblocks={},
        )


class ArrayOverlapLayer(Layer):
    """Simple HighLevelGraph array overlap layer.

    Lazily computed High-level graph layer for a array overlap operations.

    Parameters
    ----------
    name : str
        Name of new output overlap array.
    array : Dask array
    axes: Mapping
        Axes dictionary indicating overlap in each dimension,
        e.g. ``{'0': 1, '1': 1}``
    """

    def __init__(
        self,
        name,
        axes,
        chunks,
        numblocks,
        token,
    ):
        super().__init__()
        self.name = name
        self.axes = axes
        self.chunks = chunks
        self.numblocks = numblocks
        self.token = token
        self._cached_keys = None

    def __repr__(self):
        return "ArrayOverlapLayer<name='{}'".format(self.name)

    @property
    def _dict(self):
        """Materialize full dict representation"""
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def is_materialized(self):
        return hasattr(self, "_cached_dict")

    def get_output_keys(self):
        return self.keys()  # FIXME! this implementation materializes the graph

    def _dask_keys(self):
        if self._cached_keys is not None:
            return self._cached_keys

        name, chunks, numblocks = self.name, self.chunks, self.numblocks

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

    def _construct_graph(self, deserializing=False):
        """Construct graph for a simple overlap operation."""
        axes = self.axes
        chunks = self.chunks
        name = self.name
        dask_keys = self._dask_keys()

        getitem_name = "getitem-" + self.token
        overlap_name = "overlap-" + self.token

        if deserializing:
            # Use CallableLazyImport objects to avoid importing dataframe
            # module on the scheduler
            concatenate3 = CallableLazyImport("dask.array.core.concatenate3")
        else:
            # Not running on distributed scheduler - Use explicit functions
            from dask.array.core import concatenate3

        dims = list(map(len, chunks))
        expand_key2 = partial(_expand_keys_around_center, dims=dims, axes=axes)

        # Make keys for each of the surrounding sub-arrays
        interior_keys = toolz.pipe(
            dask_keys, flatten, map(expand_key2), map(flatten), toolz.concat, list
        )
        interior_slices = {}
        overlap_blocks = {}
        for k in interior_keys:
            frac_slice = fractional_slice((name,) + k, axes)
            if (name,) + k != frac_slice:
                interior_slices[(getitem_name,) + k] = frac_slice
            else:
                interior_slices[(getitem_name,) + k] = (name,) + k
                overlap_blocks[(overlap_name,) + k] = (
                    concatenate3,
                    (concrete, expand_key2((None,) + k, name=getitem_name)),
                )

        dsk = toolz.merge(interior_slices, overlap_blocks)
        return dsk

    @classmethod
    def __dask_distributed_unpack__(cls, state):
        return cls(**state)._construct_graph(deserializing=True)


def _expand_keys_around_center(k, dims, name=None, axes=None):
    """Get all neighboring keys around center

    Parameters
    ----------
    k: tuple
        They key around which to generate new keys
    dims: Sequence[int]
        The number of chunks in each dimension
    name: Option[str]
        The name to include in the output keys, or none to include no name
    axes: Dict[int, int]
        The axes active in the expansion.  We don't expand on non-active axes

    Examples
    --------
    >>> _expand_keys_around_center(('x', 2, 3), dims=[5, 5], name='y', axes={0: 1, 1: 1})  # noqa: E501 # doctest: +NORMALIZE_WHITESPACE
    [[('y', 1.1, 2.1), ('y', 1.1, 3), ('y', 1.1, 3.9)],
     [('y',   2, 2.1), ('y',   2, 3), ('y',   2, 3.9)],
     [('y', 2.9, 2.1), ('y', 2.9, 3), ('y', 2.9, 3.9)]]

    >>> _expand_keys_around_center(('x', 0, 4), dims=[5, 5], name='y', axes={0: 1, 1: 1})  # noqa: E501 # doctest: +NORMALIZE_WHITESPACE
    [[('y',   0, 3.1), ('y',   0,   4)],
     [('y', 0.9, 3.1), ('y', 0.9,   4)]]
    """

    def inds(i, ind):
        rv = []
        if ind - 0.9 > 0:
            rv.append(ind - 0.9)
        rv.append(ind)
        if ind + 0.9 < dims[i] - 1:
            rv.append(ind + 0.9)
        return rv

    shape = []
    for i, ind in enumerate(k[1:]):
        num = 1
        if ind > 0:
            num += 1
        if ind < dims[i] - 1:
            num += 1
        shape.append(num)

    args = [
        inds(i, ind) if any((axes.get(i, 0),)) else [ind] for i, ind in enumerate(k[1:])
    ]
    if name is not None:
        args = [[name]] + args
    seq = list(product(*args))
    shape2 = [d if any((axes.get(i, 0),)) else 1 for i, d in enumerate(shape)]
    result = reshapelist(shape2, seq)
    return result


def reshapelist(shape, seq):
    """Reshape iterator to nested shape

    >>> reshapelist((2, 3), range(6))
    [[0, 1, 2], [3, 4, 5]]
    """
    if len(shape) == 1:
        return list(seq)
    else:
        n = int(len(seq) / shape[0])
        return [reshapelist(shape[1:], part) for part in toolz.partition(n, seq)]


def fractional_slice(task, axes):
    """

    >>> fractional_slice(('x', 5.1), {0: 2})
    (<built-in function getitem>, ('x', 5), (slice(-2, None, None),))

    >>> fractional_slice(('x', 3, 5.1), {0: 2, 1: 3})
    (<built-in function getitem>, ('x', 3, 5), (slice(None, None, None), slice(-3, None, None)))

    >>> fractional_slice(('x', 2.9, 5.1), {0: 2, 1: 3})
    (<built-in function getitem>, ('x', 3, 5), (slice(0, 2, None), slice(-3, None, None)))
    """
    rounded = (task[0],) + tuple(int(round(i)) for i in task[1:])

    index = []
    for i, (t, r) in enumerate(zip(task[1:], rounded[1:])):
        depth = axes.get(i, 0)
        if isinstance(depth, tuple):
            left_depth = depth[0]
            right_depth = depth[1]
        else:
            left_depth = depth
            right_depth = depth

        if t == r:
            index.append(slice(None, None, None))
        elif t < r and right_depth:
            index.append(slice(0, right_depth))
        elif t > r and left_depth:
            index.append(slice(-left_depth, None))
        else:
            index.append(slice(0, 0))
    index = tuple(index)

    if all(ind == slice(None, None, None) for ind in index):
        return task
    else:
        return (operator.getitem, rounded, index)


#
##
###  DataFrame Layers & Utilities
##
#


class DataFrameLayer(Layer):
    """DataFrame-based HighLevelGraph Layer"""

    def project_columns(self, output_columns):
        """Produce a column projection for this layer.
        Given a list of required output columns, this method
        returns a tuple with the projected layer, and any column
        dependencies for this layer.  A value of ``None`` for
        ``output_columns`` means that the current layer (and
        any dependent layers) cannot be projected. This method
        should be overridden by specialized DataFrame layers
        to enable column projection.
        """

        # Default behavior.
        # Return: `projected_layer`, `dep_columns`
        return self, None


class SimpleShuffleLayer(DataFrameLayer):
    """Simple HighLevelGraph Shuffle layer

    High-level graph layer for a simple shuffle operation in which
    each output partition depends on all input partitions.

    Parameters
    ----------
    name : str
        Name of new shuffled output collection.
    column : str or list of str
        Column(s) to be used to map rows to output partitions (by hashing).
    npartitions : int
        Number of output partitions.
    npartitions_input : int
        Number of partitions in the original (un-shuffled) DataFrame.
    ignore_index: bool, default False
        Ignore index during shuffle.  If ``True``, performance may improve,
        but index values will not be preserved.
    name_input : str
        Name of input collection.
    meta_input : pd.DataFrame-like object
        Empty metadata of input collection.
    parts_out : list of int (optional)
        List of required output-partition indices.
    annotations : dict (optional)
        Layer annotations
    """

    def __init__(
        self,
        name,
        column,
        npartitions,
        npartitions_input,
        ignore_index,
        name_input,
        meta_input,
        parts_out=None,
        annotations=None,
    ):
        super().__init__(annotations=annotations)
        self.name = name
        self.column = column
        self.npartitions = npartitions
        self.npartitions_input = npartitions_input
        self.ignore_index = ignore_index
        self.name_input = name_input
        self.meta_input = meta_input
        self.parts_out = parts_out or range(npartitions)
        self.split_name = "split-" + self.name

        # The scheduling policy of Dask is generally depth-first,
        # which works great in most cases. However, in case of shuffle,
        # it increases the memory usage significantly. This is because
        # depth-first delays the freeing of the result of `shuffle_group()`
        # until the end of the shuffling.
        #
        # We address this by manually setting a high "prioroty" to the
        # `getitem()` ("split") tasks, using annotations. This forces a
        # breadth-first scheduling of the tasks tath directly depend on
        # the `shuffle_group()` output, allowing that data to be freed
        # much earlier.
        #
        # See https://github.com/dask/dask/pull/6051 for a detailed discussion.
        self.annotations = self.annotations or {}
        if "priority" not in self.annotations:
            self.annotations["priority"] = {}
        self.annotations["priority"]["__expanded_annotations__"] = None
        self.annotations["priority"].update({_key: 1 for _key in self.get_split_keys()})

    def get_split_keys(self):
        # Return SimpleShuffleLayer "split" keys
        return [
            stringify((self.split_name, part_out, part_in))
            for part_in in range(self.npartitions_input)
            for part_out in self.parts_out
        ]

    def get_output_keys(self):
        return {(self.name, part) for part in self.parts_out}

    def __repr__(self):
        return "SimpleShuffleLayer<name='{}', npartitions={}>".format(
            self.name, self.npartitions
        )

    def is_materialized(self):
        return hasattr(self, "_cached_dict")

    @property
    def _dict(self):
        """Materialize full dict representation"""
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def _keys_to_parts(self, keys):
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            try:
                _name, _part = key
            except ValueError:
                continue
            if _name != self.name:
                continue
            parts.add(_part)
        return parts

    def _cull_dependencies(self, keys, parts_out=None):
        """Determine the necessary dependencies to produce `keys`.

        For a simple shuffle, output partitions always depend on
        all input partitions. This method does not require graph
        materialization.
        """
        deps = defaultdict(set)
        parts_out = parts_out or self._keys_to_parts(keys)
        for part in parts_out:
            deps[(self.name, part)] |= {
                (self.name_input, i) for i in range(self.npartitions_input)
            }
        return deps

    def _cull(self, parts_out):
        return SimpleShuffleLayer(
            self.name,
            self.column,
            self.npartitions,
            self.npartitions_input,
            self.ignore_index,
            self.name_input,
            self.meta_input,
            parts_out=parts_out,
        )

    def cull(self, keys, all_keys):
        """Cull a SimpleShuffleLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indicies) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        culled_deps = self._cull_dependencies(keys, parts_out=parts_out)
        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def __reduce__(self):
        attrs = [
            "name",
            "column",
            "npartitions",
            "npartitions_input",
            "ignore_index",
            "name_input",
            "meta_input",
            "parts_out",
            "annotations",
        ]
        return (SimpleShuffleLayer, tuple(getattr(self, attr) for attr in attrs))

    def __dask_distributed_pack__(
        self, all_hlg_keys, known_key_dependencies, client, client_keys
    ):
        from distributed.protocol.serialize import to_serialize

        return {
            "name": self.name,
            "column": self.column,
            "npartitions": self.npartitions,
            "npartitions_input": self.npartitions_input,
            "ignore_index": self.ignore_index,
            "name_input": self.name_input,
            "meta_input": to_serialize(self.meta_input),
            "parts_out": list(self.parts_out),
        }

    @classmethod
    def __dask_distributed_unpack__(cls, state, dsk, dependencies):
        from distributed.worker import dumps_task

        # msgpack will convert lists into tuples, here
        # we convert them back to lists
        if isinstance(state["column"], tuple):
            state["column"] = list(state["column"])
        if "inputs" in state:
            state["inputs"] = list(state["inputs"])

        # Materialize the layer
        layer_dsk = cls(**state)._construct_graph(deserializing=True)

        # Convert all keys to strings and dump tasks
        layer_dsk = {
            stringify(k): stringify_collection_keys(v) for k, v in layer_dsk.items()
        }
        keys = layer_dsk.keys() | dsk.keys()

        # TODO: use shuffle-knowledge to calculate dependencies more efficiently
        deps = {k: keys_in_tasks(keys, [v]) for k, v in layer_dsk.items()}

        return {"dsk": toolz.valmap(dumps_task, layer_dsk), "deps": deps}

    def _construct_graph(self, deserializing=False):
        """Construct graph for a simple shuffle operation."""

        shuffle_group_name = "group-" + self.name

        if deserializing:
            # Use CallableLazyImport objects to avoid importing dataframe
            # module on the scheduler
            concat_func = CallableLazyImport("dask.dataframe.core._concat")
            shuffle_group_func = CallableLazyImport(
                "dask.dataframe.shuffle.shuffle_group"
            )
        else:
            # Not running on distributed scheduler - Use explicit functions
            from dask.dataframe.core import _concat as concat_func
            from dask.dataframe.shuffle import shuffle_group as shuffle_group_func

        dsk = {}
        for part_out in self.parts_out:
            _concat_list = [
                (self.split_name, part_out, part_in)
                for part_in in range(self.npartitions_input)
            ]
            dsk[(self.name, part_out)] = (
                concat_func,
                _concat_list,
                self.ignore_index,
            )
            for _, _part_out, _part_in in _concat_list:
                dsk[(self.split_name, _part_out, _part_in)] = (
                    operator.getitem,
                    (shuffle_group_name, _part_in),
                    _part_out,
                )
                if (shuffle_group_name, _part_in) not in dsk:
                    dsk[(shuffle_group_name, _part_in)] = (
                        shuffle_group_func,
                        (self.name_input, _part_in),
                        self.column,
                        0,
                        self.npartitions,
                        self.npartitions,
                        self.ignore_index,
                        self.npartitions,
                    )

        return dsk


class ShuffleLayer(SimpleShuffleLayer):
    """Shuffle-stage HighLevelGraph layer

    High-level graph layer corresponding to a single stage of
    a multi-stage inter-partition shuffle operation.

    Stage: (shuffle-group) -> (shuffle-split) -> (shuffle-join)

    Parameters
    ----------
    name : str
        Name of new (partially) shuffled collection.
    column : str or list of str
        Column(s) to be used to map rows to output partitions (by hashing).
    inputs : list of tuples
        Each tuple dictates the data movement for a specific partition.
    stage : int
        Index of the current shuffle stage.
    npartitions : int
        Number of output partitions for the full (multi-stage) shuffle.
    npartitions_input : int
        Number of partitions in the original (un-shuffled) DataFrame.
    k : int
        A partition is split into this many groups during each stage.
    ignore_index: bool, default False
        Ignore index during shuffle.  If ``True``, performance may improve,
        but index values will not be preserved.
    name_input : str
        Name of input collection.
    meta_input : pd.DataFrame-like object
        Empty metadata of input collection.
    parts_out : list of int (optional)
        List of required output-partition indices.
    annotations : dict (optional)
        Layer annotations
    """

    def __init__(
        self,
        name,
        column,
        inputs,
        stage,
        npartitions,
        npartitions_input,
        nsplits,
        ignore_index,
        name_input,
        meta_input,
        parts_out=None,
        annotations=None,
    ):
        self.inputs = inputs
        self.stage = stage
        self.nsplits = nsplits
        super().__init__(
            name,
            column,
            npartitions,
            npartitions_input,
            ignore_index,
            name_input,
            meta_input,
            parts_out=parts_out or range(len(inputs)),
            annotations=annotations,
        )

    def get_split_keys(self):
        # Return ShuffleLayer "split" keys
        keys = []
        for part in self.parts_out:
            out = self.inputs[part]
            for i in range(self.nsplits):
                keys.append(
                    stringify(
                        (
                            self.split_name,
                            out[self.stage],
                            insert(out, self.stage, i),
                        )
                    )
                )
        return keys

    def __repr__(self):
        return "ShuffleLayer<name='{}', stage={}, nsplits={}, npartitions={}>".format(
            self.name, self.stage, self.nsplits, self.npartitions
        )

    def __reduce__(self):
        attrs = [
            "name",
            "column",
            "inputs",
            "stage",
            "npartitions",
            "npartitions_input",
            "nsplits",
            "ignore_index",
            "name_input",
            "meta_input",
            "parts_out",
            "annotations",
        ]

        return (ShuffleLayer, tuple(getattr(self, attr) for attr in attrs))

    def __dask_distributed_pack__(self, *args, **kwargs):
        ret = super().__dask_distributed_pack__(*args, **kwargs)
        ret["inputs"] = self.inputs
        ret["stage"] = self.stage
        ret["nsplits"] = self.nsplits
        return ret

    def _cull_dependencies(self, keys, parts_out=None):
        """Determine the necessary dependencies to produce `keys`.

        Does not require graph materialization.
        """
        deps = defaultdict(set)
        parts_out = parts_out or self._keys_to_parts(keys)
        inp_part_map = {inp: i for i, inp in enumerate(self.inputs)}
        for part in parts_out:
            out = self.inputs[part]
            for k in range(self.nsplits):
                _inp = insert(out, self.stage, k)
                _part = inp_part_map[_inp]
                if self.stage == 0 and _part >= self.npartitions_input:
                    deps[(self.name, part)].add(("group-" + self.name, _inp, "empty"))
                else:
                    deps[(self.name, part)].add((self.name_input, _part))
        return deps

    def _cull(self, parts_out):
        return ShuffleLayer(
            self.name,
            self.column,
            self.inputs,
            self.stage,
            self.npartitions,
            self.npartitions_input,
            self.nsplits,
            self.ignore_index,
            self.name_input,
            self.meta_input,
            parts_out=parts_out,
        )

    def _construct_graph(self, deserializing=False):
        """Construct graph for a "rearrange-by-column" stage."""

        shuffle_group_name = "group-" + self.name

        if deserializing:
            # Use CallableLazyImport objects to avoid importing dataframe
            # module on the scheduler
            concat_func = CallableLazyImport("dask.dataframe.core._concat")
            shuffle_group_func = CallableLazyImport(
                "dask.dataframe.shuffle.shuffle_group"
            )
        else:
            # Not running on distributed scheduler - Use explicit functions
            from dask.dataframe.core import _concat as concat_func
            from dask.dataframe.shuffle import shuffle_group as shuffle_group_func

        dsk = {}
        inp_part_map = {inp: i for i, inp in enumerate(self.inputs)}
        for part in self.parts_out:

            out = self.inputs[part]

            _concat_list = []  # get_item tasks to concat for this output partition
            for i in range(self.nsplits):
                # Get out each individual dataframe piece from the dicts
                _inp = insert(out, self.stage, i)
                _idx = out[self.stage]
                _concat_list.append((self.split_name, _idx, _inp))

            # concatenate those pieces together, with their friends
            dsk[(self.name, part)] = (
                concat_func,
                _concat_list,
                self.ignore_index,
            )

            for _, _idx, _inp in _concat_list:
                dsk[(self.split_name, _idx, _inp)] = (
                    operator.getitem,
                    (shuffle_group_name, _inp),
                    _idx,
                )

                if (shuffle_group_name, _inp) not in dsk:

                    # Initial partitions (output of previous stage)
                    _part = inp_part_map[_inp]
                    if self.stage == 0:
                        if _part < self.npartitions_input:
                            input_key = (self.name_input, _part)
                        else:
                            # In order to make sure that to_serialize() serialize the
                            # empty dataframe input, we add it as a key.
                            input_key = (shuffle_group_name, _inp, "empty")
                            dsk[input_key] = self.meta_input
                    else:
                        input_key = (self.name_input, _part)

                    # Convert partition into dict of dataframe pieces
                    dsk[(shuffle_group_name, _inp)] = (
                        shuffle_group_func,
                        input_key,
                        self.column,
                        self.stage,
                        self.nsplits,
                        self.npartitions_input,
                        self.ignore_index,
                        self.npartitions,
                    )

        return dsk


class BroadcastJoinLayer(DataFrameLayer):
    """Broadcast-based Join Layer

    High-level graph layer for a join operation requiring the
    smaller collection to be broadcasted to every partition of
    the larger collection.

    Parameters
    ----------
    name : str
        Name of new (joined) output collection.
    lhs_name: string
        "Left" DataFrame collection to join.
    lhs_npartitions: int
        Number of partitions in "left" DataFrame collection.
    rhs_name: string
        "Right" DataFrame collection to join.
    rhs_npartitions: int
        Number of partitions in "right" DataFrame collection.
    parts_out : list of int (optional)
        List of required output-partition indices.
    annotations : dict (optional)
        Layer annotations.
    **merge_kwargs : **dict
        Keyword arguments to be passed to chunkwise merge func.
    """

    def __init__(
        self,
        name,
        npartitions,
        lhs_name,
        lhs_npartitions,
        rhs_name,
        rhs_npartitions,
        parts_out=None,
        annotations=None,
        **merge_kwargs,
    ):
        super().__init__(annotations=annotations)
        self.name = name
        self.npartitions = npartitions
        self.lhs_name = lhs_name
        self.lhs_npartitions = lhs_npartitions
        self.rhs_name = rhs_name
        self.rhs_npartitions = rhs_npartitions
        self.parts_out = parts_out or set(range(self.npartitions))
        self.merge_kwargs = merge_kwargs
        self.how = self.merge_kwargs.get("how")
        self.left_on = self.merge_kwargs.get("left_on")
        self.right_on = self.merge_kwargs.get("right_on")
        if isinstance(self.left_on, list):
            self.left_on = (list, tuple(self.left_on))
        if isinstance(self.right_on, list):
            self.right_on = (list, tuple(self.right_on))

    def get_output_keys(self):
        return {(self.name, part) for part in self.parts_out}

    def __repr__(self):
        return "BroadcastJoinLayer<name='{}', how={}, lhs={}, rhs={}>".format(
            self.name, self.how, self.lhs_name, self.rhs_name
        )

    def is_materialized(self):
        return hasattr(self, "_cached_dict")

    @property
    def _dict(self):
        """Materialize full dict representation"""
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __dask_distributed_pack__(self, *args, **kwargs):
        import pickle

        # Pickle complex merge_kwargs elements. Also
        # tuples, which may be confused with keys.
        _merge_kwargs = {}
        for k, v in self.merge_kwargs.items():
            if not isinstance(v, (str, list, bool)):
                _merge_kwargs[k] = pickle.dumps(v)
            else:
                _merge_kwargs[k] = v

        return {
            "name": self.name,
            "npartitions": self.npartitions,
            "lhs_name": self.lhs_name,
            "lhs_npartitions": self.lhs_npartitions,
            "rhs_name": self.rhs_name,
            "rhs_npartitions": self.rhs_npartitions,
            "parts_out": self.parts_out,
            "merge_kwargs": _merge_kwargs,
        }

    @classmethod
    def __dask_distributed_unpack__(cls, state, dsk, dependencies):
        from distributed.worker import dumps_task

        # Expand merge_kwargs
        merge_kwargs = state.pop("merge_kwargs", {})
        state.update(merge_kwargs)

        # Materialize the layer
        raw = cls(**state)._construct_graph(deserializing=True)

        # Convert all keys to strings and dump tasks
        raw = {stringify(k): stringify_collection_keys(v) for k, v in raw.items()}
        keys = raw.keys() | dsk.keys()
        deps = {k: keys_in_tasks(keys, [v]) for k, v in raw.items()}

        return {"dsk": toolz.valmap(dumps_task, raw), "deps": deps}

    def _keys_to_parts(self, keys):
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            try:
                _name, _part = key
            except ValueError:
                continue
            if _name != self.name:
                continue
            parts.add(_part)
        return parts

    @property
    def _broadcast_plan(self):
        # Return structure (tuple):
        # (
        #     <broadcasted-collection-name>,
        #     <broadcasted-collection-npartitions>,
        #     <other-collection-npartitions>,
        #     <other-collection-on>,
        # )
        if self.lhs_npartitions < self.rhs_npartitions:
            # Broadcasting the left
            return (
                self.lhs_name,
                self.lhs_npartitions,
                self.rhs_name,
                self.right_on,
            )
        else:
            # Broadcasting the right
            return (
                self.rhs_name,
                self.rhs_npartitions,
                self.lhs_name,
                self.left_on,
            )

    def _cull_dependencies(self, keys, parts_out=None):
        """Determine the necessary dependencies to produce `keys`.

        For a broadcast join, output partitions always depend on
        all partitions of the broadcasted collection, but only one
        partition of the "other" collecction.
        """
        # Get broadcast info
        bcast_name, bcast_size, other_name = self._broadcast_plan[:3]

        deps = defaultdict(set)
        parts_out = parts_out or self._keys_to_parts(keys)
        for part in parts_out:
            deps[(self.name, part)] |= {(bcast_name, i) for i in range(bcast_size)}
            deps[(self.name, part)] |= {
                (other_name, part),
            }
        return deps

    def _cull(self, parts_out):
        return BroadcastJoinLayer(
            self.name,
            self.npartitions,
            self.lhs_name,
            self.lhs_npartitions,
            self.rhs_name,
            self.rhs_npartitions,
            annotations=self.annotations,
            parts_out=parts_out,
            **self.merge_kwargs,
        )

    def cull(self, keys, all_keys):
        """Cull a BroadcastJoinLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indicies) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        culled_deps = self._cull_dependencies(keys, parts_out=parts_out)
        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def _construct_graph(self, deserializing=False):
        """Construct graph for a broadcast join operation."""

        inter_name = "inter-" + self.name
        split_name = "split-" + self.name

        if deserializing:
            # Use CallableLazyImport objects to avoid importing dataframe
            # module on the scheduler
            split_partition_func = CallableLazyImport(
                "dask.dataframe.multi._split_partition"
            )
            concat_func = CallableLazyImport("dask.dataframe.multi._concat_wrapper")
            merge_chunk_func = CallableLazyImport(
                "dask.dataframe.multi._merge_chunk_wrapper"
            )
        else:
            # Not running on distributed scheduler - Use explicit functions
            from dask.dataframe.multi import _concat_wrapper as concat_func
            from dask.dataframe.multi import _merge_chunk_wrapper as merge_chunk_func
            from dask.dataframe.multi import _split_partition as split_partition_func

        # Get broadcast "plan"
        bcast_name, bcast_size, other_name, other_on = self._broadcast_plan
        bcast_side = "left" if self.lhs_npartitions < self.rhs_npartitions else "right"

        # Loop over output partitions, which should be a 1:1
        # mapping with the input partitions of "other".
        # Culling should allow us to avoid generating tasks for
        # any output partitions that are not requested (via `parts_out`)
        dsk = {}
        for i in self.parts_out:

            # Split each "other" partition by hash
            if self.how != "inner":
                dsk[(split_name, i)] = (
                    split_partition_func,
                    (other_name, i),
                    other_on,
                    bcast_size,
                )

            # For each partition of "other", we need to join
            # to each partition of "bcast". If it is a "left"
            # or "right" join, there should be a unique mapping
            # between the local splits of "other" and the
            # partitions of "bcast" (which means we need an
            # additional `getitem` operation to isolate the
            # correct split of each "other" partition).
            _concat_list = []
            for j in range(bcast_size):
                # Specify arg list for `merge_chunk`
                _merge_args = [
                    (
                        operator.getitem,
                        (split_name, i),
                        j,
                    )
                    if self.how != "inner"
                    else (other_name, i),
                    (bcast_name, j),
                ]
                if bcast_side == "left":
                    # If the left is broadcasted, the
                    # arg list needs to be reversed
                    _merge_args.reverse()
                inter_key = (inter_name, i, j)
                dsk[inter_key] = (
                    apply,
                    merge_chunk_func,
                    _merge_args,
                    self.merge_kwargs,
                )
                _concat_list.append(inter_key)

            # Concatenate the merged results for each output partition
            dsk[(self.name, i)] = (concat_func, _concat_list)

        return dsk


class DataFrameIOLayer(Blockwise, DataFrameLayer):
    """DataFrame-based Blockwise Layer with IO

    Parameters
    ----------
    name : str
        Name to use for the constructed layer.
    columns : str, list or None
        Field name(s) to read in as columns in the output.
    inputs : list[tuple]
        List of arguments to be passed to ``io_func`` so
        that the materialized task to produce partition ``i``
        will be: ``(<io_func>, inputs[i])``.  Note that each
        element of ``inputs`` is typically a tuple of arguments.
    io_func : callable
        A callable function that takes in a single tuple
        of arguments, and outputs a DataFrame partition.
    label : str (optional)
        String to use as a prefix in the place-holder collection
        name. If nothing is specified (default), "subset-" will
        be used.
    produces_tasks : bool (optional)
        Whether one or more elements of `inputs` is expected to
        contain a nested task. This argument in only used for
        serialization purposes, and will be deprecated in the
        future. Default is False.
    annotations: dict (optional)
        Layer annotations to pass through to Blockwise.
    """

    def __init__(
        self,
        name,
        columns,
        inputs,
        io_func,
        label=None,
        produces_tasks=False,
        annotations=None,
    ):
        self.name = name
        self.columns = columns
        self.inputs = inputs
        self.io_func = io_func
        self.label = label
        self.produces_tasks = produces_tasks
        self.annotations = annotations

        # Define mapping between key index and "part"
        io_arg_map = BlockwiseDepDict(
            {(i,): inp for i, inp in enumerate(self.inputs)},
            produces_tasks=self.produces_tasks,
        )

        # Use Blockwise initializer
        dsk = {self.name: (io_func, blockwise_token(0))}
        super().__init__(
            output=self.name,
            output_indices="i",
            dsk=dsk,
            indices=[(io_arg_map, "i")],
            numblocks={},
            annotations=annotations,
        )

    def project_columns(self, columns):
        # Method inherited from `DataFrameLayer.project_columns`
        if columns and (self.columns is None or columns < set(self.columns)):

            # Apply column projection in IO function
            try:
                io_func = self.io_func.project_columns(list(columns))
            except AttributeError:
                io_func = self.io_func

            layer = DataFrameIOLayer(
                (self.label or "subset-") + tokenize(self.name, columns),
                list(columns),
                self.inputs,
                io_func,
                produces_tasks=self.produces_tasks,
                annotations=self.annotations,
            )
            return layer, None
        else:
            # Default behavior
            return self, None

    def __repr__(self):
        return "DataFrameIOLayer<name='{}', n_parts={}, columns={}>".format(
            self.name, len(self.inputs), self.columns
        )
