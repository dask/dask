import functools
import math
import operator
import uuid

import numpy as np
import pandas as pd
import tlz as toolz
from dask import compute
from dask.dataframe.core import _concat, make_meta
from dask.dataframe.shuffle import (
    barrier,
    collect,
    ensure_cleanup_on_exception,
    maybe_buffered_partd,
    partitioning_index,
    set_partitions_pre,
    shuffle_group,
    shuffle_group_2,
    shuffle_group_get,
)
from dask.utils import M, digit, get_default_shuffle_method, insert

from dask_expr._expr import Assign, Blockwise, Expr, PartitionsFiltered, Projection
from dask_expr._reductions import (
    All,
    Any,
    Count,
    DropDuplicates,
    Len,
    Max,
    Mean,
    MemoryUsage,
    Min,
    Mode,
    NBytes,
    NLargest,
    NSmallest,
    Prod,
    Size,
    Sum,
    Unique,
    ValueCounts,
)
from dask_expr._repartition import Repartition


class Shuffle(Expr):
    """Abstract shuffle class

    Parameters
    ----------
    frame: Expr
        The DataFrame-like expression to shuffle.
    partitioning_index: str, list
        Column and/or index names to hash and partition by.
    npartitions: int
        Number of output partitions.
    ignore_index: bool
        Whether to ignore the index during this shuffle operation.
    backend: str or Callable
        Label or callback funcition to convert a shuffle operation
        to its necessary components.
    options: dict
        Algorithm-specific options.
    """

    _parameters = [
        "frame",
        "partitioning_index",
        "npartitions_out",
        "ignore_index",
        "backend",
        "options",
    ]
    _defaults = {
        "ignore_index": False,
        "backend": None,
        "options": None,
    }

    def __str__(self):
        return f"Shuffle({self._name[-7:]})"

    def _node_label_args(self):
        return [self.frame, self.partitioning_index]

    def _lower(self):
        # Use `backend` to decide how to compose a
        # shuffle operation from concerete expressions
        backend = self.backend or get_default_shuffle_method()
        if hasattr(backend, "from_abstract_shuffle"):
            return backend.from_abstract_shuffle(self)
        elif backend == "p2p":
            return P2PShuffle.from_abstract_shuffle(self)
        elif backend == "disk":
            return DiskShuffle.from_abstract_shuffle(self)
        elif backend == "simple":
            return SimpleShuffle.from_abstract_shuffle(self)
        elif backend == "tasks":
            return TaskShuffle.from_abstract_shuffle(self)
        else:
            raise ValueError(f"{backend} not supported")

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            # Move the column projection to come
            # before the abstract Shuffle
            projection = parent.operand("columns")
            if isinstance(projection, (str, int)):
                projection = [projection]

            partitioning_index = self.partitioning_index
            if isinstance(partitioning_index, (str, int)):
                partitioning_index = [partitioning_index]

            target = self.frame
            new_projection = [
                col
                for col in target.columns
                if (col in partitioning_index or col in projection)
            ]
            if set(new_projection) < set(target.columns):
                return type(self)(target[new_projection], *self.operands[1:])[
                    parent.operand("columns")
                ]

        if isinstance(
            parent,
            (
                Unique,
                DropDuplicates,
                Sum,
                Prod,
                Max,
                Any,
                All,
                Min,
                Len,
                Size,
                NBytes,
                Mean,
                Count,
                Mode,
                NLargest,
                NSmallest,
                ValueCounts,
                MemoryUsage,
            ),
        ):
            return type(parent)(self.frame, *parent.operands[1:])

    def _layer(self):
        raise NotImplementedError(
            f"{self} is abstract! Please call `simplify`"
            f"before generating a task graph."
        )

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        return (None,) * (self.npartitions_out + 1)


#
# ShuffleBackend Implementations
#


class ShuffleBackend(Shuffle):
    """Base shuffle-backend class"""

    _parameters = [
        "frame",
        "partitioning_index",
        "npartitions_out",
        "ignore_index",
        "options",
        "_partitions",
    ]

    _defaults = {"_partitions": None}

    @classmethod
    def from_abstract_shuffle(cls, expr: Shuffle) -> Expr:
        """Create an Expr tree that uses this ShuffleBackend class"""
        raise NotImplementedError()

    def _lower(self):
        return None


class SimpleShuffle(PartitionsFiltered, ShuffleBackend):
    """Simple task-based shuffle implementation"""

    lazy_hash_support = True

    @classmethod
    def from_abstract_shuffle(cls, expr: Shuffle) -> Expr:
        frame = expr.frame
        partitioning_index = expr.partitioning_index
        npartitions_out = expr.npartitions_out
        ignore_index = expr.ignore_index
        options = expr.options

        # Normalize partitioning_index
        if isinstance(partitioning_index, str):
            partitioning_index = [partitioning_index]
        if not isinstance(partitioning_index, list):
            raise ValueError(
                f"{type(partitioning_index)} not a supported type for partitioning_index"
            )

        # Reduce partition count if necessary
        if npartitions_out < frame.npartitions:
            frame = Repartition(frame, n=npartitions_out)

        if cls.lazy_hash_support:
            # Don't need to assign "_partitions" column
            # if we are shuffling on a list of columns
            nset = set(partitioning_index)
            if nset & set(frame.columns) == nset:
                return cls(
                    frame,
                    partitioning_index,
                    npartitions_out,
                    ignore_index,
                    options,
                )

        if partitioning_index != ["_partitions"]:
            # Assign new "_partitions" column
            index_added = AssignPartitioningIndex(
                frame,
                partitioning_index,
                "_partitions",
                npartitions_out,
            )
        else:
            index_added = frame

        # Apply shuffle
        shuffled = cls(
            index_added,
            "_partitions",
            npartitions_out,
            ignore_index,
            options,
        )

        # Drop "_partitions" column and return
        return shuffled[[c for c in shuffled.columns if c != "_partitions"]]

    @staticmethod
    def _shuffle_group(df, _filter, *args):
        """Filter the output of `shuffle_group`"""
        if _filter is None:
            return shuffle_group(df, *args)
        return {k: v for k, v in shuffle_group(df, *args).items() if k in _filter}

    def _layer(self):
        """Construct graph for a simple shuffle operation."""
        shuffle_group_name = "group-" + self._name
        split_name = "split-" + self._name
        npartitions = self.npartitions_out

        dsk = {}
        _filter = self._partitions if self._filtered else None
        for global_part, part_out in enumerate(self._partitions):
            _concat_list = [
                (split_name, part_out, part_in)
                for part_in in range(self.frame.npartitions)
            ]
            dsk[(self._name, global_part)] = (
                _concat,
                _concat_list,
                self.ignore_index,
            )
            for _, _part_out, _part_in in _concat_list:
                dsk[(split_name, _part_out, _part_in)] = (
                    operator.getitem,
                    (shuffle_group_name, _part_in),
                    _part_out,
                )
                if (shuffle_group_name, _part_in) not in dsk:
                    dsk[(shuffle_group_name, _part_in)] = (
                        self._shuffle_group,
                        (self.frame._name, _part_in),
                        _filter,
                        self.partitioning_index,
                        0,
                        npartitions,
                        npartitions,
                        self.ignore_index,
                        npartitions,
                    )

        return dsk


class TaskShuffle(SimpleShuffle):
    """Staged task-based shuffle implementation"""

    def _layer(self):
        max_branch = (self.options or {}).get("max_branch", 32)
        npartitions_input = self.frame.npartitions
        if len(self._partitions) <= max_branch or npartitions_input <= max_branch:
            # We are creating a small number of output partitions,
            # or starting with a small number of input partitions.
            # No need for staged shuffling. Staged shuffling will
            # sometimes require extra work/communication in this case.
            return super()._layer()

        # Calculate number of stages and splits per stage
        npartitions = self.npartitions_out
        stages = int(math.ceil(math.log(npartitions_input) / math.log(max_branch)))
        if stages > 1:
            nsplits = int(math.ceil(npartitions_input ** (1 / stages)))
        else:
            nsplits = npartitions_input

        # Construct global data-movement plan
        inputs = [
            tuple(digit(i, j, nsplits) for j in range(stages))
            for i in range(nsplits**stages)
        ]
        inp_part_map = {inp: i for i, inp in enumerate(inputs)}
        parts_out = range(len(inputs))

        # Build graph
        dsk = {}
        name = self.frame._name
        meta_input = make_meta(self.frame._meta)
        for stage in range(stages):
            # Define names
            name_input = name
            if stage == (stages - 1) and npartitions == npartitions_input:
                name = self._name
                parts_out = self._partitions
                _filter = parts_out if self._filtered else None
            else:
                name = f"stage-{stage}-{self._name}"
                _filter = None

            shuffle_group_name = "group-" + name
            split_name = "split-" + name

            for global_part, part in enumerate(parts_out):
                out = inputs[part]

                _concat_list = []  # get_item tasks to concat for this output partition
                for i in range(nsplits):
                    # Get out each individual dataframe piece from the dicts
                    _inp = insert(out, stage, i)
                    _idx = out[stage]
                    _concat_list.append((split_name, _idx, _inp))

                # concatenate those pieces together, with their friends
                dsk[(name, global_part)] = (
                    _concat,
                    _concat_list,
                    self.ignore_index,
                )

                for _, _idx, _inp in _concat_list:
                    dsk[(split_name, _idx, _inp)] = (
                        operator.getitem,
                        (shuffle_group_name, _inp),
                        _idx,
                    )

                    if (shuffle_group_name, _inp) not in dsk:
                        # Initial partitions (output of previous stage)
                        _part = inp_part_map[_inp]
                        if stage == 0:
                            if _part < npartitions_input:
                                input_key = (name_input, _part)
                            else:
                                # In order to make sure that to_serialize() serialize the
                                # empty dataframe input, we add it as a key.
                                input_key = (shuffle_group_name, _inp, "empty")
                                dsk[input_key] = meta_input
                        else:
                            input_key = (name_input, _part)

                        # Convert partition into dict of dataframe pieces
                        dsk[(shuffle_group_name, _inp)] = (
                            self._shuffle_group,
                            input_key,
                            _filter,
                            self.partitioning_index,
                            stage,
                            nsplits,
                            npartitions_input,
                            self.ignore_index,
                            npartitions,
                        )

        if npartitions != npartitions_input:
            repartition_group_name = "repartition-group-" + name

            dsk2 = {
                (repartition_group_name, i): (
                    shuffle_group_2,
                    (name, i),
                    self.partitioning_index,
                    self.ignore_index,
                    npartitions,
                )
                for i in range(npartitions_input)
            }

            for i, p in enumerate(self._partitions):
                dsk2[(self._name, i)] = (
                    shuffle_group_get,
                    (repartition_group_name, p % npartitions_input),
                    p,
                )

            dsk.update(dsk2)
        return dsk


class DiskShuffle(SimpleShuffle):
    """Disk-based shuffle implementation"""

    lazy_hash_support = False

    @staticmethod
    def _shuffle_group(df, col, _filter, p):
        with ensure_cleanup_on_exception(p):
            g = df.groupby(col)
            d = {i: g.get_group(i) for i in g.groups if i in _filter}
            p.append(d, fsync=True)

    def _layer(self):
        column = self.partitioning_index
        df = self.frame

        always_new_token = uuid.uuid1().hex

        p = ("zpartd-" + always_new_token,)
        dsk1 = {p: (maybe_buffered_partd(),)}

        # Partition data on disk
        name = "shuffle-partition-" + always_new_token
        dsk2 = {
            (name, i): (self._shuffle_group, key, column, self._partitions, p)
            for i, key in enumerate(df.__dask_keys__())
        }

        # Barrier
        barrier_token = "barrier-" + always_new_token
        dsk3 = {barrier_token: (barrier, list(dsk2))}

        # Collect groups
        dsk4 = {
            (self._name, j): (collect, p, k, df._meta, barrier_token)
            for j, k in enumerate(self._partitions)
        }

        return toolz.merge(dsk1, dsk2, dsk3, dsk4)


class P2PShuffle(SimpleShuffle):
    """P2P worker-based shuffle implementation"""

    lazy_hash_support = False

    def _layer(self):
        from distributed.shuffle._shuffle import (
            ShuffleId,
            barrier_key,
            shuffle_barrier,
            shuffle_transfer,
            shuffle_unpack,
        )

        dsk = {}
        token = self._name.split("-")[-1]
        _barrier_key = barrier_key(ShuffleId(token))
        name = "shuffle-transfer-" + token
        transfer_keys = list()
        parts_out = (
            self._partitions if self._filtered else list(range(self.npartitions_out))
        )
        for i in range(self.frame.npartitions):
            transfer_keys.append((name, i))
            dsk[(name, i)] = (
                shuffle_transfer,
                (self.frame._name, i),
                token,
                i,
                self.npartitions_out,
                self.partitioning_index,
                set(parts_out),
            )

        dsk[_barrier_key] = (shuffle_barrier, token, transfer_keys)

        # TODO: Decompose p2p Into transfer/barrier + unpack
        name = self._name
        for i, part_out in enumerate(parts_out):
            dsk[(name, i)] = (
                shuffle_unpack,
                token,
                part_out,
                _barrier_key,
                self.frame._meta,
            )
        return dsk


#
# Helper logic
#


def _select_columns_or_index(df, columns_or_index):
    """
    Make a column selection that may include the index

    Parameters
    ----------
    columns_or_index
        Column or index name, or a list of these
    """

    # Ensure columns_or_index is a list
    columns_or_index = (
        columns_or_index if isinstance(columns_or_index, list) else [columns_or_index]
    )

    column_names = [n for n in columns_or_index if _is_column_label_reference(df, n)]

    selected_df = df[column_names]
    if _contains_index_name(df, columns_or_index):
        # Index name was included
        selected_df = selected_df.assign(_index=df.index)

    return selected_df


def _is_column_label_reference(df, key):
    """
    Test whether a key is a column label reference

    To be considered a column label reference, `key` must match the name of at
    least one column.
    """
    return (
        not isinstance(key, Expr)
        and (np.isscalar(key) or isinstance(key, tuple))
        and key in df.columns
    )


def _contains_index_name(df, columns_or_index):
    """
    Test whether the input contains a reference to the index of the df
    """
    if isinstance(columns_or_index, list):
        return any(_is_index_level_reference(df, n) for n in columns_or_index)
    else:
        return _is_index_level_reference(df, columns_or_index)


def _is_index_level_reference(df, key):
    """
    Test whether a key is an index level reference

    To be considered an index level reference, `key` must match the index name
    and must NOT match the name of any column.
    """
    index_name = df.index._meta.name if isinstance(df, Expr) else df.index.name
    return (
        index_name is not None
        and not isinstance(key, Expr)
        and (np.isscalar(key) or isinstance(key, tuple))
        and key == index_name
        and key not in getattr(df, "columns", ())
    )


class AssignPartitioningIndex(Blockwise):
    """Assign a partitioning index

    This class is used to construct a hash-based
    partitioning index for shuffling.

    Parameters
    ----------
    frame: Expr
        Frame-like expression being partitioned.
    partitioning_index: Expr or list
        Index-like expression or list of columns to construct
        the partitioning-index from.
    index_name: str
        New column name to assign.
    npartitions_out: int
        Number of partitions after repartitioning is finished.
    """

    _parameters = ["frame", "partitioning_index", "index_name", "npartitions_out"]

    @classmethod
    def operation(cls, df, index, name: str, npartitions: int):
        """Construct a hash-based partitioning index"""
        index = _select_columns_or_index(df, index)
        if isinstance(index, (str, list, tuple)):
            # Assume column selection from df
            index = [index] if isinstance(index, str) else list(index)
            index = partitioning_index(df[index], npartitions)
        else:
            index = partitioning_index(index, npartitions)
        return df.assign(**{name: index})


class BaseSetIndexSortValues(Expr):
    def _divisions(self):
        if self.user_divisions is not None:
            return self.user_divisions
        divisions, mins, maxes, presorted = _calculate_divisions(
            self.frame,
            self.other,
            self.npartitions,
            self.ascending,
            upsample=self.upsample,
        )
        if presorted:
            divisions = mins.copy() + [maxes[-1]]
        return divisions

    @property
    def npartitions(self):
        return self.operand("npartitions") or self.frame.npartitions


class SetIndex(BaseSetIndexSortValues):
    """Abstract ``set_index`` class.

    Simplifies (later lowers) either to Blockwise ops if we are already sorted
    or to ``SetPartition`` which handles shuffling.

    Parameters
    ----------
    frame: Expr
        Frame-like expression where the index is set.
    _other: Expr | Scalar
        Either a Series-like expression to use as Index or a scalar defining the column.
    drop: bool
        Whether we drop the old column.
    sorted: str
        No need for shuffling if we are already sorted.
    user_divisions: int
        Divisions as passed by the user.
    upsample: float
        Used to increase the number of samples for quantiles.
    """

    _parameters = [
        "frame",
        "_other",
        "drop",
        "user_divisions",
        "partition_size",
        "ascending",
        "npartitions",
        "upsample",
    ]
    _defaults = {
        "drop": True,
        "user_divisions": None,
        "partition_size": 128e6,
        "ascending": True,
        "npartitions": None,
        "upsample": 1.0,
    }

    def _divisions(self):
        if self.user_divisions is not None:
            return self.user_divisions
        divisions, mins, maxes, presorted = _calculate_divisions(
            self.frame,
            self.other,
            self.npartitions,
            self.ascending,
            upsample=self.upsample,
        )
        if presorted:
            divisions = mins.copy() + [maxes[-1]]
        return divisions

    @property
    def npartitions(self):
        if self.operand("npartitions") is not None:
            return self.operand("npartitions")
        return self.frame.npartitions

    @property
    def _meta(self):
        if isinstance(self._other, Expr):
            other = self._other._meta
        else:
            other = self._other
        return self.frame._meta.set_index(other, drop=self.drop)

    @property
    def other(self):
        if isinstance(self._other, Expr):
            return self._other
        return self.frame[self._other]

    def _lower(self):
        if self.user_divisions is None:
            divisions = self._divisions()
            presorted = _calculate_divisions(
                self.frame,
                self.other,
                self.npartitions,
                self.ascending,
                upsample=self.upsample,
            )[3]

            if presorted and self.npartitions == self.frame.npartitions:
                index_set = SetIndexBlockwise(
                    self.frame, self._other, self.drop, divisions
                )
                return SortIndexBlockwise(index_set)

        else:
            divisions = self.user_divisions

        return SetPartition(self.frame, self._other, self.drop, divisions)

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = parent.columns + (
                [self._other] if not isinstance(self._other, Expr) else []
            )
            if self.frame.columns == columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], *self.operands[1:]),
                parent.operand("columns"),
            )


class SortValues(BaseSetIndexSortValues):
    _parameters = [
        "frame",
        "by",
        "ascending",
        "na_position",
        "npartitions",
        "partition_size",
        "sort_function",
        "sort_function_kwargs",
        "upsample",
    ]
    _defaults = {
        "partition_size": 128e6,
        "ascending": True,
        "npartitions": None,
        "na_position": "last",
        "sort_function": None,
        "sort_function_kwargs": None,
        "upsample": 1.0,
    }

    @property
    def sort_function(self):
        if self.operand("sort_function") is not None:
            return self.operand("sort_function")
        return M.sort_values

    @property
    def sort_function_kwargs(self):
        sort_kwargs = {
            "by": self.by,
            "ascending": self.ascending,
            "na_position": self.na_position,
        }
        if self.operand("sort_function_kwargs") is not None:
            sort_kwargs.update(self.operand("sort_function_kwargs"))
        return sort_kwargs

    @property
    def _meta(self):
        return self.frame._meta

    def _lower(self):
        by = self.frame[self.by[0]]
        divisions, _, _, presorted = _calculate_divisions(
            self.frame, by, self.npartitions, self.ascending, upsample=self.upsample
        )
        if presorted and self.npartitions == self.frame.npartitions:
            return SortValuesBlockwise(
                self.frame, self.sort_function, self.sort_function_kwargs
            )

        partitions = _SetPartitionsPreSetIndex(by, by._meta._constructor(divisions))
        assigned = Assign(self.frame, "_partitions", partitions)
        shuffled = Shuffle(
            assigned,
            "_partitions",
            npartitions_out=len(divisions) - 1,
            ignore_index=True,
        )
        return SortValuesBlockwise(
            shuffled, self.sort_function, self.sort_function_kwargs
        )

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            parent_columns = parent.columns
            columns = parent_columns + [
                col for col in self.by if col not in parent_columns
            ]
            if self.frame.columns == columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], *self.operands[1:]),
                parent.operand("columns"),
            )


class SetPartition(SetIndex):
    """Shuffles the DataFrame according to its new divisions.

    Simplifies the Expression to blockwise pre-processing, shuffle and
    blockwise post-processing expressions.

    Parameters
    ----------
    frame: Expr
        Frame-like expression where the index is set.
    _other: Expr | Scalar
        Either a Series-like expression to use as Index or a scalar defining the column.
    drop: bool
        Whether to drop the old column.
    new_divisions: int
        Divisions of the resulting expression.
    """

    _parameters = ["frame", "_other", "drop", "new_divisions"]

    def _divisions(self):
        return self.new_divisions

    @functools.cached_property
    def new_divisions(self):
        # TODO: Adjust for categoricals and NA values
        return self.other._meta._constructor(self.operand("new_divisions"))

    def _lower(self):
        partitions = _SetPartitionsPreSetIndex(self.other, self.new_divisions)
        assigned = Assign(self.frame, "_partitions", partitions)
        if isinstance(self._other, Expr):
            assigned = Assign(assigned, "_index", self._other)
        shuffled = Shuffle(
            assigned,
            "_partitions",
            npartitions_out=len(self.new_divisions) - 1,
            ignore_index=True,
        )

        if isinstance(self._other, Expr):
            drop, set_name = True, "_index"
        else:
            drop, set_name = self.drop, self.other._meta.name
        index_set = _SetIndexPost(
            shuffled, self.other._meta.name, drop=drop, set_name=set_name
        )
        return SortIndexBlockwise(index_set)


class _SetPartitionsPreSetIndex(Blockwise):
    _parameters = ["frame", "new_divisions", "ascending", "na_position"]
    _defaults = {"ascending": True, "na_position": "last"}
    operation = staticmethod(set_partitions_pre)

    @property
    def _meta(self):
        return self.frame._meta._constructor([0])


class _SetIndexPost(Blockwise):
    _parameters = ["frame", "index_name", "drop", "set_name"]

    def operation(self, df, index_name, drop, set_name):
        return df.set_index(set_name, drop=drop).rename_axis(index=index_name)


class SortIndexBlockwise(Blockwise):
    _projection_passthrough = True
    _parameters = ["frame"]
    operation = M.sort_index


def sort_function(self, *args, **kwargs):
    sort_func = kwargs.pop("sort_function")
    sort_kwargs = kwargs.pop("sort_kwargs")
    return sort_func(*args, **kwargs, **sort_kwargs)


class SortValuesBlockwise(Blockwise):
    _projection_passthrough = False
    _parameters = ["frame", "sort_function", "sort_kwargs"]
    operation = sort_function
    _keyword_only = ["sort_function", "sort_kwargs"]


class SetIndexBlockwise(Blockwise):
    _parameters = ["frame", "other", "drop", "new_divisions"]
    _keyword_only = ["drop", "new_divisions"]

    def operation(self, df, *args, new_divisions, **kwargs):
        return df.set_index(*args, **kwargs)

    def _divisions(self):
        if self.new_divisions is None:
            return (None,) * (self.frame.npartitions + 1)
        return tuple(self.new_divisions)

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = parent.columns + (
                [self.other] if not isinstance(self.other, Expr) else []
            )
            if self.frame.columns == columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], *self.operands[1:]),
                parent.operand("columns"),
            )


@functools.lru_cache  # noqa: B019
def _calculate_divisions(
    frame,
    other,
    npartitions: int,
    ascending: bool = True,
    partition_size: float = 128e6,
    upsample: float = 1.0,
):
    from dask_expr import RepartitionQuantiles, new_collection

    divisions, mins, maxes = compute(
        new_collection(RepartitionQuantiles(other, npartitions, upsample=upsample)),
        new_collection(other).map_partitions(M.min),
        new_collection(other).map_partitions(M.max),
    )
    sizes = []

    empty_dataframe_detected = pd.isna(divisions).all()
    if empty_dataframe_detected:
        total = sum(sizes)
        npartitions = max(math.ceil(total / partition_size), 1)
        npartitions = min(npartitions, frame.npartitions)
        n = divisions.size
        try:
            divisions = np.interp(
                x=np.linspace(0, n - 1, npartitions + 1),
                xp=np.linspace(0, n - 1, n),
                fp=divisions.tolist(),
            ).tolist()
        except (TypeError, ValueError):  # str type
            indexes = np.linspace(0, n - 1, npartitions + 1).astype(int)
            divisions = divisions.iloc[indexes].tolist()
    else:
        # Drop duplicate divisions returned by partition quantiles
        n = divisions.size
        divisions = (
            list(divisions.iloc[: n - 1].unique()) + divisions.iloc[n - 1 :].tolist()
        )

    mins = mins.bfill()
    maxes = maxes.bfill()
    if isinstance(other._meta.dtype, pd.CategoricalDtype):
        dtype = other._meta.dtype
        mins = mins.astype(dtype)
        maxes = maxes.astype(dtype)

    if mins.isna().any() or maxes.isna().any():
        presorted = False
    else:
        n = mins.size
        maxes2 = (maxes.iloc[: n - 1] if ascending else maxes.iloc[1:]).reset_index(
            drop=True
        )
        mins2 = (mins.iloc[1:] if ascending else mins.iloc[: n - 1]).reset_index(
            drop=True
        )
        presorted = (
            mins.tolist() == mins.sort_values(ascending=ascending).tolist()
            and maxes.tolist() == maxes.sort_values(ascending=ascending).tolist()
            and (maxes2 < mins2).all()
        )
    return divisions, mins.tolist(), maxes.tolist(), presorted
