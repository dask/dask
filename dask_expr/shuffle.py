import math
import operator
import uuid

import numpy as np
import tlz as toolz
from dask.dataframe.core import _concat, make_meta
from dask.dataframe.shuffle import (
    barrier,
    collect,
    ensure_cleanup_on_exception,
    maybe_buffered_partd,
    partitioning_index,
    shuffle_group,
    shuffle_group_2,
    shuffle_group_get,
)
from dask.utils import digit, get_default_shuffle_method, insert

from dask_expr.expr import Blockwise, Expr, PartitionsFiltered, Projection
from dask_expr.reductions import (
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
from dask_expr.repartition import Repartition


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

    def _simplify_down(self):
        # Use `backend` to decide how to compose a
        # shuffle operation from concerete expressions
        # TODO: Support "p2p"
        backend = self.backend or get_default_shuffle_method()
        backend = "tasks" if backend == "p2p" else backend
        if hasattr(backend, "from_abstract_shuffle"):
            return backend.from_abstract_shuffle(self)
        elif backend == "disk":
            return DiskShuffle.from_abstract_shuffle(self)
        elif backend == "simple":
            return SimpleShuffle.from_abstract_shuffle(self)
        elif backend == "tasks":
            return TaskShuffle.from_abstract_shuffle(self)
        else:
            # Only support task-based shuffling for now
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

    def _simplify_down(self):
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

        # Assign new "_partitions" column
        index_added = AssignPartitioningIndex(
            frame,
            partitioning_index,
            "_partitions",
            npartitions_out,
        )

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

        npartitions = self.npartitions_out
        if npartitions is None:
            npartitions = df.npartitions

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
