import operator
import numpy as np
import math
import tlz as toolz
import uuid

from dask.dataframe.core import _concat, make_meta
from dask.dataframe.shuffle import (
    barrier,
    collect,
    maybe_buffered_partd,
    partitioning_index,
    shuffle_group,
    shuffle_group_2,
    shuffle_group_3,
    shuffle_group_get,
)
from dask.utils import digit, insert, get_default_shuffle_algorithm

from dask_match.expr import Assign, Expr, Blockwise


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

    def __str__(self):
        return f"Shuffle({self._name[-7:]})"

    def _simplify_down(self):
        # Use `backend` to decide how to compose a
        # shuffle operation from concerete expressions
        # TODO: Support "p2p"
        backend = self.backend or get_default_shuffle_algorithm()
        backend = "tasks" if backend == "p2p" else backend
        if isinstance(backend, ShuffleBackend):
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
    ]

    @classmethod
    def from_abstract_shuffle(cls, expr: Shuffle) -> Expr:
        """Create an Expr tree that uses this ShuffleBackend class"""
        raise NotImplementedError()

    def _simplify_down(self):
        return None


class SimpleShuffle(ShuffleBackend):
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
            from dask_match.repartition import ReducePartitionCount

            frame = ReducePartitionCount(frame, npartitions_out)

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

        # Assign partitioning-index as a new "_partitions" column
        partitioning_index = _select_columns_or_index(frame, partitioning_index)
        index_added = Assign(
            frame,
            "_partitions",
            PartitioningIndex(frame, partitioning_index, npartitions_out),
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

    def _layer(self):
        """Construct graph for a simple shuffle operation."""
        shuffle_group_name = "group-" + self._name
        split_name = "split-" + self._name

        dsk = {}
        for part_out in range(self.npartitions):
            _concat_list = [
                (split_name, part_out, part_in)
                for part_in in range(self.frame.npartitions)
            ]
            dsk[(self._name, part_out)] = (
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
                        shuffle_group,
                        (self.frame._name, _part_in),
                        self.partitioning_index,
                        0,
                        self.npartitions,
                        self.npartitions,
                        self.ignore_index,
                        self.npartitions,
                    )

        return dsk


class TaskShuffle(SimpleShuffle):
    """Staged task-based shuffle implementation"""

    def _layer(self):
        max_branch = self.options.get("max_branch", 32)
        npartitions = self.npartitions_out
        if npartitions <= max_branch:
            # We are creating a small number of output partitions.
            # No need for staged shuffling. Staged shuffling will
            # sometimes require extra work/communication in this case.
            return super()._layer()

        # Calculate number of stages and splits per stage
        npartitions_input = self.frame.npartitions
        stages = int(math.ceil(math.log(npartitions_input) / math.log(max_branch)))
        if stages > 1:
            nsplits = int(math.ceil(npartitions_input ** (1 / stages)))
        else:
            nsplits = npartitions_input

        # Figure out how many
        inputs = [
            tuple(digit(i, j, nsplits) for j in range(stages))
            for i in range(nsplits**stages)
        ]
        parts_out = range(len(inputs))  # Could apply culling here

        # Build graph
        dsk = {}
        meta_input = make_meta(self.frame._meta)
        for stage in range(stages):
            # Define input-stage name
            if stage == 0:
                name_input = self.frame._name
            else:
                name_input = name

            # Define current stage name
            if stage == (stages - 1) and npartitions == npartitions_input:
                name = self._name
            else:
                name = f"stage-{stage}-{self._name}"

            shuffle_group_name = "group-" + name
            split_name = "split-" + name

            inp_part_map = {inp: i for i, inp in enumerate(inputs)}
            for part in parts_out:
                out = inputs[part]

                _concat_list = []  # get_item tasks to concat for this output partition
                for i in range(nsplits):
                    # Get out each individual dataframe piece from the dicts
                    _inp = insert(out, stage, i)
                    _idx = out[stage]
                    _concat_list.append((split_name, _idx, _inp))

                # concatenate those pieces together, with their friends
                dsk[(name, part)] = (
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
                            shuffle_group,
                            input_key,
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

            for p in range(npartitions):
                dsk2[(self._name, p)] = (
                    shuffle_group_get,
                    (repartition_group_name, p % npartitions_input),
                    p,
                )

            dsk.update(dsk2)
        return dsk


class DiskShuffle(SimpleShuffle):
    """Disk-based shuffle implementation"""

    lazy_hash_support = False

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
            (name, i): (shuffle_group_3, key, column, npartitions, p)
            for i, key in enumerate(df.__dask_keys__())
        }

        # Barrier
        barrier_token = "barrier-" + always_new_token
        dsk3 = {barrier_token: (barrier, list(dsk2))}

        # Collect groups
        dsk4 = {
            (self._name, i): (collect, p, i, df._meta, barrier_token)
            for i in range(npartitions)
        }

        return toolz.merge(dsk1, dsk2, dsk3, dsk4)


#
# Helper logic
#


def _select_columns_or_index(expr, columns_or_index):
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

    column_names = [n for n in columns_or_index if _is_column_label_reference(expr, n)]

    selected_expr = expr[column_names]
    if _contains_index_name(expr, columns_or_index):
        # Index name was included
        selected_expr = Assign(selected_expr, "_index", expr.index)

    return selected_expr


def _is_column_label_reference(expr, key):
    """
    Test whether a key is a column label reference

    To be considered a column label reference, `key` must match the name of at
    least one column.
    """
    return (
        not isinstance(key, Expr)
        and (np.isscalar(key) or isinstance(key, tuple))
        and key in expr.columns
    )


def _contains_index_name(expr, columns_or_index):
    """
    Test whether the input contains a reference to the index of the Expr
    """
    if isinstance(columns_or_index, list):
        return any(_is_index_level_reference(expr, n) for n in columns_or_index)
    else:
        return _is_index_level_reference(expr, columns_or_index)


def _is_index_level_reference(expr, key):
    """
    Test whether a key is an index level reference

    To be considered an index level reference, `key` must match the index name
    and must NOT match the name of any column.
    """
    index_name = expr.index._meta.name
    return (
        index_name is not None
        and not isinstance(key, Expr)
        and (np.isscalar(key) or isinstance(key, tuple))
        and key == index_name
        and key not in getattr(expr, "columns", ())
    )


class PartitioningIndex(Blockwise):
    """Create a partitioning index

    This class is used to construct a hash-based
    partitioning index for shuffling.

    Parameters
    ----------
    frame: Expr
        Frame-like expression being partitioned.
    index: Expr or list
        Index-like expression or list of columns to construct
        the partitioning-index from.
    npartitions_out: int
        Number of partitions after repartitioning is finished.
    """

    _parameters = ["frame", "index", "npartitions_out"]

    @classmethod
    def operation(cls, df, index, npartitions: int):
        """Construct a hash-based partitioning index"""
        if isinstance(index, (str, list, tuple)):
            # Assume column selection from df
            index = [index] if isinstance(index, str) else list(index)
            return partitioning_index(df[index], npartitions)
        return partitioning_index(index, npartitions)
