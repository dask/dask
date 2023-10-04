import functools

from dask.core import flatten
from dask.dataframe.dispatch import make_meta, meta_nonempty
from dask.utils import M, apply, get_default_shuffle_method

from dask_expr._expr import (
    Blockwise,
    Expr,
    Filter,
    Index,
    PartitionsFiltered,
    Projection,
)
from dask_expr._repartition import Repartition
from dask_expr._shuffle import AssignPartitioningIndex, Shuffle, _contains_index_name
from dask_expr._util import _convert_to_list

_HASH_COLUMN_NAME = "__hash_partition"


class Merge(Expr):
    """Merge / join two dataframes

    This is an abstract class.  It will be transformed into a concrete
    implementation before graph construction.

    See Also
    --------
    BlockwiseMerge
    Repartition
    Shuffle
    """

    _parameters = [
        "left",
        "right",
        "how",
        "left_on",
        "right_on",
        "left_index",
        "right_index",
        "suffixes",
        "indicator",
        "shuffle_backend",
    ]
    _defaults = {
        "how": "inner",
        "left_on": None,
        "right_on": None,
        "left_index": False,
        "right_index": False,
        "suffixes": ("_x", "_y"),
        "indicator": False,
        "shuffle_backend": None,
    }

    def __str__(self):
        return f"Merge({self._name[-7:]})"

    @property
    def kwargs(self):
        return {
            k: self.operand(k)
            for k in [
                "how",
                "left_on",
                "right_on",
                "left_index",
                "right_index",
                "suffixes",
                "indicator",
            ]
        }

    @functools.cached_property
    def _meta(self):
        left = meta_nonempty(self.left._meta)
        right = meta_nonempty(self.right._meta)
        return make_meta(left.merge(right, **self.kwargs))

    def _divisions(self):
        npartitions_left = self.left.npartitions
        npartitions_right = self.right.npartitions
        npartitions = max(npartitions_left, npartitions_right)
        return (None,) * (npartitions + 1)

    def _lower(self):
        # Lower from an abstract expression
        left = self.left
        right = self.right
        how = self.how
        left_on = self.left_on
        right_on = self.right_on
        left_index = self.left_index
        right_index = self.right_index
        shuffle_backend = self.shuffle_backend

        # TODO:
        #  1. Handle unnamed index with unknown divisions
        #  2. Add multi-partition broadcast merge
        #  3. Add/leverage partition statistics

        # Check for "trivial" broadcast (single partition)
        npartitions = max(left.npartitions, right.npartitions)
        if (
            npartitions == 1
            or left.npartitions == 1
            and how in ("right", "inner")
            or right.npartitions == 1
            and how in ("left", "inner")
        ):
            return BlockwiseMerge(left, right, **self.kwargs)

        # Check if we are merging on indices with known divisions
        merge_indexed_left = (
            left_index or _contains_index_name(left, left_on)
        ) and left.known_divisions
        merge_indexed_right = (
            right_index or _contains_index_name(right, right_on)
        ) and right.known_divisions

        # NOTE: Merging on an index is fragile. Pandas behavior
        # depends on the actual data, and so we cannot use `meta`
        # to accurately predict the output columns. Once general
        # partition statistics are available, it may make sense
        # to drop support for left_index and right_index.

        shuffle_left_on = left_on
        shuffle_right_on = right_on
        if merge_indexed_left and merge_indexed_right:
            # fully-indexed merge
            if left.npartitions >= right.npartitions:
                right = Repartition(right, new_divisions=left.divisions, force=True)
            else:
                left = Repartition(left, new_divisions=right.divisions, force=True)
            shuffle_left_on = shuffle_right_on = None

        # TODO:
        #   - Need 'rearrange_by_divisions' equivalent
        #     to avoid shuffle when we are merging on known
        #     divisions on one side only.
        #   - Need mechanism to shuffle by an un-named index.
        else:
            if left_index:
                shuffle_left_on = left.index._meta.name
                if shuffle_left_on is None:
                    raise NotImplementedError("Cannot shuffle unnamed index")
            if right_index:
                shuffle_right_on = right.index._meta.name
                if shuffle_right_on is None:
                    raise NotImplementedError("Cannot shuffle unnamed index")

        if (shuffle_left_on or shuffle_right_on) and (
            shuffle_backend == "p2p"
            or shuffle_backend is None
            and get_default_shuffle_method() == "p2p"
        ):
            left = AssignPartitioningIndex(
                left, shuffle_left_on, _HASH_COLUMN_NAME, self.npartitions
            )
            right = AssignPartitioningIndex(
                right, shuffle_right_on, _HASH_COLUMN_NAME, self.npartitions
            )
            return HashJoinP2P(
                left,
                right,
                left_on=left_on,
                right_on=right_on,
                suffixes=self.suffixes,
                indicator=self.indicator,
                left_index=left_index,
                right_index=right_index,
            )

        if shuffle_left_on:
            # Shuffle left
            left = Shuffle(
                left,
                shuffle_left_on,
                npartitions_out=npartitions,
                backend=shuffle_backend,
            )

        if shuffle_right_on:
            # Shuffle right
            right = Shuffle(
                right,
                shuffle_right_on,
                npartitions_out=npartitions,
                backend=shuffle_backend,
            )

        # Blockwise merge
        return BlockwiseMerge(left, right, **self.kwargs)

    def _simplify_up(self, parent):
        if isinstance(parent, (Projection, Index)):
            # Reorder the column projection to
            # occur before the Merge
            if isinstance(parent, Index):
                # Index creates an empty column projection
                projection, parent_columns = [], None
            else:
                projection, parent_columns = parent.operand("columns"), parent.operand(
                    "columns"
                )
                if isinstance(projection, (str, int)):
                    projection = [projection]

            left, right = self.left, self.right
            left_on = _convert_to_list(self.left_on)
            if left_on is None:
                left_on = []

            right_on = _convert_to_list(self.right_on)
            if right_on is None:
                right_on = []

            left_suffix, right_suffix = self.suffixes[0], self.suffixes[1]
            project_left, project_right = [], []

            # Find columns to project on the left
            for col in left.columns:
                if col in left_on or col in projection:
                    project_left.append(col)
                elif f"{col}{left_suffix}" in projection:
                    project_left.append(col)
                    if col in right.columns:
                        # Right column must be present
                        # for the suffix to be applied
                        project_right.append(col)

            # Find columns to project on the right
            for col in right.columns:
                if col in right_on or col in projection:
                    project_right.append(col)
                elif f"{col}{right_suffix}" in projection:
                    project_right.append(col)
                    if col in left.columns and col not in project_left:
                        # Left column must be present
                        # for the suffix to be applied
                        project_left.append(col)

            if set(project_left) < set(left.columns) or set(project_right) < set(
                right.columns
            ):
                result = type(self)(
                    left[project_left], right[project_right], *self.operands[2:]
                )
                if parent_columns is None:
                    return type(parent)(result)
                return result[parent_columns]

    def _validate_same_operations(self, common, op, remove_ops, skip_ops):
        # Travers left and right to check if we can find the same operation
        # more than once. We have to account for potential projections on both sides
        name = common._name
        if name == op._name:
            return True
        op_left, _ = self._remove_operations(op.left, remove_ops, skip_ops)
        op_right, _ = self._remove_operations(op.right, remove_ops, skip_ops)
        return type(op)(op_left, op_right, *op.operands[2:])._name == name

    def _combine_similar(self, root: Expr):
        # Push projections back up to avoid performing the same merge multiple times
        skip_ops = (Filter, AssignPartitioningIndex, Shuffle)
        remove_ops = (Projection,)

        def _flatten_columns(columns, side):
            if len(columns) == 0:
                return getattr(self, side).columns
            else:
                return list(set(flatten(columns)))

        left, columns_left = self._remove_operations(self.left, remove_ops, skip_ops)
        columns_left = _flatten_columns(columns_left, "left")
        right, columns_right = self._remove_operations(self.right, remove_ops, skip_ops)
        columns_right = _flatten_columns(columns_right, "right")

        if left._name == self.left._name and right._name == self.right._name:
            # There aren't any ops we can remove, so bail
            return

        common = type(self)(left, right, *self.operands[2:])

        push_up_op = False
        for op in self._find_similar_operations(root, ignore=self._parameters):
            if self._validate_same_operations(common, op, remove_ops, skip_ops):
                push_up_op = True
                break

        if push_up_op:
            columns = columns_left.copy()
            columns += [col for col in columns_right if col not in columns_left]
            if _HASH_COLUMN_NAME in columns:
                # Don't filter for hash_column_name which is removed in p2p merge
                columns.remove(_HASH_COLUMN_NAME)
            if sorted(common.columns) != sorted(columns):
                common = common[columns]
            c = common._simplify_down()
            common = c if c is not None else common
            return common


class HashJoinP2P(Merge, PartitionsFiltered):
    _parameters = [
        "left",
        "right",
        "how",
        "left_on",
        "right_on",
        "left_index",
        "right_index",
        "suffixes",
        "indicator",
        "_partitions",
    ]
    _defaults = {
        "how": "inner",
        "left_on": None,
        "right_on": None,
        "left_index": None,
        "right_index": None,
        "suffixes": ("_x", "_y"),
        "indicator": False,
        "_partitions": None,
    }

    def _lower(self):
        return None

    @functools.cached_property
    def _meta(self):
        left = self.left._meta.drop(columns=_HASH_COLUMN_NAME)
        right = self.right._meta.drop(columns=_HASH_COLUMN_NAME)
        return left.merge(
            right,
            left_on=self.left_on,
            right_on=self.right_on,
            indicator=self.indicator,
            suffixes=self.suffixes,
            left_index=self.left_index,
            right_index=self.right_index,
        )

    def _layer(self) -> dict:
        from distributed.shuffle._core import ShuffleId, barrier_key
        from distributed.shuffle._merge import merge_transfer, merge_unpack
        from distributed.shuffle._shuffle import shuffle_barrier

        dsk = {}
        name_left = "hash-join-transfer-" + self.left._name
        name_right = "hash-join-transfer-" + self.right._name
        transfer_keys_left = list()
        transfer_keys_right = list()
        for i in range(self.left.npartitions):
            transfer_keys_left.append((name_left, i))
            dsk[(name_left, i)] = (
                merge_transfer,
                (self.left._name, i),
                self.left._name,
                i,
                self.npartitions,
                self.left._meta,
                self._partitions,
            )
        for i in range(self.right.npartitions):
            transfer_keys_right.append((name_right, i))
            dsk[(name_right, i)] = (
                merge_transfer,
                (self.right._name, i),
                self.right._name,
                i,
                self.npartitions,
                self.right._meta,
                self._partitions,
            )

        _barrier_key_left = barrier_key(ShuffleId(self.left._name))
        _barrier_key_right = barrier_key(ShuffleId(self.right._name))
        dsk[_barrier_key_left] = (shuffle_barrier, self.left._name, transfer_keys_left)
        dsk[_barrier_key_right] = (
            shuffle_barrier,
            self.right._name,
            transfer_keys_right,
        )

        for part_out in self._partitions:
            dsk[(self._name, part_out)] = (
                merge_unpack,
                self.left._name,
                self.right._name,
                part_out,
                _barrier_key_left,
                _barrier_key_right,
                self.how,
                self.left_on,
                self.right_on,
                self._meta,
                self.suffixes,
                self.left_index,
                self.right_index,
            )
        return dsk

    def _simplify_up(self, parent):
        return


class BlockwiseMerge(Merge, Blockwise):
    """Merge two dataframes with aligned partitions

    This operation will directly merge partition i of the
    left dataframe with partition i of the right dataframe.
    The two dataframes must be shuffled or partitioned
    by the merge key(s) before this operation is performed.
    Single-partition dataframes will always be broadcasted.

    See Also
    --------
    Merge
    """

    def _lower(self):
        return None

    def _broadcast_dep(self, dep: Expr):
        return dep.npartitions == 1

    def _task(self, index: int):
        return (
            apply,
            M.merge,
            [
                self._blockwise_arg(self.left, index),
                self._blockwise_arg(self.right, index),
            ],
            self.kwargs,
        )


class JoinRecursive(Expr):
    _parameters = ["frames", "how"]
    _defaults = {"right_index": True, "how": "outer"}

    @functools.cached_property
    def _meta(self):
        if len(self.frames) == 1:
            return self.frames[0]._meta
        else:
            return self.frames[0]._meta.join(
                [op._meta for op in self.frames[1:]],
            )

    def _divisions(self):
        npartitions = [frame.npartitions for frame in self.frames]
        return (None,) * (max(npartitions) + 1)

    def _lower(self):
        if self.how == "left":
            right = self._recursive_join(self.frames[1:])
            return Merge(
                self.frames[0],
                right,
                how=self.how,
                left_index=True,
                right_index=True,
            )

        return self._recursive_join(self.frames)

    def _recursive_join(self, frames):
        if len(frames) == 1:
            return frames[0]

        if len(frames) == 2:
            return Merge(
                frames[0],
                frames[1],
                how="outer",
                left_index=True,
                right_index=True,
            )

        midx = len(self.frames) // 2

        return self._recursive_join(
            [
                self._recursive_join(frames[:midx]),
                self._recursive_join(frames[midx:]),
            ],
        )
