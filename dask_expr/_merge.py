import functools

from dask.dataframe.dispatch import make_meta, meta_nonempty
from dask.utils import M, apply

from dask_expr._expr import Blockwise, Expr, Projection
from dask_expr._repartition import Repartition
from dask_expr._shuffle import Shuffle, _contains_index_name


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
        if isinstance(parent, Projection):
            # Reorder the column projection to
            # occur before the Merge
            projection = parent.operand("columns")
            if isinstance(projection, (str, int)):
                projection = [projection]

            left, right = self.left, self.right
            left_on, right_on = self.left_on, self.right_on
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
                return type(self)(
                    left[project_left], right[project_right], *self.operands[2:]
                )[parent.operand("columns")]


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
