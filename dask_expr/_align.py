import functools

from tlz import merge_sorted, unique

from dask_expr._expr import Expr, Projection, is_broadcastable
from dask_expr._repartition import RepartitionDivisions


class AlignPartitions(Expr):
    _parameters = ["frame"]

    @property
    def _meta(self):
        return self.frame._meta

    @functools.cached_property
    def dfs(self):
        return [
            df for df in self.dependencies() if df.ndim > 0 and not is_broadcastable(df)
        ]

    def _divisions(self):
        divisions = list(unique(merge_sorted(*[df.divisions for df in self.dfs])))
        if len(divisions) == 1:  # single value for index
            divisions = (divisions[0], divisions[0])
        return divisions

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])

    def _lower(self):
        if not self.dfs:
            return self.frame

        if all(df.divisions == self.dfs[0].divisions for df in self.dfs):
            return self.frame

        if not all(df.known_divisions for df in self.dfs):
            raise ValueError(
                "Not all divisions are known, can't align "
                "partitions. Please use `set_index` "
                "to set the index."
            )

        return RepartitionDivisions(
            self.frame, new_divisions=self.divisions, force=True
        )
