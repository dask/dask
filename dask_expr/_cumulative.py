import functools

from dask.dataframe import methods
from dask.utils import M

from dask_expr._expr import Blockwise, Expr, Projection, plain_column_projection


class CumulativeAggregations(Expr):
    _parameters = ["frame", "axis", "skipna"]
    _defaults = {"axis": None}

    chunk_operation = None
    aggregate_operation = None

    def _divisions(self):
        return self.frame._divisions()

    @functools.cached_property
    def _meta(self):
        return self.frame._meta

    def _lower(self):
        chunks = CumulativeBlockwise(
            self.frame, self.axis, self.skipna, self.chunk_operation
        )
        chunks_last = TakeLast(chunks, self.skipna)
        return CumulativeFinalize(chunks, chunks_last, self.aggregate_operation)

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Projection):
            return plain_column_projection(self, parent, dependents)


class CumulativeBlockwise(Blockwise):
    _parameters = ["frame", "axis", "skipna", "operation"]
    _defaults = {"skipna": True, "axis": None}
    _projection_passthrough = True

    @functools.cached_property
    def _meta(self):
        return self.frame._meta

    @functools.cached_property
    def operation(self):
        return self.operand("operation")

    @functools.cached_property
    def _args(self) -> list:
        return self.operands[:-1]


class TakeLast(Blockwise):
    _parameters = ["frame", "skipna"]
    _projection_passthrough = True

    @staticmethod
    def operation(a, skipna=True):
        if skipna:
            if a.ndim == 1 and (a.empty or a.isna().all()):
                return None
            a = a.ffill()
        return a.tail(n=1).squeeze()


class CumulativeFinalize(Expr):
    _parameters = ["frame", "previous_partitions", "aggregator"]

    def _divisions(self):
        return self.frame._divisions()

    @functools.cached_property
    def _meta(self):
        return self.frame._meta

    def _layer(self) -> dict:
        dsk = {}
        frame, previous_partitions = self.frame, self.previous_partitions
        dsk[(self._name, 0)] = (frame._name, 0)

        intermediate_name = self._name + "-intermediate"
        for i in range(1, self.frame.npartitions):
            if i == 1:
                dsk[(intermediate_name, i)] = (previous_partitions._name, i - 1)
            else:
                # aggregate with previous cumulation results
                dsk[(intermediate_name, i)] = (
                    methods._cum_aggregate_apply,
                    self.aggregator,
                    (intermediate_name, i - 1),
                    (previous_partitions._name, i - 1),
                )
            dsk[(self._name, i)] = (
                self.aggregator,
                (self.frame._name, i),
                (intermediate_name, i),
            )
        return dsk


class CumSum(CumulativeAggregations):
    chunk_operation = M.cumsum
    aggregate_operation = staticmethod(methods.cumsum_aggregate)


class CumProd(CumulativeAggregations):
    chunk_operation = M.cumprod
    aggregate_operation = staticmethod(methods.cumprod_aggregate)


class CumMax(CumulativeAggregations):
    chunk_operation = M.cummax
    aggregate_operation = staticmethod(methods.cummax_aggregate)


class CumMin(CumulativeAggregations):
    chunk_operation = M.cummin
    aggregate_operation = staticmethod(methods.cummin_aggregate)
