import functools

from dask.dataframe import methods

from dask_match.expr import Expr


class Repartition(Expr):
    # TODO: Make this a proper abstract expression
    pass


class ReducePartitionCount(Repartition):
    _parameters = ["frame", "n"]

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        return tuple(self.frame.divisions[i] for i in self._partitions_boundaries)

    @functools.cached_property
    def _partitions_boundaries(self):
        npartitions = self.n
        npartitions_input = self.frame.npartitions
        assert npartitions_input > self.n

        npartitions_ratio = npartitions_input / npartitions
        new_partitions_boundaries = [
            int(new_partition_index * npartitions_ratio)
            for new_partition_index in range(npartitions + 1)
        ]

        if not isinstance(new_partitions_boundaries, list):
            new_partitions_boundaries = list(new_partitions_boundaries)
        if new_partitions_boundaries[0] > 0:
            new_partitions_boundaries.insert(0, 0)
        if new_partitions_boundaries[-1] < self.frame.npartitions:
            new_partitions_boundaries.append(self.frame.npartitions)
        return new_partitions_boundaries

    def _layer(self):
        new_partitions_boundaries = self._partitions_boundaries
        return {
            (self._name, i): (
                methods.concat,
                [(self.frame._name, j) for j in range(start, end)],
            )
            for i, (start, end) in enumerate(
                zip(new_partitions_boundaries, new_partitions_boundaries[1:])
            )
        }
