import functools

from dask_expr.io.io import BlockwiseIO, PartitionsFiltered


class ReadCSV(PartitionsFiltered, BlockwiseIO):
    _parameters = ["filename", "usecols", "header", "_partitions"]
    _defaults = {"usecols": None, "header": "infer", "_partitions": None}

    @functools.cached_property
    def _ddf(self):
        # Temporary hack to simplify logic
        import dask.dataframe as dd

        return dd.read_csv(
            self.filename,
            usecols=self.usecols,
            header=self.header,
        )

    @property
    def _meta(self):
        return self._ddf._meta

    def _divisions(self):
        return self._ddf.divisions

    @functools.cached_property
    def _tasks(self):
        return list(self._ddf.dask.to_dict().values())

    def _filtered_task(self, index: int):
        return self._tasks[index]
