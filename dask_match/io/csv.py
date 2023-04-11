import functools

from dask.base import tokenize

from dask_match.expr import BlockwiseArg
from dask_match.io.io import BlockwiseIO


class ReadCSV(BlockwiseIO):
    _parameters = ["filename", "usecols", "header"]
    _defaults = {"usecols": None, "header": "infer"}

    def __str__(self):
        return f"{type(self).__name__}({self.filename})"

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

    @property
    def _tasks(self):
        return list(self._ddf.dask.to_dict().values())

    def dependencies(self):
        # Need to pass `token` to ensure deterministic name
        name = f"csvdep-{tokenize(self._ddf)}"
        return [BlockwiseArg([t[1] for t in self._tasks], name)]

    def _blockwise_layer(self):
        dsk = self._tasks[0][0].dsk
        return {
            self._name: (
                next(iter(dsk.values()))[0],
                self.dependencies()[0]._name,
            )
        }
