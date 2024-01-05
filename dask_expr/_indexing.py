from dask.array import Array
from dask.dataframe import methods
from pandas.api.types import is_bool_dtype
from pandas.errors import IndexingError

from dask_expr._collection import Series, new_collection
from dask_expr._expr import Blockwise, Projection
from dask_expr._util import is_scalar


class Indexer:
    def __init__(self, obj):
        self.obj = obj


class ILocIndexer(Indexer):
    def __getitem__(self, key):
        msg = (
            "'DataFrame.iloc' only supports selecting columns. "
            "It must be used like 'df.iloc[:, column_indexer]'."
        )
        if not isinstance(key, tuple):
            raise NotImplementedError(msg)

        if len(key) > 2:
            raise ValueError("Too many indexers")

        iindexer, cindexer = key

        if iindexer != slice(None):
            raise NotImplementedError(msg)

        if len(self.obj.columns) == len(set(self.obj.columns)):
            col_names = self.obj.columns[cindexer]
            if not is_scalar(col_names):
                col_names = list(col_names)
            return new_collection(Projection(self.obj, col_names))
        else:
            raise NotImplementedError


class LocIndexer(Indexer):
    def __getitem__(self, key):
        if isinstance(key, tuple):
            if len(key) > self.obj.ndim:
                raise IndexingError("Too many indexers")

            iindexer = key[0]
            cindexer = key[1]

            if isinstance(cindexer, slice):
                cindexer = list(self.obj._meta.loc[:, cindexer].columns)
        else:
            iindexer = key
            cindexer = None

        return self._loc(iindexer, cindexer)

    def _loc(self, iindexer, cindexer):
        if iindexer is None or isinstance(iindexer, slice) and iindexer == slice(None):
            return new_collection(Projection(self.obj, cindexer))
        if isinstance(iindexer, Series):
            return self._loc_series(iindexer, cindexer)
        elif isinstance(iindexer, Array):
            raise NotImplementedError("Passing an Array to loc is not implemented")
        elif callable(iindexer):
            return self._loc(iindexer(self.obj), cindexer)

        raise NotImplementedError

    def _loc_series(self, iindexer, cindexer):
        if not is_bool_dtype(iindexer.dtype):
            raise KeyError(
                "Cannot index with non-boolean dask Series. Try passing computed "
                "values instead (e.g. ``ddf.loc[iindexer.compute()]``)"
            )
        return new_collection(Loc(Projection(self.obj, cindexer), iindexer))


class Loc(Blockwise):
    _parameters = ["frame", "iindexer"]
    operation = staticmethod(methods.loc)
