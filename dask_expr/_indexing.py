from dask.dataframe import methods

from dask_expr import new_collection
from dask_expr._expr import Blockwise, Projection


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
            return new_collection(Projection(self.obj.expr, col_names))
        else:
            return new_collection(ILoc(self.obj.expr, key))


class ILoc(Blockwise):
    operation = staticmethod(methods.iloc)
