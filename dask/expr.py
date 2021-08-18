from .base import DaskMethodsMixin
from .highlevelgraph import HighLevelGraph
from .utils import OperatorMethodMixin


class Expr(DaskMethodsMixin, OperatorMethodMixin):
    _args = ()
    _inputs = ()

    def __dask_graph__(self):
        return HighLevelGraph.from_collections(
            self._name,
            self._generate_dask_layer(),
            dependencies=self._dependencies,
        )

    def __dask_keys__(self):
        return sorted(self._generate_dask_layer())

    def __dask_layers__(self):
        return (self._name,)

    def __dask_tokenize__(self):
        return self._name

    def __dask_postcompute__(self):
        raise NotImplementedError()

    @property
    def _dependencies(self):
        return [getattr(self, arg) for arg in self._args if arg in self._inputs]
