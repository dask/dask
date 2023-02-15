class Expression:
    pass


class Opaque(Expression):
    def __init__(self, graph, name, meta, divisions):
        from dask.dataframe.core import HighLevelGraph, make_meta

        if graph is not None and not isinstance(graph, HighLevelGraph):
            graph = HighLevelGraph.from_collections(name, graph, dependencies=[])
        if meta is not None:
            meta = make_meta(meta)
        if divisions is not None:
            divisions = tuple(divisions)
        self.dask = graph
        self._name = name
        self._meta = meta
        self.divisions = divisions
