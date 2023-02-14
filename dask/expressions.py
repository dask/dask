class Expression:
    pass


class Opaque(Expression):
    def __init__(self, graph, name, meta, divisions):
        self.dask = graph
        self._name = name
        self._meta = meta
        self.divisions = divisions
