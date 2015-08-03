class Base(object):
    """Base class for dask collections"""

    def visualize(self, optimize_graph=False):
        from dask.dot import dot_graph
        if optimize_graph:
            return dot_graph(self._optimize(self.dask, self._keys()))
        else:
            return dot_graph(self.dask)
