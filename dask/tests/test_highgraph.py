import dask.array as da
import os
from dask.utils_test import inc
from dask.highgraph import HighGraph


def test_visualize(tmpdir):
    fn = str(tmpdir)
    a = da.ones(10, chunks=(5,))
    b = a + 1
    c = a + 2
    d = b + c
    d.dask.visualize(fn)
    assert os.path.exists(fn)


def test_basic():
    a = {'x': 1}
    b = {'y': (inc, 'x')}
    layers = {'a': a, 'b': b}
    dependencies = {'a': set(), 'b': {'a'}}
    hg = HighGraph(layers, dependencies)

    assert dict(hg) == {'x': 1, 'y': (inc, 'x')}
