import os
from functools import partial

import numpy as np
import pytest

pydot = pytest.importorskip("pydot")

from dask.dot import dot_graph, label


def add(x, y):
    return x + y


def inc(x):
    return x + 1


dsk = {'x': 1, 'y': (inc, 'x'),
       'a': 2, 'b': (inc, 'a'),
       'z': (add, 'y', 'b'),
       'c': (sum, ['y', 'b'])}


def test_label():
    assert label(partial(add, 1)) == 'add'


def test_dot_graph():
    fn = 'test_dot_graph'
    fns = [fn + ext for ext in ['.png', '.pdf', '.dot']]
    try:
        dot_graph(dsk, filename=fn)
        assert all(os.path.exists(f) for f in fns)
    except (ImportError, AttributeError):
        pass
    finally:
        for f in fns:
            if os.path.exists(f):
                os.remove(f)
