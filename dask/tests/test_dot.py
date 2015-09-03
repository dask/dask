import os
from functools import partial
import re
from operator import add, neg

import pytest

pytest.importorskip("graphviz")

from dask.dot import dot_graph, task_label, label, to_graphviz
from IPython.display import Image

# Since graphviz doesn't store a graph, we need to parse the output
label_re = re.compile('.*\[label=(.*?) shape=.*\]')
def get_label(line):
    m = label_re.match(line)
    if m:
        return m.group(1)


dsk = {'a': 1,
       'b': 2,
       'c': (neg, 'a'),
       'd': (neg, 'b'),
       'e': (add, 'c', 'd'),
       'f': (sum, ['a', 'e'])}


def test_task_label():
    assert task_label((partial(add, 1), 1)) == 'add'
    assert task_label((add, 1)) == 'add'
    assert task_label((add, (add, 1, 2))) == 'add(...)'


def test_label():
    assert label('x') == 'x'
    assert label('elemwise-ffcd9aa2231d466b5aa91e8bfa9e9487') == 'elemwise-#'

    cache = {}
    result = label('elemwise-ffcd9aa2231d466b5aa91e8bfa9e9487', cache=cache)
    assert result == 'elemwise-#0'
    # cached
    result = label('elemwise-ffcd9aa2231d466b5aa91e8bfa9e9487', cache=cache)
    assert result == 'elemwise-#0'
    assert len(cache) == 1

    result = label('elemwise-e890b510984f344edea9a5e5fe05c0db', cache=cache)
    assert result == 'elemwise-#1'
    assert len(cache) == 2

    result = label('elemwise-ffcd9aa2231d466b5aa91e8bfa9e9487', cache=cache)
    assert result == 'elemwise-#0'
    assert len(cache) == 2

    assert label('x', cache=cache) == 'x'
    assert len(cache) == 2


def test_to_graphviz():
    g = to_graphviz(dsk)
    labels = list(filter(None, map(get_label, g.body)))
    assert len(labels) == 10        # 10 nodes total
    funcs = set(('add', 'sum', 'neg'))
    assert set(labels).difference(dsk) == funcs
    assert set(labels).difference(funcs) == set(dsk)


def test_aliases():
    g = to_graphviz({'x': 1, 'y': 'x'})
    labels = list(filter(None, map(get_label, g.body)))
    assert len(labels) == 2
    assert len(g.body) - len(labels) == 1   # Single edge


def test_dot_graph():
    fn = 'test_dot_graph'
    fns = [fn + ext for ext in ['.png', '.pdf', '.dot']]
    try:
        i = dot_graph(dsk, filename=fn)
        assert all(os.path.exists(f) for f in fns)
        assert isinstance(i, Image)
    finally:
        for f in fns:
            if os.path.exists(f):
                os.remove(f)

    fn = 'mydask' # default, remove existing files
    fns = [fn + ext for ext in ['.png', '.pdf', '.dot']]
    for f in fns:
        if os.path.exists(f):
            os.remove(f)
    i = dot_graph(dsk, filename=None)
    assert all(not os.path.exists(f) for f in fns)
    assert isinstance(i, Image)

