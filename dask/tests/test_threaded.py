import dask
from dask.threaded import *
from contextlib import contextmanager
from dask.utils import raises
from operator import add, mul
from copy import deepcopy


fib_dask = {'f0': 0, 'f1': 1, 'f2': 1, 'f3': 2, 'f4': 3, 'f5': 5, 'f6': 8}


def test_start_state():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    result = start_state_from_dask(dsk)

    expeted = {'cache': {'x': 1, 'y': 2},
               'dependencies': {'w': set(['y', 'z']),
                                'x': set([]),
                                'y': set([]),
                                'z': set(['x'])},
               'dependents': {'w': set([]),
                              'x': set(['z']),
                              'y': set(['w']),
                              'z': set(['w'])},
               'finished': set([]),
               'released': set([]),
               'running': set([]),
               'ready': set(['z']),
               'waiting': {'w': set(['z'])},
               'waiting_data': {'x': set(['z']),
                                'y': set(['w']),
                                'z': set(['w'])}}



def test_start_state_with_independent_but_runnable_tasks():
    assert start_state_from_dask({'x': (inc, 1)})['ready'] == set(['x'])


def test_finish_task():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    state = start_state_from_dask(dsk)
    state['running'] = set(['z', 'other-task'])
    task = 'z'
    result = 2

    oldstate = deepcopy(state)
    finish_task(dsk, task, result, state, set())

    assert state == {
          'cache': {'y': 2, 'z': 2},
          'dependencies': {'w': set(['y', 'z']),
                           'x': set([]),
                           'y': set([]),
                           'z': set(['x'])},
          'finished': set(['z']),
          'released': set(['x']),
          'running': set(['other-task']),
          'dependents': {'w': set([]),
                         'x': set(['z']),
                         'y': set(['w']),
                         'z': set(['w'])},
          'ready': set(['w']),
          'waiting': {},
          'waiting_data': {'y': set(['w']),
                           'z': set(['w'])}}


def test_get():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    assert get(dsk, 'w') == 4
    assert get(dsk, ['w', 'z']) == (4, 2)


def test_nested_get():
    dsk = {'x': 1, 'y': 2, 'a': (add, 'x', 'y'), 'b': (sum, ['x', 'y'])}
    assert get(dsk, ['a', 'b']) == (3, 3)


def bad(x):
    raise ValueError()

def test_exceptions_rise_to_top():
    dsk = {'x': 1, 'y': (bad, 'x')}
    assert raises(ValueError, lambda: get(dsk, 'y'))


def test_choose_task():
    dsk = {'x': 1, 'y': 1, 'a': (add, 'x', 'y'), 'b': (inc, 'x')}
    state = start_state_from_dask(dsk)
    assert choose_task(state) == 'a'  # can remove two data at once


    dsk = {'x': 1, 'y': 1, 'a': (inc, 'x'), 'b': (inc, 'y'), 'c': (inc, 'x')}
    state = start_state_from_dask(dsk)
    assert choose_task(state) == 'b'  # only task that removes data


def test_state_to_networkx():
    import networkx as nx
    dsk = {'x': 1, 'y': 1, 'a': (add, 'x', 'y'), 'b': (inc, 'x')}
    state = start_state_from_dask(dsk)
    g = state_to_networkx(dsk, state)
    assert isinstance(g, nx.DiGraph)


def double(x):
    return x * 2

def test_inline():
    x, y, i, d = 'xyid'
    dsk = {'out': (add, i, d),
           i: (inc, x),
           d: (double, y),
           x: 1, y: 1}

    result = inline(dsk, fast_functions=set([inc]))
    expected = {'out': (add, (inc, x), d),
                d: (double, y),
                x: 1, y: 1}
    assert result == expected


def test_inline_ignores_curries_and_partials():
    dsk = {'x': 1, 'y': 2,
           'a': (partial(add, 1), 'x'),
           'b': (inc, 'a')}

    result = inline(dsk, fast_functions=set([add]))
    assert 'a' not in set(result.keys())


def test_inline_doesnt_shrink_fast_functions_at_top():
    dsk = {'x': (inc, 'y'), 'y': 1}
    result = inline(dsk, fast_functions=set([inc]))
    assert result == dsk


def test_inline_traverses_lists():
    x, y, i, d = 'xyid'
    dsk = {'out': (sum, [i, d]),
           i: (inc, x),
           d: (double, y),
           x: 1, y: 1}
    expected = {'out': (sum, [(inc, x), d]),
                d: (double, y),
                x: 1, y: 1}
    result = inline(dsk, fast_functions=set([inc]))
    assert result == expected


def test_expand_value():
    dsk = {'out': (sum, ['i', 'd']),
           'i': (inc, 'x'),
           'd': (double, 'y'),
           'x': 1, 'y': 1}
    assert expand_value(dsk, [inc], 'd') == (double, 'y')
    assert expand_value(dsk, [inc], 'i') == (inc, 'x')
    assert expand_value(dsk, [inc], 'out') == (sum, [(inc, 'x'), 'd'])


def test_expand_key():
    dsk = {'out': (sum, ['i', 'd']),
           'i': (inc, 'x'),
           'd': (double, 'y'),
           'x': 1, 'y': 1}
    assert expand_key(dsk, [inc], 'd') == 'd'
    assert expand_key(dsk, [inc], 'i') == (inc, 'x')
    assert expand_key(dsk, [inc], ['i', 'd']) == [(inc, 'x'), 'd']
