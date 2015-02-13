from dask.async import *
from operator import add
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


def test_get():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    assert get_sync(dsk, 'w') == 4
    assert get_sync(dsk, ['w', 'z']) == (4, 2)


def test_nested_get():
    dsk = {'x': 1, 'y': 2, 'a': (add, 'x', 'y'), 'b': (sum, ['x', 'y'])}
    assert get_sync(dsk, ['a', 'b']) == (3, 3)
