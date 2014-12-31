import dask
from dask.thget import *
from contextlib import contextmanager


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
               'ready': set(['z']),
               'waiting': {'w': set(['z'])},
               'waiting_data': {'z': set(['w'])}}


def test_finish_task():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    state = start_state_from_dask(dsk)
    task = 'z'
    result = 2

    finish_task(dsk, task, result, state, set())

    assert state == {
          'cache': {'y': 2, 'z': 2},
          'dependencies': {'w': set(['y', 'z']),
                           'x': set([]),
                           'y': set([]),
                           'z': set(['x'])},
          'dependents': {'w': set([]),
                         'x': set(['z']),
                         'y': set(['w']),
                         'z': set(['w'])},
          'ready': set(['w']),
          'waiting': {},
          'waiting_data': {'z': set(['w'])}}


def test_get():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    assert get(dsk, 'w') == 4
    assert get(dsk, ['w', 'z']) == [4, 2]

