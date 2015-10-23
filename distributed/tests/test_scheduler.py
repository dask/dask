from copy import deepcopy
from operator import add

from dask.core import get_deps
from toolz import merge, concat
import pytest

from distributed.client import WrappedKey
from distributed.scheduler import (validate_state, heal, update_state,
        decide_worker, assign_many_tasks, heal_missing_data)
from distributed.utils_test import inc


def test_heal():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    dependents = {'x': {'y'}, 'y': set()}

    in_memory = set()
    stacks = {'alice': [], 'bob': []}
    processing = {'alice': set(), 'bob': set()}

    waiting = {'x': set(), 'y': {'x'}}
    waiting_data = {'x': {'y'}, 'y': set()}
    finished_results = set()

    local = {k: v for k, v in locals().items() if '@' not in k}

    output = heal(dependencies, dependents, in_memory, stacks, processing,
            {}, {})

    assert output['dependencies'] == dependencies
    assert output['dependents'] == dependents
    assert output['in_memory'] == in_memory
    assert output['processing'] == processing
    assert output['stacks'] == stacks
    assert output['waiting'] == waiting
    assert output['waiting_data'] == waiting_data
    assert output['released'] == set()

    state = {'in_memory': set(),
             'stacks': {'alice': ['x'], 'bob': []},
             'processing': {'alice': set(), 'bob': set()},
             'waiting': {}, 'waiting_data': {}}

    heal(dependencies, dependents, **state)


def test_heal_2():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y'),
           'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'result': (add, 'z', 'c')}
    dependencies = {'x': set(), 'y': {'x'}, 'z': {'y'},
                    'a': set(), 'b': {'a'}, 'c': {'b'},
                    'result': {'z', 'c'}}
    dependents = {'x': {'y'}, 'y': {'z'}, 'z': {'result'},
                  'a': {'b'}, 'b': {'c'}, 'c': {'result'},
                  'result': set()}

    state = {'in_memory': {'y', 'a'},  # missing 'b'
             'stacks': {'alice': ['z'], 'bob': []},
             'processing': {'alice': set(), 'bob': set(['c'])},
             'waiting': {}, 'waiting_data': {}}

    output = heal(dependencies, dependents, **state)
    assert output['waiting'] == {'b': set(), 'c': {'b'}, 'result': {'c', 'z'}}
    assert output['waiting_data'] == {'a': {'b'}, 'b': {'c'}, 'c': {'result'},
                                      'y': {'z'}, 'z': {'result'},
                                      'result': set()}
    assert output['in_memory'] == set(['y', 'a'])
    assert output['stacks'] == {'alice': ['z'], 'bob': []}
    assert output['processing'] == {'alice': set(), 'bob': set()}
    assert output['released'] == {'x'}


def test_heal_restarts_leaf_tasks():
    dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependents, dependencies = get_deps(dsk)

    state = {'in_memory': set(),  # missing 'b'
             'stacks': {'alice': ['a'], 'bob': ['x']},
             'processing': {'alice': set(), 'bob': set()},
             'waiting': {}, 'waiting_data': {}}

    del state['stacks']['bob']
    del state['processing']['bob']

    output = heal(dependencies, dependents, **state)
    assert 'x' in output['waiting']


def test_heal_culls():
    dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    state = {'in_memory': {'c', 'y'},
             'stacks': {'alice': ['a'], 'bob': []},
             'processing': {'alice': set(), 'bob': set('y')},
             'waiting': {}, 'waiting_data': {}}

    output = heal(dependencies, dependents, **state)
    assert 'a' not in output['stacks']['alice']
    assert output['released'] == {'a', 'b', 'x'}
    assert output['finished_results'] == {'c'}
    assert 'y' not in output['processing']['bob']

    assert output['waiting']['z'] == set()


def test_update_state():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    dependents = {'x': {'y'}, 'y': set()}

    waiting = {'y': set()}
    waiting_data = {'x': {'y'}, 'y': set()}

    held_data = {'y'}
    in_memory = {'x'}
    processing = set()
    released = set()
    in_play = {'x', 'y'}

    new_dsk = {'a': 1, 'z': (add, 'y', 'a')}
    new_keys = {'z'}

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert dependencies == e_dependencies
    assert dependents == e_dependents
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'a', 'z'}


def test_update_state_with_processing():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'z': {'y'}}
    waiting_data = {'x': {'y'}, 'y': {'z'}, 'z': set()}

    held_data = {'z'}
    in_memory = {'x'}
    processing = {'y'}
    released = set()
    in_play = {'z', 'x', 'y'}

    new_dsk = {'a': (inc, 'x'), 'b': (add, 'a', 'y'), 'c': (inc, 'z')}
    new_keys = {'b', 'c'}

    e_waiting = {'z': {'y'}, 'a': set(), 'b': {'a', 'y'}, 'c': {'z'}}
    e_waiting_data = {'x': {'y', 'a'}, 'y': {'z', 'b'}, 'z': {'c'},
                      'a': {'b'}, 'b': set(), 'c': set()}

    e_held_data = {'b', 'c', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z', 'a', 'b', 'c'}


def test_update_state_respects_WrappedKeys():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'y': set()}
    waiting_data = {'x': {'y'}, 'y': set()}

    held_data = {'y'}
    in_memory = {'x'}
    processing = set()
    released = set()
    in_play = {'x', 'y'}

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    new_dsk = {'z': (add, WrappedKey('y'), 10)}
    a = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     in_memory, in_play,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    new_dsk = {'z': (add, 'y', 10)}
    b = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     in_memory, in_play,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    assert a == b


def test_update_state_respects_data_in_memory():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = dict()
    waiting_data = {'y': set()}

    held_data = {'y'}
    in_memory = {'y'}
    processing = set()
    released = {'x'}
    in_play = {'y'}

    new_dsk = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'y', 'x')}
    new_keys = {'z'}

    e_dsk = new_dsk.copy()
    e_waiting = {'z': {'x'}, 'x': set()}
    e_waiting_data = {'x': {'z'}, 'y': {'z'}, 'z': set()}
    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z'}


def test_update_state_supports_recomputing_released_results():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = dict()
    waiting_data = {'z': set()}

    held_data = {'z'}
    in_memory = {'z'}
    processing = set()
    released = {'x', 'y'}
    in_play = {'z'}

    new_dsk = {'x': 1, 'y': (inc, 'x')}
    new_keys = {'y'}

    e_dsk = dsk.copy()
    e_waiting = {'x': set(), 'y': {'x'}}
    e_waiting_data = {'x': {'y'}, 'y': set(), 'z': set()}
    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z'}


def test_decide_worker_with_many_independent_leaves():
    dsk = merge({('y', i): (inc, ('x', i)) for i in range(100)},
                {('x', i): i for i in range(100)})
    dependencies, dependents = get_deps(dsk)
    stacks = {'alice': [], 'bob': []}
    who_has = merge({('x', i * 2): {'alice'} for i in range(50)},
                    {('x', i * 2 + 1): {'bob'} for i in range(50)})

    for key in dsk:
        worker = decide_worker(dependencies, stacks, who_has, {}, key)
        stacks[worker].append(key)

    nhits = (len([k for k in stacks['alice'] if 'alice' in who_has[('x', k[1])]])
             + len([k for k in stacks['bob'] if 'bob' in who_has[('x', k[1])]]))

    assert nhits > 90


def test_decide_worker_with_restrictions():
    dependencies = {'x': set()}
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    stacks = {alice: [], bob: [], charlie: []}
    who_has = {}
    restrictions = {'x': {'alice', 'charlie'}}
    result = decide_worker(dependencies, stacks, who_has, restrictions, 'x')
    assert result in {alice, charlie}

    stacks = {alice: [1, 2, 3], bob: [], charlie: [4, 5, 6]}
    result = decide_worker(dependencies, stacks, who_has, restrictions, 'x')
    assert result in {alice, charlie}

    dependencies = {'x': {'y'}}
    who_has = {'y': {bob}}
    result = decide_worker(dependencies, stacks, who_has, restrictions, 'x')
    assert result in {alice, charlie}


def test_validate_state():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    waiting = {'y': {'x'}, 'x': set()}
    dependents = {'x': {'y'}, 'y': set()}
    waiting_data = {'x': {'y'}}
    in_memory = set()
    stacks = {'alice': [], 'bob': []}
    processing = {'alice': set(), 'bob': set()}
    finished_results = set()
    released = set()
    in_play = {'x', 'y'}

    validate_state(**locals())

    in_memory.add('x')
    with pytest.raises(Exception):
        validate_state(**locals())

    del waiting['x']
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting['y'].remove('x')
    validate_state(**locals())

    stacks['alice'].append('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting.pop('y')
    validate_state(**locals())

    stacks['alice'].pop()
    with pytest.raises(Exception):
        validate_state(**locals())

    processing['alice'].add('y')
    validate_state(**locals())

    processing['alice'].pop()
    with pytest.raises(Exception):
        validate_state(**locals())

    in_memory.add('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    finished_results.add('y')
    validate_state(**locals())


def test_assign_many_tasks():
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    dependencies = {'y': {'x'}, 'b': {'a'}, 'x': set(), 'a': set()}
    waiting = {'y': set(), 'b': {'a'}, 'a': set()}
    keyorder = {c: 1 for c in 'abxy'}
    who_has = {'x': {alice}}
    stacks = {alice: [], bob: []}
    restrictions = {}
    keys = ['y', 'a']

    new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                   stacks, restrictions, ['y', 'a'])

    assert 'y' in stacks[alice]
    assert 'a' in stacks[alice] + stacks[bob]
    assert 'a' not in waiting
    assert 'y' not in waiting

    assert set(concat(new_stacks.values())) == set(concat(stacks.values()))


def test_assign_many_tasks_with_restrictions():
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    dependencies = {'y': {'x'}, 'b': {'a'}, 'x': set(), 'a': set()}
    waiting = {'y': set(), 'b': {'a'}, 'a': set()}
    keyorder = {c: 1 for c in 'abxy'}
    who_has = {'x': {alice}}
    stacks = {alice: [], bob: []}
    restrictions = {'y': {'bob'}, 'a': {'alice'}}
    keys = ['y', 'a']

    new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                   stacks, restrictions, ['y', 'a'])

    assert 'y' in stacks[bob]
    assert 'a' in stacks[alice]
    assert 'a' not in waiting
    assert 'y' not in waiting

    assert set(concat(new_stacks.values())) == set(concat(stacks.values()))


def test_fill_missing_data():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    waiting = {}
    waiting_data = {'z': set()}

    held_data = {'z'}
    in_memory = {'z'}
    processing = set()
    released = set()
    in_play = {'z'}

    e_waiting = {'x': set(), 'y': {'x'}, 'z': {'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'z': set()}
    e_in_play = {'x', 'y', 'z'}

    lost = {'z'}
    in_memory.remove('z')
    in_play.remove('z')

    heal_missing_data(dsk, dependencies, dependents, held_data,
                      in_memory, in_play,
                      waiting, waiting_data, lost)

    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert in_play == e_in_play
