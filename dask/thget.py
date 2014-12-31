"""
Players:

Constants
1.  dependencies: {x: [a, b ,c]} a,b,c, must be run before x
2.  dependents: {a: [x, y]} a must run before x or y

Changing states
1.  cache: available concrete data.  {key: actual-data}
2.  waiting: which tasks are waiting on others.  {key: {keys}}
3.  ready: A set of ready-to-run tasks
4.  waiting_data: available data to yet-to-be-run-tasks {key: {keys}}
"""
from .core import istask
from operator import add
from toolz import concat
from multiprocessing.pool import ThreadPool
from multiprocessing import Queue

def inc(x):
    return x + 1


def get_arg(arg, cache):
    """

    >>> cache = {'x': 1, 'y': 2}

    >>> get_arg('x', cache)
    1

    >>> list(get_arg(['x', 'y'], cache))
    [1, 2]

    >>> list(map(list, get_arg([['x', 'y'], ['y', 'x']], cache)))
    [[1, 2], [2, 1]]
    """
    if isinstance(arg, list):
        return (get_arg(a, cache) for a in arg)
    else:
        return cache[arg]


def execute_task(dsk, task, state, queue):
    func, args = task[0], task[1:]
    args2 = [get_arg(arg, state['cache']) for arg in args]
    result = func(*args2)
    finish_task(dsk, task, result, state)
    queue.put(task)


def finish_task(dsk, task, result, state):
    """
    Update executation state after a task finishes

    Mutates.  This should run atomically (with a lock).
    """
    state['cache'][task] = result
    state['ready'].remove(task)

    for dep in state['dependents'][task]:
        s = state['waiting'][dep]
        s.remove(task)
        if not s:
            del state['waiting'][dep]
            state['ready'].add(dep)

    for dep in state['dependencies'][task]:
        if dep in state['waiting_data']:
            s = state['waiting_data'][dep]
            s.remove(task)
            if not s:
                release_data(dsk, dep, **state)
        elif dep in state['cache']:
            del state['cache'][dep]

    return state


def release_data(key, state):
    """ Remove data from temporary storage """
    assert not state['waiting_data'][key]
    del state['waiting_data'][key]
    del state['cache'][key]


def get_dependencies(dsk, task):
    """ Get the immediate tasks on which this task depends

    >>> dsk = {'x': 1,
    ...        'y': (inc, 'x'),
    ...        'z': (add, 'x', 'y'),
    ...        'w': (inc, 'z')}

    >>> get_dependencies(dsk, 'x')
    set([])

    >>> get_dependencies(dsk, 'y')
    set(['x'])

    >>> get_dependencies(dsk, 'z')  # doctest: +SKIP
    set(['x', 'y'])

    >>> get_dependencies(dsk, 'w')  # Only direct dependencies
    set(['z'])
    """
    val = dsk[task]
    if not istask(val):
        return set([])
    else:
        return set(list(val[1:]))


def reverse_dict(d):
    """

    >>> a, b, c = 'abc'
    >>> d = {a: [b, c], b: [c]}
    >>> reverse_dict(d)  # doctest: +SKIP
    {'a': set([]), 'b': set(['a']}, 'c': set(['a', 'b'])}
    """
    terms = list(d.keys()) + list(concat(d.values()))
    result = {t: set() for t in terms}
    for k, vals in d.items():
        for val in vals:
            result[val].add(k)
    return result


def start_state_from_dask(dsk, cache=None):
    """ Start state from a dask

    >>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    >>> import pprint
    >>> pprint.pprint(start_state_from_dask(dsk))
    {'cache': {'x': 1, 'y': 2},
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
    """
    cache = cache or dict()
    for k, v in dsk.items():
        if not istask(v):
            cache[k] = v

    dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    waiting = {k: v.copy() for k, v in dependencies.items() if v}

    dependents = reverse_dict(dependencies)
    for a in cache:
        for b in dependents[a]:
            waiting[b].remove(a)
    waiting_data = reverse_dict(waiting)
    waiting_data = {k: v for k, v in waiting_data.items() if v}

    ready = {k for k, v in waiting.items() if not v}
    waiting = {k: v for k, v in waiting.items() if v}

    state = {'dependencies': dependencies,
             'dependents': dependents,
             'waiting': waiting,
             'waiting_data': waiting_data,
             'cache': cache,
             'ready': ready}

    return state


def get(dsk, result, pool=None, cache=None):
    if not isinstance(result, list):
        result = [result]

    pool = pool or ThreadPool()

    state = start_state_from_dask(dsk, cache=cache)

    queue = Queue()

    for task in ready:
        pool.apply_async(execute_task, args=[task, state, queue])

    for finished_task in queue:
        print("Finished %s" % finished_task)
        for new_task in ready:
            # TODO: choose tasks more intelligently
            #       and do not aggressively send tasks to pool
            pool.apply_async(execute_task, args=[new_task, state])
