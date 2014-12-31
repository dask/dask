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
from toolz import concat, first
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


def execute_task(dsk, key, state, queue, results):
    task = dsk[key]
    func, args = task[0], task[1:]
    args2 = [get_arg(arg, state['cache']) for arg in args]
    result = func(*args2)
    finish_task(dsk, key, result, state, results)
    queue.put(task)


def finish_task(dsk, key, result, state, results):
    """
    Update executation state after a task finishes

    Mutates.  This should run atomically (with a lock).
    """
    state['cache'][key] = result
    state['ready'].remove(key)

    for dep in state['dependents'][key]:
        s = state['waiting'][dep]
        s.remove(key)
        if not s:
            del state['waiting'][dep]
            state['ready'].add(dep)

    for dep in state['dependencies'][key]:
        if dep in state['waiting_data']:
            s = state['waiting_data'][dep]
            s.remove(key)
            if not s and s not in results:
                release_data(dsk, dep, **state)
        elif dep in state['cache'] and dep not in results:
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
        return set(flatten(val[1:]))


def flatten(seq):
    """

    >>> list(flatten([1]))
    [1]

    >>> list(flatten([[1, 2], [1, 2]]))
    [1, 2, 1, 2]

    >>> list(flatten([[[1], [2]], [[1], [2]]]))
    [1, 2, 1, 2]
    """
    if not isinstance(first(seq), (list, tuple, set)):
        return seq
    else:
        return concat(map(flatten, seq))

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


def ndget(ind, coll, lazy=False):
    """

    >>> ndget(1, 'abc')
    'b'
    >>> ndget([1, 0], 'abc')
    ('b', 'a')
    >>> ndget([[1, 0], [0, 1]], 'abc')
    (('b', 'a'), ('a', 'b'))
    """
    if isinstance(ind, list):
        seq = (ndget(i, coll, lazy=lazy) for i in ind)
        if not lazy:
            seq = tuple(seq)
        return seq
    else:
        return coll[ind]


def get(dsk, result, pool=None, cache=None):
    """ Threaded cached implementation of dask.get

    Parameters
    ----------

    dsk: dict
        A dask dictionary specifying a workflow
    result: key or list of keys
        Keys corresponding to desired data
    pool: multiprocessing.pool.ThreadPool (optional)
        A thread pool to use (default to creating one)
    cache: dict-like (optional)
        Temporary storage of results

    Examples
    --------

    >>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    >>> get(dsk, 'w')
    4
    >>> get(dsk, ['w', 'y'])
    (4, 2)
    """
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])

    pool = pool or ThreadPool()

    state = start_state_from_dask(dsk, cache=cache)

    queue = Queue()
    jobs = []

    # Seed initial tasks into the thread pool
    for key in state['ready']:
        jobs.append(pool.apply_async(execute_task, args=[dsk, key, state, queue, results]))

    # Main loop, wait on tasks to finish, insert new ones
    while state['waiting']:
        finished_task = queue.get()
        # print("Finished %s" % str(finished_task))
        for new_key in state['ready']:
            # TODO: choose tasks more intelligently
            #       and do not aggressively send tasks to pool
            pool.apply_async(execute_task, args=[dsk, new_key, state, queue, results])

    # Clean up thread pool
    pool.close()
    pool.join()

    # Final reporting
    while not queue.empty():
        finished_task = queue.get()
        # print("Finished %s" % str(finished_task))

    return ndget(result, state['cache'])
