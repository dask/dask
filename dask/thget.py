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
from toolz import concat, first, partial
from multiprocessing.pool import ThreadPool
from Queue import Queue
from threading import Lock
import psutil

def inc(x):
    return x + 1


def get_arg(arg, cache, dsk=None):
    """

    >>> cache = {'x': 1, 'y': 2}

    >>> get_arg('x', cache)
    1

    >>> list(get_arg(['x', 'y'], cache))
    [1, 2]

    >>> list(map(list, get_arg([['x', 'y'], ['y', 'x']], cache)))
    [[1, 2], [2, 1]]

    >>> get_arg('foo', cache)  # Passes through on non-keys
    'foo'
    """
    dsk = dsk or dict()
    if isinstance(arg, list):
        return (get_arg(a, cache) for a in arg)
    elif arg in cache:
        return cache[arg]
    elif arg in dsk:
        raise ValueError("Premature deletion of data.  Key: %s" % str(arg))
    else:
        return arg


def execute_task(dsk, key, state, queue, results, lock):
    try:
        task = dsk[key]
        func, args = task[0], task[1:]
        args2 = [get_arg(arg, state['cache'], dsk=dsk) for arg in args]
        result = func(*args2)
        with lock:
            finish_task(dsk, key, result, state, results)
    except Exception as e:
        queue.put((key, task, e))
        return key, task, e
    queue.put((key, task, result))
    return key, task, result


def finish_task(dsk, key, result, state, results):
    """
    Update executation state after a task finishes

    Mutates.  This should run atomically (with a lock).
    """
    state['cache'][key] = result
    if key in state['ready']:
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
            if not s and dep not in results:
                release_data(dep, state)
        elif dep in state['cache'] and dep not in results:
            del state['cache'][dep]

    state['finished'].add(key)
    state['num-active-threads'] -= 1

    return state


def release_data(key, state):
    """ Remove data from temporary storage """
    assert not state['waiting_data'][key]
    state['released'].add(key)
    del state['waiting_data'][key]
    del state['cache'][key]


def get_dependencies(dsk, task):
    """ Get the immediate tasks on which this task depends

    >>> dsk = {'x': 1,
    ...        'y': (inc, 'x'),
    ...        'z': (add, 'x', 'y'),
    ...        'w': (inc, 'z'),
    ...        'a': (add, 'x', 1)}

    >>> get_dependencies(dsk, 'x')
    set([])

    >>> get_dependencies(dsk, 'y')
    set(['x'])

    >>> get_dependencies(dsk, 'z')  # doctest: +SKIP
    set(['x', 'y'])

    >>> get_dependencies(dsk, 'w')  # Only direct dependencies
    set(['z'])

    >>> get_dependencies(dsk, 'a')  # Ignore non-keys
    set(['x'])
    """
    val = dsk[task]
    if not istask(val):
        return set([])
    else:
        return set(k for k in flatten(val[1:]) if k in dsk)


def flatten(seq):
    """

    >>> list(flatten([1]))
    [1]

    >>> list(flatten([[1, 2], [1, 2]]))
    [1, 2, 1, 2]

    >>> list(flatten([[[1], [2]], [[1], [2]]]))
    [1, 2, 1, 2]

    >>> list(flatten(((1, 2), (1, 2)))) # Don't flatten tuples
    [(1, 2), (1, 2)]
    """
    if not isinstance(first(seq), list):
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
    >>> pprint.pprint(start_state_from_dask(dsk)) # doctest: +NORMALIZE_WHITESPACE
    {'cache': {'x': 1, 'y': 2},
     'dependencies': {'w': set(['y', 'z']),
                      'x': set([]),
                      'y': set([]),
                      'z': set(['x'])},
     'dependents': {'w': set([]),
                    'x': set(['z']),
                    'y': set(['w']),
                    'z': set(['w'])},
     'finished': set([]),
     'num-active-threads': 0,
     'ready': set(['z']),
     'released': set([]),
     'waiting': {'w': set(['z'])},
     'waiting_data': {'x': set(['z']),
                      'y': set(['w']),
                      'z': set(['w'])}}
    """
    if cache is None:
        cache = dict()
    for k, v in dsk.items():
        if not istask(v):
            cache[k] = v

    dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    waiting = {k: v.copy() for k, v in dependencies.items() if v}

    dependents = reverse_dict(dependencies)
    for a in cache:
        for b in dependents[a]:
            waiting[b].remove(a)
    waiting_data = {k: v.copy() for k, v in dependents.items() if v}

    ready = {k for k, v in waiting.items() if not v}
    waiting = {k: v for k, v in waiting.items() if v}

    state = {'dependencies': dependencies,
             'dependents': dependents,
             'waiting': waiting,
             'waiting_data': waiting_data,
             'cache': cache,
             'ready': ready,
             'finished': set(),
             'released': set(),
             'num-active-threads': 0}

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
        if lazy:
            return (ndget(i, coll, lazy=lazy) for i in ind)
        else:
            return tuple([ndget(i, coll, lazy=lazy) for i in ind])
        return seq
    else:
        return coll[ind]


def get(dsk, result, nthreads=psutil.NUM_CPUS, cache=None, debug_counts=None, **kwargs):
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
    results = set(result_flat)

    pool = ThreadPool(nthreads)

    state = start_state_from_dask(dsk, cache=cache)

    queue = Queue()
    lock = Lock()  # lock for state dict
    jobs = dict()
    tick = [0]

    if not state['ready']:
        raise ValueError("Found no accessible jobs in dask")

    def fire_task():
        tick[0] += 1
        if debug_counts and tick[0] % debug_counts == 0:
            visualize(dsk, state, jobs, filename='dask_%d' % tick)
        key = choose_task(state)
        state['ready'].remove(key)
        state['num-active-threads'] += 1
        jobs[key] = pool.apply_async(execute_task, args=[dsk, key, state,
            queue, results, lock])


    try:
        # Seed initial tasks into the thread pool
        with lock:
            while state['ready'] and state['num-active-threads'] < nthreads:
                fire_task()

        # Main loop, wait on tasks to finish, insert new ones
        while state['waiting'] or state['ready'] or len(state['finished']) < len(jobs):
            key, finished_task, res = queue.get()
            if isinstance(res, Exception):
                raise res
            with lock:
                while state['ready'] and state['num-active-threads'] < nthreads:
                    fire_task()

    finally:
        # Clean up thread pool
        pool.close()
        pool.join()

    # Final reporting
    while not queue.empty():
        key, finished_task, res = queue.get()
        # print("Finished %s" % str(finished_task))

    if debug_counts:
        visualize(dsk, state, jobs, filename='dask_end')

    return ndget(result, state['cache'])


def visualize(dsk, state, jobs, filename='dask'):
    from dask.dot import dot_graph
    data, func = dict(), dict()
    for key in state['cache']:
        data[key] = {'color': 'blue'}
    for key in state['released']:
        data[key] = {'color': 'gray'}
    for key in jobs:
        func[key] = {'color': 'green'}
    for key in state['finished']:
        func[key] = {'color': 'blue'}
    for key in state['waiting']:
        func[key] = {'color': 'gray'}
    return dot_graph(dsk, data_attributes=data, function_attributes=func,
            filename=filename)


def score(key, state):
    """ Prefer to run tasks that remove need to hold on to data """
    deps = state['dependencies'][key]
    wait = state['waiting_data']
    return sum([1./len(wait[dep])**2 for dep in deps])

def choose_task(state, score=score):
    """
    Select a task that maximizes scoring function

    Default scoring function selects tasks that free up the maximum number of
    resources.

    E.g. for ready tasks a, b with dependencies:

        {a: {x, y},
         b: {x, w}}

    and for data w, x, y, z waiting on the following tasks

        {w: {b, c}
         x: {a, b, c},
         y: {a}}

    We choose task a because it will completely free up resource y and
    partially free up resource x.  Task b only partially frees up resources x
    and w and completely frees none so it is given a lower score.

    See also:
        score
    """
    return max(state['ready'], key=partial(score, state=state))
