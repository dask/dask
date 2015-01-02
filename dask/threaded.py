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
from .core import istask, flatten, reverse_dict, get_dependencies
from operator import add
from toolz import concat, partial
from multiprocessing.pool import ThreadPool
from Queue import Queue
from threading import Lock
import psutil

def inc(x):
    return x + 1


def _execute_task(arg, cache, dsk=None):
    """

    >>> cache = {'x': 1, 'y': 2}

    Compute tasks against a cache
    >>> _execute_task((add, (inc, 'x'), 1), cache)  # Support nested computation
    3

    >>> _execute_task((add, 'x', 1), cache)  # Compute task in naive manner
    2

    Also grab data from cache
    >>> _execute_task('x', cache)
    1

    Support nested lists
    >>> list(_execute_task(['x', 'y'], cache))
    [1, 2]

    >>> list(map(list, _execute_task([['x', 'y'], ['y', 'x']], cache)))
    [[1, 2], [2, 1]]

    >>> _execute_task('foo', cache)  # Passes through on non-keys
    'foo'
    """
    dsk = dsk or dict()
    if isinstance(arg, list):
        return (_execute_task(a, cache) for a in arg)
    elif istask(arg):
        func, args = arg[0], arg[1:]
        args2 = [_execute_task(a, cache, dsk=dsk) for a in args]
        return func(*args2)
    elif arg in cache:
        return cache[arg]
    elif arg in dsk:
        raise ValueError("Premature deletion of data.  Key: %s" % str(arg))
    else:
        return arg


def execute_task(dsk, key, state, queue, results, lock):
    """ Compute task and handle all administration

    See also:
        _execute_task - actually execute task
    """
    try:
        task = dsk[key]
        result = _execute_task(task, state['cache'], dsk=dsk)
        with lock:
            finish_task(dsk, key, result, state, results)
        result = key, task, result, None
    except Exception as e:
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        result = key, task, e, exc_traceback
    queue.put(result)
    return result


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
            release_data(dep, state)

    state['finished'].add(key)
    state['num-active-threads'] -= 1

    return state


def release_data(key, state):
    """ Remove data from temporary storage """
    if key in state['waiting_data']:
        assert not state['waiting_data'][key]
        del state['waiting_data'][key]

    state['released'].add(key)

    del state['cache'][key]


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
        """ Fire off a task to the thread pool """
        # Update heartbeat
        tick[0] += 1
        # Emit visualization if called for
        if debug_counts and tick[0] % debug_counts == 0:
            visualize(dsk, state, jobs, filename='dask_%d' % tick[0])
        # Choose a good task to compute
        key = choose_task(state)
        state['ready'].remove(key)
        state['num-active-threads'] += 1
        # Submit
        jobs[key] = pool.apply_async(execute_task, args=[dsk, key, state,
            queue, results, lock])


    try:
        # Seed initial tasks into the thread pool
        with lock:
            while state['ready'] and state['num-active-threads'] < nthreads:
                fire_task()

        # Main loop, wait on tasks to finish, insert new ones
        while state['waiting'] or state['ready'] or len(state['finished']) < len(jobs):
            key, finished_task, res, tb = queue.get()
            if isinstance(res, Exception):
                import traceback
                traceback.print_tb(tb)
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
        key, finished_task, res, tb = queue.get()
        # print("Finished %s" % str(finished_task))

    if debug_counts:
        visualize(dsk, state, jobs, filename='dask_end')

    return ndget(result, state['cache'])


def state_to_networkx(dsk, state, jobs):
    import networkx as nx
    from .dot import to_networkx
    data, func = dict(), dict()
    for key in dsk:
        func[key] = {'color': 'gray'}
        data[key] = {'color': 'gray'}

    for key in state['released']:
        data[key] = {'color': 'black'}
    for key in state['cache']:
        assert key in dsk
        data[key] = {'color': 'red'}

    for key in jobs:
        if key in state['finished']:
            func[key] = {'color': 'blue'}
        else:
            func[key] = {'color': 'red'}
    return to_networkx(dsk, data, func)


def visualize(dsk, state, jobs, filename='dask'):
    from dask.dot import dot_graph, write_networkx_to_dot
    g = state_to_networkx(dsk, state, jobs)
    write_networkx_to_dot(g, filename=filename)


def deepmap(func, seq):
    """ Apply function inside nested lists

    >>> deepmap(inc, [[1, 2], [3, 4]])
    [[2, 3], [4, 5]]
    """
    if isinstance(seq, list):
        return [deepmap(func, item) for item in seq]
    else:
        return func(seq)


def double(x):
    return x * 2

def expand_key(dsk, fast, key):
    """

    >>> dsk = {'out': (sum, ['i', 'd']),
    ...        'i': (inc, 'x'),
    ...        'd': (double, 'y'),
    ...        'x': 1, 'y': 1}
    >>> expand_key(dsk, [inc], 'd')
    'd'
    >>> expand_key(dsk, [inc], 'i')  # doctest: +SKIP
    (inc, 'x')
    >>> expand_key(dsk, [inc], ['i', 'd'])  # doctest: +SKIP
    [(inc, 'x'), 'd']
    """
    if isinstance(key, list):
        return [expand_key(dsk, fast, item) for item in key]
    elif key in dsk and istask(dsk[key]) and dsk[key][0] in fast:
        task = dsk[key]
        return (task[0],) + tuple([expand_key(dsk, fast, k) for k in task[1:]])
    else:
        return key


def expand_value(dsk, fast, key):
    """

    >>> dsk = {'out': (sum, ['i', 'd']),
    ...        'i': (inc, 'x'),
    ...        'd': (double, 'y'),
    ...        'x': 1, 'y': 1}
    >>> expand_value(dsk, [inc], 'd')  # doctest: +SKIP
    (double, 'y')
    >>> expand_value(dsk, [inc], 'i')  # doctest: +SKIP
    (inc, 'x')
    >>> expand_value(dsk, [inc], 'out')  # doctest: +SKIP
    (sum, [(inc, 'x'), 'd'])
    """
    task = dsk[key]
    if not istask(task):
        return task
    func, args = task[0], task[1:]
    return (func,) + tuple([expand_key(dsk, fast, arg) for arg in args])


def inline(dsk, fast_functions=None):
    """ Inline cheap functions into larger operations

    >>> dsk = {'out': (add, 'i', 'd'),  # doctest: +SKIP
    ...        'i': (inc, 'x'),
    ...        'd': (double, 'y'),
    ...        'x': 1, 'y': 1}
    >>> inline(dsk, [inc])  # doctest: +SKIP
    {'out': (add, (inc, 'x'), 'd'),
     'd': (double, 'y'),
     'x': 1, 'y': 1}
    """
    if not fast_functions:
        return dsk
    dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    dependents = reverse_dict(dependencies)

    result = {k: expand_value(dsk, fast_functions, k) for k, v in dsk.items()
            if not dependents[k] or not istask(v) or v[0] not in fast_functions}
    return result

'''
    def value(task):
        """

        Given a task like (add, 'i', 'x')
        return a compound task like (add, (inc, 'j'), 'x')
        """
        if not istask(task):
            return task
        func, args = task[0], task[1:]

        new_args = list()
        for arg in args:
            if isinstance(arg, list):
                import pdb; pdb.set_trace()
                new_args.append(deepmap(lambda k: value(dsk.get(k, k)), arg))
                continue
            elif arg not in dsk:
                new_args.append(arg)
                continue
            else:
                subtask = dsk[arg]
                if istask(subtask) and subtask[0] in fast_functions:
                    new_args.append(value(subtask))
                else:
                    new_args.append(arg)

        return (func,) + tuple(new_args)

'''
