"""
An asynchronous shared-memory scheduler for dask graphs.

This code is experimental and fairly ugly.  It should probably be rewritten
before anyone really depends on it.  It is very stateful and error-prone.

That being said, it is decently fast.

State
=====

Many functions pass around a ``state`` variable that holds the current state of
the computation.  This variable consists of several other dictionaries and
sets, explained below.

Constant state
--------------

1.  dependencies: {x: [a, b ,c]} a,b,c, must be run before x
2.  dependents: {a: [x, y]} a must run before x or y

Changing state
--------------

### Data

1.  cache: available concrete data.  {key: actual-data}
2.  released: data that we've seen, used, and released because it is no longer
    needed

### Jobs

1.  ready: A set of ready-to-run tasks
1.  running: A set of tasks currently in execution
2.  finished: A set of finished tasks
3.  waiting: which tasks are still waiting on others :: {key: {keys}}
    Real-time equivalent of dependencies
4.  waiting_data: available data to yet-to-be-run-tasks :: {key: {keys}}
    Real-time equivalent of dependents


Example
-------

>>> import pprint
>>> dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
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
 'ready': set(['z']),
 'released': set([]),
 'running': set([]),
 'waiting': {'w': set(['z'])},
 'waiting_data': {'x': set(['z']),
                  'y': set(['w']),
                  'z': set(['w'])}}

Optimizations
=============

We build this scheduler with out-of-core array operations in mind.  To this end
we have encoded some particular optimizations.

Compute to release data
-----------------------

When we choose a new task to execute we often have many options.  Policies at
this stage are cheap and can significantly impact performance.  One could
imagine policies that expose parallelism, drive towards a paticular output,
etc..  Our current policy is the compute tasks that free up data resources.

See the functions ``choose_task`` and ``score`` for more information


Inlining computations
---------------------

We hold on to intermediate computations either in memory or on disk.

For very cheap computations that may emit new copies of the data, like
``np.transpose`` or possibly even ``x + 1`` we choose not to store these as
separate pieces of data / tasks.  Instead we combine them with the computations
that require them.  This may result in repeated computation but saves
significantly on space and computation complexity.

See the function ``inline_functions`` for more information.
"""
from __future__ import absolute_import, division, print_function

import psutil
import sys
import traceback
from operator import add
from toolz import concat, partial
from .core import istask, flatten, reverse_dict, get_dependencies, ishashable
from .optimize import inline_functions
from .utils import deepmap

def inc(x):
    return x + 1

def double(x):
    return x * 2

DEBUG = False

def start_state_from_dask(dsk, cache=None):
    """ Start state from a dask

    Example
    -------

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
     'ready': set(['z']),
     'released': set([]),
     'running': set([]),
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

    dependencies = dict((k, get_dependencies(dsk, k)) for k in dsk)
    waiting = dict((k, v.copy()) for k, v in dependencies.items()
                                 if k not in cache)

    dependents = reverse_dict(dependencies)
    for a in cache:
        for b in dependents[a]:
            waiting[b].remove(a)
    waiting_data = dict((k, v.copy()) for k, v in dependents.items() if v)

    ready = set([k for k, v in waiting.items() if not v])
    waiting = dict((k, v) for k, v in waiting.items() if v)

    state = {'dependencies': dependencies,
             'dependents': dependents,
             'waiting': waiting,
             'waiting_data': waiting_data,
             'cache': cache,
             'ready': ready,
             'running': set(),
             'finished': set(),
             'released': set()}

    return state


'''
Running tasks
-------------

When we execute tasks we both

1.  Perform the actual work of collecting the appropriate data and calling the function
2.  Manage administrative state to coordinate with the scheduler
'''

def _execute_task(arg, cache, dsk=None):
    """ Do the actual work of collecting data and executing a function

    Examples
    --------

    >>> cache = {'x': 1, 'y': 2}

    Compute tasks against a cache
    >>> _execute_task((add, 'x', 1), cache)  # Compute task in naive manner
    2
    >>> _execute_task((add, (inc, 'x'), 1), cache)  # Support nested computation
    3

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
    if isinstance(arg, list):
        return (_execute_task(a, cache) for a in arg)
    elif istask(arg):
        func, args = arg[0], arg[1:]
        args2 = [_execute_task(a, cache) for a in args]
        return func(*args2)
    elif not ishashable(arg):
        return arg
    elif arg in cache:
        return cache[arg]
    else:
        return arg


def execute_task(key, task, data, queue):
    """
    Compute task and handle all administration

    See also:
        _execute_task - actually execute task
    """
    try:
        result = _execute_task(task, data)
        result = key, result, None
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = ''.join(traceback.format_tb(exc_traceback))
        result = key, e, tb
    try:
        queue.put(result)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = ''.join(traceback.format_tb(exc_traceback))
        queue.put((key, e, tb))


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
                if DEBUG:
                    from chest.core import nbytes
                    print("Key: %s\tDep: %s\t NBytes: %.2f\t Release" % (key, dep,
                        sum(map(nbytes, state['cache'].values()) / 1e6)))
                assert dep in state['cache']
                release_data(dep, state)
                assert dep not in state['cache']
        elif dep in state['cache'] and dep not in results:
            release_data(dep, state)

    state['finished'].add(key)
    state['running'].remove(key)

    return state


def release_data(key, state):
    """ Remove data from temporary storage

    See Also
        finish_task
    """
    if key in state['waiting_data']:
        assert not state['waiting_data'][key]
        del state['waiting_data'][key]

    state['released'].add(key)

    del state['cache'][key]


def nested_get(ind, coll, lazy=False):
    """ Get nested index from collection

    Examples
    --------

    >>> nested_get(1, 'abc')
    'b'
    >>> nested_get([1, 0], 'abc')
    ('b', 'a')
    >>> nested_get([[1, 0], [0, 1]], 'abc')
    (('b', 'a'), ('a', 'b'))
    """
    if isinstance(ind, list):
        if lazy:
            return (nested_get(i, coll, lazy=lazy) for i in ind)
        else:
            return tuple([nested_get(i, coll, lazy=lazy) for i in ind])
        return seq
    else:
        return coll[ind]

'''
Task Selection
--------------

We often have a choice among many tasks to run next.  This choice is both
cheap and can significantly impact performance.

Here we choose tasks that immediately free data resources.
'''

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

'''
`get`
-----

The main function of the scheduler.  Get is the main entry point.
'''

def get_async(apply_async, num_workers, dsk, result, cache=None,
                debug_counts=None, queue=None, **kwargs):
    """ Asynchronous get function

    Parameters
    ----------

    apply_async : function
        Asynchronous apply function as found on Pool or ThreadPool
    num_workers : int
        The number of active tasks we should have at any one time
    dsk: dict
        A dask dictionary specifying a workflow
    result: key or list of keys
        Keys corresponding to desired data
    cache: dict-like (optional)
        Temporary storage of results
    debug_counts: integer or None
        This integer tells how often the scheduler should dump debugging info

    See Also
    --------

    threaded.get
    """
    assert queue

    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    results = set(result_flat)

    state = start_state_from_dask(dsk, cache=cache)

    tick = [0]

    if state['waiting'] and not state['ready']:
        raise ValueError("Found no accessible jobs in dask")

    def fire_task():
        """ Fire off a task to the thread pool """
        # Update heartbeat
        tick[0] += 1
        # Emit visualization if called for
        if debug_counts and tick[0] % debug_counts == 0:
            visualize(dsk, state, filename='dask_%03d' % tick[0])
        # Choose a good task to compute
        key = choose_task(state)
        state['ready'].remove(key)
        state['running'].add(key)

        # Prep data to send
        data = dict((dep, state['cache'][dep])
                    for dep in get_dependencies(dsk, key))
        # Submit
        apply_async(execute_task, args=[key, dsk[key], data, queue])

    # Seed initial tasks into the thread pool
    while state['ready'] and len(state['running']) < num_workers:
        fire_task()

    # Main loop, wait on tasks to finish, insert new ones
    while state['waiting'] or state['ready'] or state['running']:
        key, res, tb = queue.get()
        if isinstance(res, Exception):
            raise type(res)(" Execption in remote Process\n\n"
                + str(res) + "\n\nTraceback:\n" + tb)
        finish_task(dsk, key, res, state, results)
        while state['ready'] and len(state['running']) < num_workers:
            fire_task()

    # Final reporting
    while state['running'] or not queue.empty():
        key, res, tb = queue.get()

    if debug_counts:
        visualize(dsk, state, filename='dask_end')

    return nested_get(result, state['cache'])


'''
Debugging
---------

The threaded nature of this project presents challenging to normal unit-test
and debug workflows.  Visualization of the execution state has value.

Our main mechanism is a visualization of the execution state as colors on our
normal dot graphs (see dot module).
'''

def visualize(dsk, state, filename='dask'):
    """ Visualize state of compputation as dot graph """
    from dask.dot import dot_graph, write_networkx_to_dot
    g = state_to_networkx(dsk, state)
    write_networkx_to_dot(g, filename=filename)


def color_nodes(dsk, state):
    data, func = dict(), dict()
    for key in dsk:
        func[key] = {'color': 'gray'}
        data[key] = {'color': 'gray'}

    for key in state['released']:
        data[key] = {'color': 'blue'}

    for key in state['cache']:
        data[key] = {'color': 'red'}

    for key in state['finished']:
            func[key] = {'color': 'blue'}
    for key in state['running']:
            func[key] = {'color': 'red'}

    for key in dsk:
        func[key]['penwidth'] = 4
        data[key]['penwidth'] = 4
    return data, func


def state_to_networkx(dsk, state):
    """ Convert state to networkx for visualization

    See Also:
        visualize
    """
    from .dot import to_networkx
    data, func = color_nodes(dsk, state)
    return to_networkx(dsk, data_attributes=data, function_attributes=func)


def apply_sync(func, args=(), kwds={}):
    """ A naive synchronous version of apply_async """
    return func(*args, **kwds)


def get_sync(dsk, keys, **kwargs):
    from .compatibility import Queue
    queue = Queue()
    return get_async(apply_sync, 1, dsk, keys, queue=queue, **kwargs)
