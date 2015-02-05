from .core import (istask, get_dependencies, subs, toposort, flatten,
                   reverse_dict, add, inc)


def cull(dsk, keys):
    """ Return new dask with only the tasks required to calculate keys.

    In other words, remove unnecessary tasks from dask.
    ``keys`` may be a single key or list of keys.

    Example
    -------

    >>> d = {'x': 1, 'y': (inc, 'x'), 'out': (add, 'x', 10)}
    >>> cull(d, 'out')  # doctest: +SKIP
    {'x': 1, 'out': (add, 'x', 10)}
    """
    if not isinstance(keys, list):
        keys = [keys]
    nxt = set(flatten(keys))
    seen = nxt
    while nxt:
        cur = nxt
        nxt = set()
        for item in cur:
            for dep in get_dependencies(dsk, item):
                if dep not in seen:
                    nxt.add(dep)
        seen.update(nxt)
    return dict((k, v) for k, v in dsk.items() if k in seen)


def fuse(dsk):
    """ Return new dask with linear sequence of tasks fused together.

    This may be used as an optimization step.

    Example
    -------

    >>> d = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> fuse(d)  # doctest: +SKIP
    {'c': (inc, (inc, 1))}
    """
    # locate all members of linear chains
    parents = {}
    deadbeats = set()
    for parent in dsk:
        deps = get_dependencies(dsk, parent, as_list=True)
        for child in deps:
            if child in parents:
                del parents[child]
                deadbeats.add(child)
            elif len(deps) > 1:
                deadbeats.add(child)
            elif child not in deadbeats:
                parents[child] = parent

    # construct the chains from ancestor to descendant
    chains = []
    children = dict(map(reversed, parents.items()))
    while parents:
        child, parent = parents.popitem()
        chain = [child, parent]
        while parent in parents:
            parent = parents.pop(parent)
            del children[parent]
            chain.append(parent)
        chain.reverse()
        while child in children:
            child = children.pop(child)
            del parents[child]
            chain.append(child)
        chains.append(chain)

    # create a new dask with fused chains
    rv = {}
    fused = set()
    for chain in chains:
        child = chain.pop()
        val = dsk[child]
        while chain:
            parent = chain.pop()
            val = subs(dsk[parent], child, val)
            fused.add(child)
            child = parent
        fused.add(child)
        rv[child] = val

    for key, val in dsk.items():
        if key not in fused:
            rv[key] = val
    return rv


def inline(dsk, keys=None, inline_constants=True):
    """ Return new dask with the given keys inlined with their values.

    Inlines all constants if ``inline_constants`` keyword is True.

    Example
    -------

    >>> d = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'x', 'y')}
    >>> inline(d)  # doctest: +SKIP
    {'y': (inc, 1), 'z': (add, 1, 'y')}

    >>> inline(d, keys='y')  # doctest: +SKIP
    {'z': (add, 1, (inc, 1))}

    >>> inline(d, keys='y', inline_constants=False)  # doctest: +SKIP
    {'x': 1, 'z': (add, 'x', (inc, 'x'))}
    """
    if keys is None:
        keys  = set()
    elif isinstance(keys, list):
        keys = set(keys)
    else:
        keys = set([keys])
    if inline_constants:
        keys.update(k for k, v in dsk.items() if not istask(v))

    # Keys may depend on other keys, so determine replace order with toposort.
    # The values stored in `keysubs` do not include other keys.
    replaceorder = toposort(dict((k, dsk[k]) for k in keys if k in dsk))
    keysubs = {}
    for key in replaceorder:
        val = dsk[key]
        for dep in keys & get_dependencies(dsk, key):
            if dep in keysubs:
                replace = keysubs[dep]
            else:
                replace = dsk[dep]
            val = subs(val, dep, replace)
        keysubs[key] = val

    # Make new dask with substitutions
    rv = {}
    for key, val in dsk.items():
        if key in keys:
            continue
        for item in keys & get_dependencies(dsk, key):
            val = subs(val, item, keysubs[item])
        rv[key] = val
    return rv


def inline_functions(dsk, fast_functions=None, inline_constants=False):
    """ Inline cheap functions into larger operations

    >>> dsk = {'out': (add, 'i', 'd'),  # doctest: +SKIP
    ...        'i': (inc, 'x'),
    ...        'd': (double, 'y'),
    ...        'x': 1, 'y': 1}
    >>> inline_functions(dsk, [inc])  # doctest: +SKIP
    {'out': (add, (inc, 'x'), 'd'),
     'd': (double, 'y'),
     'x': 1, 'y': 1}
    """
    if not fast_functions:
        return dsk
    fast_functions = set(fast_functions)

    dependencies = dict((k, get_dependencies(dsk, k)) for k in dsk)
    dependents = reverse_dict(dependencies)

    keys = [k for k, v in dsk.items()
              if istask(v)
              and functions_of(v).issubset(fast_functions)
              and dependents[k]]
    if keys:
        return inline(dsk, keys, inline_constants=inline_constants)
    else:
        return dsk


def functions_of(task):
    """ Set of functions contained within nested task

    >>> task = (add, (mul, 1, 2), (inc, 3))  # doctest: +SKIP
    >>> functions_of(task)  # doctest: +SKIP
    set([add, mul, inc])
    """
    result = set()
    if istask(task):
        args = set.union(*map(functions_of, task[1:])) if task[1:] else set()
        return set([unwrap_partial(task[0])]) | args
    if isinstance(task, (list, tuple)):
        return set.union(*map(functions_of, task))
    return set()


def unwrap_partial(func):
    while hasattr(func, 'func'):
        func = func.func
    return func

