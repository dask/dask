from itertools import count
from .compatibility import zip_longest
from .core import (istask, get_dependencies, subs, toposort, flatten,
                   reverse_dict, add, inc, ishashable, preorder_traversal)
from .rewrite import END
from toolz import identity


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
    elif isinstance(keys, (set, tuple, list)):
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
        if not task:
            return set()
        return set.union(*map(functions_of, task))
    return set()


def unwrap_partial(func):
    while hasattr(func, 'func'):
        func = func.func
    return func


def dealias(dsk):
    """ Remove aliases from dask

    Removes and renames aliases using ``inline``.  Keeps aliases at the top of
    the DAG to ensure entry points stay the same.

    Aliases are not expected by schedulers.  It's unclear that this is a legal
    state.

    Example
    -------

    >>> dsk = {'a': (range, 5),
    ...        'b': 'a',
    ...        'c': 'b',
    ...        'd': (sum, 'c'),
    ...        'e': 'd',
    ...        'f': (inc, 'd')}

    >>> dealias(dsk)  # doctest: +SKIP
    {'a': (range, 5),
     'd': (sum, 'a'),
     'e': (identity, 'd'),
     'f': (inc, 'd')}
    """
    dependencies = dict((k, get_dependencies(dsk, k)) for k in dsk)
    dependents = reverse_dict(dependencies)

    aliases = set((k for k, task in dsk.items() if ishashable(task) and task in dsk))
    roots = set((k for k, v in dependents.items() if not v))

    dsk2 = inline(dsk, aliases - roots, inline_constants=False)
    dsk3 = dsk2.copy()

    dependencies = dict((k, get_dependencies(dsk2, k)) for k in dsk2)
    dependents = reverse_dict(dependencies)

    for k in roots & aliases:
        k2 = dsk3[k]
        if len(dependents[k2]) == 1:
            dsk3[k] = dsk3[k2]
            del dsk3[k2]
        else:
            dsk3[k] = (identity, k2)
    return dsk3


def equivalent(term1, term2, subs=None):
    """Determine if two terms are equivalent, modulo variable substitution.

    Equivalent to applying substitutions in `subs` to `term2`, then checking if
    `term1 == term2`.

    If a subterm doesn't support comparison (i.e. `term1 == term2` errors),
    returns `False`.

    Parameters
    ----------
    term1, term2 : terms
    subs : dict, optional
        Mapping of substitutions from `term2` to `term1`

    Example
    -------
    >>> from operator import add
    >>> term1 = (add, 'a', 'b')
    >>> term2 = (add, 'x', 'y')
    >>> subs = {'x': 'a', 'y': 'b'}
    >>> equivalent(term1, term2, subs)
    True
    >>> subs = {'x': 'a'}
    >>> equivalent(term1, term2, subs)
    False
    """

    # Quick escape for special cases
    head_type = type(term1)
    if type(term2) != head_type:
        # If terms aren't same type, fail
        return False
    elif head_type not in (tuple, list):
        # For literals, just compare
        try:
            # `is` is tried first, to allow objects that don't implement `==`
            # to work for cases where term1 is term2. If `is` returns False,
            # and `==` errors, then the only thing we can do is return False.
            return term1 is term2 or term1 == term2
        except:
            return False

    pot1 = preorder_traversal(term1)
    pot2 = preorder_traversal(term2)
    subs = {} if subs is None else subs

    for t1, t2 in zip_longest(pot1, pot2, fillvalue=END):
        if t1 is END or t2 is END:
            # If terms aren't same length: fail
            return False
        elif ishashable(t2) and t2 in subs:
            val = subs[t2]
        else:
            val = t2
        try:
            if t1 is not t2 and t1 != val:
                return False
        except:
            return False
    return True


def dependency_dict(dsk):
    """Create a dict matching ordered dependencies to keys.

    Example
    -------
    >>> from operator import add
    >>> dsk = {'a': 1, 'b': 2, 'c': (add, 'a', 'a'), 'd': (add, 'b', 'a')}
    >>> dependency_dict(dsk)    # doctest: +SKIP
    {(): ['a', 'b'], ('a', 'a'): ['c'], ('b', 'a'): ['d']}
    """

    dep_dict = {}
    for key in dsk:
        deps = tuple(get_dependencies(dsk, key, True))
        dep_dict.setdefault(deps, []).append(key)
    return dep_dict


def _possible_matches(dep_dict, deps, subs):
    deps2 = []
    for d in deps:
        v = subs.get(d, None)
        if v is not None:
            deps2.append(v)
        else:
            return []
    deps2 = tuple(deps2)
    return dep_dict.get(deps2, [])


def _sync_keys(dsk1, dsk2, dsk2_topo):
    dep_dict1 = dependency_dict(dsk1)
    subs = {}

    for key2 in toposort(dsk2):
        deps = tuple(get_dependencies(dsk2, key2, True))
        # List of keys in dsk1 that have terms that *may* match key2
        possible_matches = _possible_matches(dep_dict1, deps, subs)
        if possible_matches:
            val2 = dsk2[key2]
            for key1 in possible_matches:
                val1 = dsk1[key1]
                if equivalent(val1, val2, subs):
                    subs[key2] = key1
                    break
    return subs


def sync_keys(dsk1, dsk2):
    """Return a dict matching keys in `dsk2` to equivalent keys in `dsk1`.

    Parameters
    ----------
    dsk1, dsk2 : dict

    Example
    -------
    >>> from operator import add, mul
    >>> dsk1 = {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5)}
    >>> dsk2 = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> sync_keys(dsk1, dsk2)   # doctest: +SKIP
    {'x': 'a', 'y': 'b'}
    """

    return _sync_keys(dsk1, dsk2, toposort(dsk2))


def merge_sync(dsk1, dsk2):
    """Merge two dasks together, combining equivalent tasks.

    If a task in `dsk2` exists in `dsk1`, the task and key from `dsk1` is used.
    If a task in `dsk2` has the same key as a task in `dsk1` (and they aren't
    equivalent tasks), then a new key is created for the task in `dsk2`. This
    prevents name conflicts.

    Parameters
    ----------
    dsk1, dsk2 : dict
        Variable names in `dsk2` are replaced with equivalent ones in `dsk1`
        before merging.

    Returns
    -------
    new_dsk : dict
        The merged dask.
    key_map : dict
        A mapping between the keys from `dsk2` to their new names in `new_dsk`.

    Example
    -------
    >>> from operator import add, mul
    >>> dsk1 = {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5)}
    >>> dsk2 = {'x': 1, 'y': (add, 'x', 10), 'z': (mul, 'y', 2)}
    >>> new_dsk, key_map = merge_sync(dsk1, dsk2)
    >>> new_dsk     # doctest: +SKIP
    {'a': 1, 'b': (add, 'a', 10), 'c': (mul, 'b', 5), 'z': (mul, 'b', 2)}
    >>> key_map     # doctest: +SKIP
    {'x': 'a', 'y': 'b', 'z': 'z'}

    Conflicting names are replaced with auto-generated names upon merging.

    >>> dsk1 = {'a': 1, 'res': (add, 'a', 1)}
    >>> dsk2 = {'x': 1, 'res': (add, 'x', 2)}
    >>> new_dsk, key_map = merge_sync(dsk1, dsk2)
    >>> new_dsk     # doctest: +SKIP
    {'a': 1, 'res': (add, 'a', 1), 'merge_1': (add, 'a', 2)}
    >>> key_map     # doctest: +SKIP
    {'x': 'a', 'res': 'merge_1'}
    """

    dsk2_topo = toposort(dsk2)
    sd = _sync_keys(dsk1, dsk2, dsk2_topo)
    new_dsk = dsk1.copy()
    for key in dsk2_topo:
        if key in sd:
            new_key = sd[key]
        else:
            if key in dsk1:
                new_key = next(merge_sync.names)
            else:
                new_key = key
            sd[key] = new_key
        task = dsk2[key]
        for a, b in sd.items():
            task = subs(task, a, b)
        new_dsk[new_key] = task
    return new_dsk, sd

# store the name iterator in the function
merge_sync.names = ('merge_%d' % i for i in count(1))
