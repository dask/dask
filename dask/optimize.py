from __future__ import absolute_import, division, print_function

from itertools import count
from operator import getitem

from .compatibility import zip_longest

from .core import add, inc  # noqa: F401
from .core import (istask, get_dependencies, subs, toposort, flatten,
                   reverse_dict, ishashable, preorder_traversal)
from .rewrite import END


def cull(dsk, keys):
    """ Return new dask with only the tasks required to calculate keys.

    In other words, remove unnecessary tasks from dask.
    ``keys`` may be a single key or list of keys.

    Examples
    --------
    >>> d = {'x': 1, 'y': (inc, 'x'), 'out': (add, 'x', 10)}
    >>> dsk, dependencies = cull(d, 'out')  # doctest: +SKIP
    >>> dsk  # doctest: +SKIP
    {'x': 1, 'out': (add, 'x', 10)}
    >>> dependencies  # doctest: +SKIP
    {'x': set(), 'out': set(['x'])}

    Returns
    -------
    dsk: culled dask graph
    dependencies: Dict mapping {key: [deps]}.  Useful side effect to accelerate
        other optimizations, notably fuse.
    """
    if not isinstance(keys, (list, set)):
        keys = [keys]
    out_keys = []
    seen = set()
    dependencies = dict()

    work = list(set(flatten(keys)))
    while work:
        new_work = []
        out_keys += work
        deps = [(k, get_dependencies(dsk, k, as_list=True))  # fuse needs lists
                for k in work]
        dependencies.update(deps)
        for _, deplist in deps:
            for d in deplist:
                if d not in seen:
                    seen.add(d)
                    new_work.append(d)
        work = new_work

    out = {k: dsk[k] for k in out_keys}

    return out, dependencies


def fuse(dsk, keys=None, dependencies=None):
    """ Return new dask graph with linear sequence of tasks fused together.

    If specified, the keys in ``keys`` keyword argument are *not* fused.
    Supply ``dependencies`` from output of ``cull`` if available to avoid
    recomputing dependencies.

    Parameters
    ----------
    dsk: dict
    keys: list
    dependencies: dict, optional
        {key: [list-of-keys]}.  Must be a list to provide count of each key
        This optional input often comes from ``cull``

    Examples
    --------
    >>> d = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dsk, dependencies = fuse(d)
    >>> dsk # doctest: +SKIP
    {'c': (inc, (inc, 1))}
    >>> dsk, dependencies = fuse(d, keys=['b'])
    >>> dsk  # doctest: +SKIP
    {'b': (inc, 1), 'c': (inc, 'b')}

    Returns
    -------
    dsk: output graph with keys fused
    dependencies: dict mapping dependencies after fusion.  Useful side effect
        to accelerate other downstream optimizations.
    """
    if keys is not None and not isinstance(keys, set):
        if not isinstance(keys, list):
            keys = [keys]
        keys = set(flatten(keys))

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k, as_list=True)
                        for k in dsk}

    # locate all members of linear chains
    child2parent = {}
    unfusible = set()
    for parent in dsk:
        deps = dependencies[parent]
        has_many_children = len(deps) > 1
        for child in deps:
            if keys is not None and child in keys:
                unfusible.add(child)
            elif child in child2parent:
                del child2parent[child]
                unfusible.add(child)
            elif has_many_children:
                unfusible.add(child)
            elif child not in unfusible:
                child2parent[child] = parent

    # construct the chains from ancestor to descendant
    chains = []
    parent2child = dict(map(reversed, child2parent.items()))
    while child2parent:
        child, parent = child2parent.popitem()
        chain = [child, parent]
        while parent in child2parent:
            parent = child2parent.pop(parent)
            del parent2child[parent]
            chain.append(parent)
        chain.reverse()
        while child in parent2child:
            child = parent2child.pop(child)
            del child2parent[child]
            chain.append(child)
        chains.append(chain)

    dependencies = dict((k, set(v)) for k, v in dependencies.items())

    # create a new dask with fused chains
    rv = {}
    fused = set()
    for chain in chains:
        child = chain.pop()
        val = dsk[child]
        while chain:
            parent = chain.pop()
            dependencies[parent].update(dependencies.pop(child))
            dependencies[parent].remove(child)
            val = subs(dsk[parent], child, val)
            fused.add(child)
            child = parent
        fused.add(child)
        rv[child] = val

    for key, val in dsk.items():
        if key not in fused:
            rv[key] = val
    return rv, dependencies


def _flat_set(x):
    if x is None:
        return set()
    elif isinstance(x, set):
        return x
    elif not isinstance(x, (list, set)):
        x = [x]
    return set(x)


def inline(dsk, keys=None, inline_constants=True, dependencies=None):
    """ Return new dask with the given keys inlined with their values.

    Inlines all constants if ``inline_constants`` keyword is True. Note that
    the constant keys will remain in the graph, to remove them follow
    ``inline`` with ``cull``.

    Examples
    --------
    >>> d = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'x', 'y')}
    >>> inline(d)  # doctest: +SKIP
    {'x': 1, 'y': (inc, 1), 'z': (add, 1, 'y')}

    >>> inline(d, keys='y')  # doctest: +SKIP
    {'x': 1, 'y': (inc, 1), 'z': (add, 1, (inc, 1))}

    >>> inline(d, keys='y', inline_constants=False)  # doctest: +SKIP
    {'x': 1, 'y': (inc, 1), 'z': (add, 'x', (inc, 'x'))}
    """
    if dependencies and isinstance(next(iter(dependencies.values())), list):
        dependencies = {k: set(v) for k, v in dependencies.items()}

    keys = _flat_set(keys)

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k)
                        for k in dsk}

    if inline_constants:
        keys.update(k for k, v in dsk.items() if
                    (ishashable(v) and v in dsk) or
                    (not dependencies[k] and not istask(v)))

    # Keys may depend on other keys, so determine replace order with toposort.
    # The values stored in `keysubs` do not include other keys.
    replaceorder = toposort(dict((k, dsk[k]) for k in keys if k in dsk),
                            dependencies=dependencies)
    keysubs = {}
    for key in replaceorder:
        val = dsk[key]
        for dep in keys & dependencies[key]:
            if dep in keysubs:
                replace = keysubs[dep]
            else:
                replace = dsk[dep]
            val = subs(val, dep, replace)
        keysubs[key] = val

    # Make new dask with substitutions
    dsk2 = keysubs.copy()
    for key, val in dsk.items():
        if key not in dsk2:
            for item in keys & dependencies[key]:
                val = subs(val, item, keysubs[item])
            dsk2[key] = val
    return dsk2


def inline_functions(dsk, output, fast_functions=None, inline_constants=False,
                     dependencies=None):
    """ Inline cheap functions into larger operations

    Examples
    --------
    >>> dsk = {'out': (add, 'i', 'd'),  # doctest: +SKIP
    ...        'i': (inc, 'x'),
    ...        'd': (double, 'y'),
    ...        'x': 1, 'y': 1}
    >>> inline_functions(dsk, [], [inc])  # doctest: +SKIP
    {'out': (add, (inc, 'x'), 'd'),
     'd': (double, 'y'),
     'x': 1, 'y': 1}

    Protect output keys.  In the example below ``i`` is not inlined because it
    is marked as an output key.

    >>> inline_functions(dsk, ['i', 'out'], [inc, double])  # doctest: +SKIP
    {'out': (add, 'i', (double, 'y')),
     'i': (inc, 'x'),
     'x': 1, 'y': 1}
    """
    if not fast_functions:
        return dsk

    output = set(output)

    fast_functions = set(fast_functions)

    if dependencies is None:
        dependencies = {k: get_dependencies(dsk, k)
                        for k in dsk}
    dependents = reverse_dict(dependencies)

    keys = [k for k, v in dsk.items()
            if istask(v) and functions_of(v).issubset(fast_functions) and
            dependents[k] and k not in output
            ]

    if keys:
        dsk = inline(dsk, keys, inline_constants=inline_constants,
                     dependencies=dependencies)
        for k in keys:
            del dsk[k]
    return dsk


def unwrap_partial(func):
    while hasattr(func, 'func'):
        func = func.func
    return func


def functions_of(task):
    """ Set of functions contained within nested task

    Examples
    --------
    >>> task = (add, (mul, 1, 2), (inc, 3))  # doctest: +SKIP
    >>> functions_of(task)  # doctest: +SKIP
    set([add, mul, inc])
    """
    funcs = set()

    work = [task]
    sequence_types = {list, tuple}

    while work:
        new_work = []
        for task in work:
            if type(task) in sequence_types:
                if istask(task):
                    funcs.add(unwrap_partial(task[0]))
                    new_work += task[1:]
                else:
                    new_work += task
        work = new_work

    return funcs


def dealias(dsk, keys=None, dependencies=None):
    """ Remove aliases from dask

    Removes and renames aliases using ``inline``. Optional ``keys`` keyword
    argument protects keys from being deleted. This is useful to protect keys
    that would be expected by a scheduler. If not provided, all inlined aliases
    are removed.

    Examples
    --------
    >>> dsk = {'a': (range, 5),
    ...        'b': 'a',
    ...        'c': 'b',
    ...        'd': (sum, 'c'),
    ...        'e': 'd',
    ...        'f': (inc, 'd')}

    >>> dealias(dsk)  # doctest: +SKIP
    {'a': (range, 5),
     'd': (sum, 'a'),
     'f': (inc, 'd')}

    >>> dsk = {'a': (range, 5),
    ...        'b': 'a'}
    >>> dealias(dsk)  # doctest: +SKIP
    {'a': (range, 5)}

    >>> dealias(dsk, keys=['a', 'b'])  # doctest: +SKIP
    {'a': (range, 5),
     'b': 'a'}
    """
    keys = keys or set()
    if not isinstance(keys, set):
        keys = set(keys)

    if not dependencies:
        dependencies = {k: get_dependencies(dsk, k)
                        for k in dsk}

    aliases = set(k for k, task in dsk.items() if
                  ishashable(task) and task in dsk)

    dsk2 = inline(dsk, aliases, inline_constants=False)
    for k in aliases.difference(keys):
        del dsk2[k]
    return dsk2


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

    Examples
    --------
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

    Examples
    --------
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

    Examples
    --------
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

    Examples
    --------
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


def fuse_selections(dsk, head1, head2, merge):
    """Fuse selections with lower operation.

    Handles graphs of the form:
    ``{key1: (head1, key2, ...), key2: (head2, ...)}``

    Parameters
    ----------
    dsk : dict
        dask graph
    head1 : function
        The first element of task1
    head2 : function
        The first element of task2
    merge : function
        Takes ``task1`` and ``task2`` and returns a merged task to
        replace ``task1``.

    Examples
    --------
    >>> def load(store, partition, columns):
    ...     pass
    >>> dsk = {'x': (load, 'store', 'part', ['a', 'b']),
    ...        'y': (getitem, 'x', 'a')}
    >>> merge = lambda t1, t2: (load, t2[1], t2[2], t1[2])
    >>> dsk2 = fuse_selections(dsk, getitem, load, merge)
    >>> cull(dsk2, 'y')[0]
    {'y': (<function load at ...>, 'store', 'part', 'a')}
    """
    dsk2 = dict()
    for k, v in dsk.items():
        try:
            if (istask(v) and v[0] == head1 and v[1] in dsk and
                    istask(dsk[v[1]]) and dsk[v[1]][0] == head2):
                dsk2[k] = merge(v, dsk[v[1]])
            else:
                dsk2[k] = v
        except TypeError:
            dsk2[k] = v
    return dsk2


def fuse_getitem(dsk, func, place):
    """ Fuse getitem with lower operation

    Parameters
    ----------
    dsk: dict
        dask graph
    func: function
        A function in a task to merge
    place: int
        Location in task to insert the getitem key

    Examples
    --------
    >>> def load(store, partition, columns):
    ...     pass
    >>> dsk = {'x': (load, 'store', 'part', ['a', 'b']),
    ...        'y': (getitem, 'x', 'a')}
    >>> dsk2 = fuse_getitem(dsk, load, 3)  # columns in arg place 3
    >>> cull(dsk2, 'y')[0]
    {'y': (<function load at ...>, 'store', 'part', 'a')}
    """
    return fuse_selections(dsk, getitem, func,
                           lambda a, b: tuple(b[:place]) + (a[2], ) + tuple(b[place + 1:]))
