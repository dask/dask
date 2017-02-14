from __future__ import absolute_import, division, print_function

import math
import re
from operator import getitem

from .compatibility import unicode

from .context import _globals
from .core import add, inc  # noqa: F401
from .core import (istask, get_dependencies, subs, toposort, flatten,
                   reverse_dict, ishashable)


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


def default_fused_keys_renamer(keys):
    """Create new keys for fused tasks"""
    typ = type(keys[0])
    if typ is str or typ is unicode:
        names = [key_split(x) for x in keys[:0:-1]]
        names.append(keys[0])
        return '-'.join(names)
    elif (typ is tuple and len(keys[0]) > 0 and
          isinstance(keys[0][0], (str, unicode))):
        names = [key_split(x) for x in keys[:0:-1]]
        names.append(keys[0][0])
        return ('-'.join(names),) + keys[0][1:]
    else:
        return None


def fuse(dsk, keys=None, dependencies=None, rename_fused_keys=True):
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
    rename_fused_keys: bool or func, optional
        Whether to rename the fused keys with ``default_fused_keys_renamer``
        or not.  Renaming fused keys can keep the graph more understandable
        and comprehensive, but it comes at the cost of additional processing.
        If False, then the top-most key will be used.  For advanced usage, a
        func is also accepted, ``new_key = rename_fused_keys(fused_key_list)``.

    Examples
    --------
    >>> d = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b')}
    >>> dsk, dependencies = fuse(d)
    >>> dsk # doctest: +SKIP
    {'a-b-c': (inc, (inc, 1)), 'c': 'a-b-c'}
    >>> dsk, dependencies = fuse(d, rename_fused_keys=False)
    >>> dsk # doctest: +SKIP
    {'c': (inc, (inc, 1))}
    >>> dsk, dependencies = fuse(d, keys=['b'], rename_fused_keys=False)
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

    dependencies = {k: set(v) for k, v in dependencies.items()}

    if rename_fused_keys is True:
        key_renamer = default_fused_keys_renamer
    elif rename_fused_keys is False:
        key_renamer = None
    else:
        key_renamer = rename_fused_keys

    # create a new dask with fused chains
    rv = {}
    fused = set()
    aliases = set()
    is_renamed = False
    for chain in chains:
        if key_renamer is not None:
            new_key = key_renamer(chain)
            is_renamed = (new_key is not None and new_key not in dsk and
                          new_key not in rv)
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
        if is_renamed:
            rv[new_key] = val
            rv[child] = new_key
            dependencies[new_key] = dependencies[child]
            dependencies[child] = {new_key}
            aliases.add(child)
        else:
            rv[child] = val
    for key, val in dsk.items():
        if key not in fused:
            rv[key] = val
    if aliases:
        for key, deps in dependencies.items():
            for old_key in deps & aliases:
                new_key = rv[old_key]
                deps.remove(old_key)
                deps.add(new_key)
                rv[key] = subs(rv[key], old_key, new_key)
        if keys is not None:
            for key in aliases - keys:
                del rv[key]
                del dependencies[key]
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


def fuse_reductions(dsk, keys=None, dependencies=None, ave_width=None, max_width=None,
                    max_depth_new_edges=None, max_height=None, rename_fused_keys=False):
    """ WIP. Probably broken.  Fuse tasks that form reductions.

    This trades parallelism opportunities for faster scheduling by making
    tasks less granular.

    There are many options (and opinions) for what the parameterization
    of this function should be.  For the sake of experimentation, I have
    included many parameters as described below.  My gut feeling right
    now is that we only need one parameter, `ave_width`, and to choose
    a reasonable value of `max_depth_new_edges` based on `ave_width`
    (this controls against pathologies while allowing desirable behavior).

    This optimization operation is more general than "single input, single
    output".  It applies to all reductions, so it may be "multiple input,
    single output".  I have attempted to make this well-behaved and
    robust against pathologies.  Notably, by allowing multiple inputs
    (what I refer to as `edges` in the code), there may be parallelism
    opportunities w.r.t. the edges that are otherwise not captured by
    analyzing the fusible reduction tasks alone.  I added a cheap-to-
    compute heuristic that is conservative; i.e., it will never
    underestimate the degree of edge parallelism.  This heuristic
    increases the value compared against `min_width`--a measure of
    parallizability--which is one of the reasons I prefer `min_width`.

    I think it will be easy for this operation to supercede `fuse`.
    I'm ignoring task renaming for now.

    Parameters
    ----------
    dsk: dict
        dask graph
    keys: list or set
        Keys that must remain in the dask graph
    ave_width: float
        Limit for `width = num_nodes / height`, a good measure of
        parallelizability.
    max_depth_new_edges: int
        Don't fuse if new dependencies are added after this many levels
    max_height: int
        Don't fuse more than this many levels
    max_width: int
        Don't fuse if total width is greater than this

    """
    # XXX: uncomment to allow `fuse_reducible` to run against the `fuse` tests
    # if rename_fused_keys:
    #     return fuse(dsk, keys=keys, dependencies=dependencies, rename_fused_keys=True)
    if keys is not None and not isinstance(keys, set):
        if not isinstance(keys, list):
            keys = [keys]
        keys = set(flatten(keys))

    # Assign reasonable, not too restrictive defaults
    ave_width = ave_width or _globals.get('fuse_ave_width') or 1
    max_height = max_height or _globals.get('fuse_max_height') or len(dsk)
    max_depth_new_edges = (
        max_depth_new_edges or
        _globals.get('fuse_max_depth_new_edges') or
        ave_width + 1.5
    )
    max_width = (
        max_width or
        _globals.get('fuse_max_width') or
        1.5 + ave_width * math.log(ave_width + 1)
    )

    if dependencies is None:
        deps = {k: get_dependencies(dsk, k, as_list=True) for k in dsk}
    else:
        deps = dict(dependencies)

    rdeps = {}
    for k, vals in deps.items():
        for v in vals:
            if v not in rdeps:
                rdeps[v] = []
            rdeps[v].append(k)
        deps[k] = set(vals)

    reducible = {k for k, vals in rdeps.items() if len(vals) == 1}
    if keys:
        reducible -= keys
    if not reducible:
        return dsk, deps

    rv = dsk.copy()
    # These are the stacks we use to store data as we traverse the graph
    info_stack = []
    children_stack = []
    while reducible:
        child = next(iter(reducible))
        parent = rdeps[child][0]
        children_stack.append(parent)
        children_stack.extend(reducible & deps[parent])
        while True:
            child = children_stack.pop()
            if child != parent:
                children = reducible & deps[child]
                if children:
                    # Depth-first search
                    children_stack.append(child)
                    children_stack.extend(children)
                    parent = child
                else:
                    # This is a leaf node in the reduction region
                    # key, task, height, width, number of nodes, set of edges
                    info_stack.append((child, dsk[child], 0, 1, 1, deps[child] - reducible))
            else:
                # Calculate metrics and fuse as appropriate
                edges = deps[parent] - reducible
                children = deps[parent] - edges
                num_children = len(children)
                height = 1
                width = 0
                num_single_nodes = 0
                num_nodes = 0
                children_edges = set()
                max_num_edges = 0
                children_info = info_stack[-num_children:]
                del info_stack[-num_children:]
                for cur_key, cur_task, cur_height, cur_width, cur_num_nodes, cur_edges in children_info:
                    if cur_height == 0:
                        num_single_nodes += 1
                    elif cur_height > height:
                        height = cur_height
                    width += cur_width
                    num_nodes += cur_num_nodes
                    if len(cur_edges) > max_num_edges:
                        max_num_edges = len(cur_edges)
                    children_edges |= cur_edges

                edges |= children_edges
                # Fudge factor to account for possible parallelism with the boundaries
                num_nodes += min(num_children - 1, max(0, len(edges) - max_num_edges))
                if (
                    width <= max_width and
                    height <= max_height and
                    num_single_nodes <= ave_width and
                    num_nodes / height <= ave_width and
                    # Sanity check; don't go too deep if new levels introduce new edge dependencies
                    (len(edges) == len(children_edges) or height < max_depth_new_edges)
                ):
                    # Perform substitutions as we go
                    val = dsk[parent]
                    children_deps = set()
                    for child_info in children_info:
                        cur_child = child_info[0]
                        val = subs(val, cur_child, child_info[1])
                        del rv[cur_child]
                        children_deps.update(deps.pop(cur_child))
                        reducible.remove(cur_child)
                    deps[parent] -= children
                    deps[parent] |= children_deps
                    if parent in reducible:
                        # key, task, height, width, number of nodes, set of edges
                        if num_children == 1 and len(edges) == len(children_edges):
                            # Linear fuse
                            info_stack.append((parent, val, height, width, num_nodes, edges))
                        else:
                            info_stack.append((parent, val, height + 1, width, num_nodes + 1, edges))
                    else:
                        rv[parent] = val
                        break
                else:
                    for child_info in children_info:
                        rv[child_info[0]] = child_info[1]
                        reducible.remove(child_info[0])
                    if parent in reducible:
                        # Allow the parent to be fused, but only under strict circumstances
                        if width > max_width:
                            width = max_width
                        if width > 1:
                            width -= 1
                        # key, task, height, width, number of nodes, set of edges
                        # This task *implicitly* depends on `edges`
                        info_stack.append((parent, dsk[parent], 0, width, width, edges))
                    else:
                        break

                # Traverse upwards
                parent = rdeps[parent][0]
                if not children_stack:
                    siblings = reducible & deps[parent]
                    siblings.remove(child)
                    children_stack.append(parent)
                    children_stack.extend(siblings)
    return rv, deps


# Defining `key_split` (used by `default_fused_keys_renamer`) in utils.py
# results in messy circular imports, so define it here instead.
hex_pattern = re.compile('[a-f]+')


def key_split(s):
    """
    >>> key_split('x')
    u'x'
    >>> key_split('x-1')
    u'x'
    >>> key_split('x-1-2-3')
    u'x'
    >>> key_split(('x-2', 1))
    'x'
    >>> key_split("('x-2', 1)")
    u'x'
    >>> key_split('hello-world-1')
    u'hello-world'
    >>> key_split(b'hello-world-1')
    u'hello-world'
    >>> key_split('ae05086432ca935f6eba409a8ecd4896')
    'data'
    >>> key_split('<module.submodule.myclass object at 0xdaf372')
    u'myclass'
    >>> key_split(None)
    'Other'
    >>> key_split('x-abcdefab')  # ignores hex
    u'x'
    """
    if type(s) is bytes:
        s = s.decode()
    if type(s) is tuple:
        s = s[0]
    try:
        words = s.split('-')
        if not words[0][0].isalpha():
            result = words[0].lstrip("'(\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (len(word) == 8 and
                                       hex_pattern.match(word) is not None):
                result += '-' + word
            else:
                break
        if len(result) == 32 and re.match(r'[a-f0-9]{32}', result):
            return 'data'
        else:
            if result[0] == '<':
                result = result.strip('<>').split()[0].split('.')[-1]
            return result
    except Exception:
        return 'Other'
