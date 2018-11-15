import itertools
import numbers

import numpy as np
import toolz

from .. import base, core, sharedict, utils
from ..compatibility import apply, Mapping
from ..delayed import to_task_dask
from ..optimization import SubgraphCallable


def subs(task, substitution):
    """ Create a new task with the values substituted

    This is like dask.core.subs, but takes a dict of many substitutions to
    perform simultaneously.  It is not as concerned with micro performance.
    """
    if isinstance(task, dict):
        return {k: subs(v, substitution) for k, v in task.items()}
    if type(task) in (tuple, list, set):
        return type(task)([subs(x, substitution) for x in task])
    try:
        return substitution[task]
    except (KeyError, TypeError):
        return task


def index_subs(ind, substitution):
    """ A simple subs function that works both on tuples and strings """
    if ind is None:
        return ind
    else:
        return tuple([substitution.get(c, c) for c in ind])


def atop_token(i, prefix='_'):
    return prefix + '%d' % i


def _top(func, output, output_indices, *arrind_pairs, **kwargs):
    """ Create a TOP symbolic mutable mapping, given the inputs to top

    This is like the ``top`` function, but rather than construct a dict, it
    returns a symbolic TOP object.

    See Also
    --------
    top
    TOP
    """
    numblocks = kwargs.pop('numblocks')
    concatenate = kwargs.pop('concatenate', None)
    new_axes = kwargs.pop('new_axes', {})

    graph = sharedict.ShareDict()

    # Transform indices to canonical elements
    # We use terms like _0, and _1 rather than provided index elements
    arrind_pairs = list(arrind_pairs)
    unique_indices = {i for ii in arrind_pairs[1::2]
                      if ii is not None
                      for i in ii} | set(output_indices)
    sub = {k: atop_token(i, '.')
           for i, k in enumerate(sorted(unique_indices))}
    output_indices = index_subs(tuple(output_indices), sub)
    arrind_pairs[1::2] = [tuple(a) if a is not None else a
                          for a in arrind_pairs[1::2]]
    arrind_pairs[1::2] = [index_subs(a, sub)
                          for a in arrind_pairs[1::2]]
    new_axes = {index_subs((k,), sub)[0]: v for k, v in new_axes.items()}

    # Unpack dask values in non-array arguments
    argpairs = list(toolz.partition(2, arrind_pairs))
    for i, (arg, ind) in enumerate(argpairs):
        if ind is None:
            arg2, dsk2 = to_task_dask(arg)
            if dsk2:
                graph.update(dsk2)
                argpairs[i] = (arg2, ind)

    # separate argpairs into two separate tuples
    inputs = tuple([name for name, _ in argpairs])
    inputs_indices = tuple([index for _, index in argpairs])

    # Unpack delayed objects in kwargs
    if kwargs:
        kwargs, dsk_kwargs = to_task_dask(kwargs)

        # replace keys in kwargs with _0 tokens
        new_keys = list(core.get_dependencies(dsk_kwargs, task=kwargs))
        new_tokens = tuple(atop_token(i) for i in range(len(inputs), len(inputs) + len(new_keys)))
        sub = dict(zip(new_keys, new_tokens))
        inputs = inputs + tuple(new_keys)
        inputs_indices = inputs_indices + (None,) * len(new_keys)
        kwargs = subs(kwargs, sub)
        graph.update(dsk_kwargs)

    indices = [(k, v) for k, v in zip(inputs, inputs_indices)]
    keys = tuple(map(atop_token, range(len(inputs))))

    # Construct local graph
    if not kwargs:
        dsk = {output: (func,) + keys}
    else:
        _keys = list(keys)
        if new_keys:
            _keys = _keys[:-len(new_keys)]
        dsk = {output: (apply, func, _keys, kwargs)}

    # Construct final output
    top = TOP(output, output_indices, dsk, indices,
              numblocks=numblocks, concatenate=concatenate, new_axes=new_axes)
    graph.update_with_key(top, output)
    graph.dependencies = {output: {arg for arg, ind in argpairs if ind is not None}}
    return graph


class TOP(Mapping):
    """ Tensor Operation

    This is a lazily constructed mapping for tensor operation graphs.
    This defines a dictionary using an operation and an indexing pattern.
    It is built for many operations like elementwise, transpose, tensordot, and
    so on.  We choose to keep these as symbolic mappings rather than raw
    dictionaries because we are able to fuse them during optimization,
    sometimes resulting in much lower overhead.

    See Also
    --------
    top
    atop
    """
    def __init__(self, output, output_indices, dsk, indices,
                 numblocks, concatenate=None, new_axes=None):
        self.output = output
        self.output_indices = tuple(output_indices)
        self.dsk = dsk
        self.indices = tuple((name, tuple(ind) if ind is not None else ind)
                             for name, ind in indices)
        self.numblocks = numblocks
        self.concatenate = concatenate
        self.new_axes = new_axes or {}

    @property
    def _dict(self):
        if hasattr(self, '_cached_dict'):
            return self._cached_dict
        else:
            keys = tuple(map(atop_token, range(len(self.indices))))
            func = SubgraphCallable(self.dsk, self.output, keys)
            self._cached_dict = top(
                func,
                self.output,
                self.output_indices,
                *list(toolz.concat(self.indices)),
                new_axes=self.new_axes,
                numblocks=self.numblocks,
                concatenate=self.concatenate
            )
        return self._cached_dict

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return int(np.prod(list(self._out_numblocks().values())))

    def _out_numblocks(self):
        d = {}
        indices = {k: v for k, v in self.indices if v is not None}
        for k, v in self.numblocks.items():
            for a, b in zip(indices[k], v):
                d[a] = max(d.get(a, 0), b)

        return {k: v for k, v in d.items() if k in self.output_indices}


def top(func, output, out_indices, *arrind_pairs, **kwargs):
    """ Tensor operation

    Applies a function, ``func``, across blocks from many different input
    dasks.  We arrange the pattern with which those blocks interact with sets
    of matching indices.  E.g.::

        top(func, 'z', 'i', 'x', 'i', 'y', 'i')

    yield an embarrassingly parallel communication pattern and is read as

        $$ z_i = func(x_i, y_i) $$

    More complex patterns may emerge, including multiple indices::

        top(func, 'z', 'ij', 'x', 'ij', 'y', 'ji')

        $$ z_{ij} = func(x_{ij}, y_{ji}) $$

    Indices missing in the output but present in the inputs results in many
    inputs being sent to one function (see examples).

    Examples
    --------

    Simple embarrassing map operation

    >>> inc = lambda x: x + 1
    >>> top(inc, 'z', 'ij', 'x', 'ij', numblocks={'x': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (inc, ('x', 0, 0)),
     ('z', 0, 1): (inc, ('x', 0, 1)),
     ('z', 1, 0): (inc, ('x', 1, 0)),
     ('z', 1, 1): (inc, ('x', 1, 1))}

    Simple operation on two datasets

    >>> add = lambda x, y: x + y
    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (2, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Operation that flips one of the datasets

    >>> addT = lambda x, y: x + y.T  # Transpose each chunk
    >>> #                                        z_ij ~ x_ij y_ji
    >>> #               ..         ..         .. notice swap
    >>> top(addT, 'z', 'ij', 'x', 'ij', 'y', 'ji', numblocks={'x': (2, 2),
    ...                                                       'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 1, 0)),
     ('z', 1, 0): (add, ('x', 1, 0), ('y', 0, 1)),
     ('z', 1, 1): (add, ('x', 1, 1), ('y', 1, 1))}

    Dot product with contraction over ``j`` index.  Yields list arguments

    >>> top(dotmany, 'z', 'ik', 'x', 'ij', 'y', 'jk', numblocks={'x': (2, 2),
    ...                                                          'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 0, 1): (dotmany, [('x', 0, 0), ('x', 0, 1)],
                            [('y', 0, 1), ('y', 1, 1)]),
     ('z', 1, 0): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 0), ('y', 1, 0)]),
     ('z', 1, 1): (dotmany, [('x', 1, 0), ('x', 1, 1)],
                            [('y', 0, 1), ('y', 1, 1)])}

    Pass ``concatenate=True`` to concatenate arrays ahead of time

    >>> top(f, 'z', 'i', 'x', 'ij', 'y', 'ij', concatenate=True,
    ...     numblocks={'x': (2, 2), 'y': (2, 2,)})  # doctest: +SKIP
    {('z', 0): (f, (concatenate_axes, [('x', 0, 0), ('x', 0, 1)], (1,)),
                   (concatenate_axes, [('y', 0, 0), ('y', 0, 1)], (1,)))
     ('z', 1): (f, (concatenate_axes, [('x', 1, 0), ('x', 1, 1)], (1,)),
                   (concatenate_axes, [('y', 1, 0), ('y', 1, 1)], (1,)))}

    Supports Broadcasting rules

    >>> top(add, 'z', 'ij', 'x', 'ij', 'y', 'ij', numblocks={'x': (1, 2),
    ...                                                      'y': (2, 2)})  # doctest: +SKIP
    {('z', 0, 0): (add, ('x', 0, 0), ('y', 0, 0)),
     ('z', 0, 1): (add, ('x', 0, 1), ('y', 0, 1)),
     ('z', 1, 0): (add, ('x', 0, 0), ('y', 1, 0)),
     ('z', 1, 1): (add, ('x', 0, 1), ('y', 1, 1))}

    Support keyword arguments with apply

    >>> def f(a, b=0): return a + b
    >>> top(f, 'z', 'i', 'x', 'i', numblocks={'x': (2,)}, b=10)  # doctest: +SKIP
    {('z', 0): (apply, f, [('x', 0)], {'b': 10}),
     ('z', 1): (apply, f, [('x', 1)], {'b': 10})}

    Include literals by indexing with ``None``

    >>> top(add, 'z', 'i', 'x', 'i', 100, None,  numblocks={'x': (2,)})  # doctest: +SKIP
    {('z', 0): (add, ('x', 0), 100),
     ('z', 1): (add, ('x', 1), 100)}


    See Also
    --------
    atop
    """
    from .core import broadcast_dimensions, zero_broadcast_dimensions, concatenate_axes
    numblocks = kwargs.pop('numblocks')
    concatenate = kwargs.pop('concatenate', None)
    new_axes = kwargs.pop('new_axes', {})
    argpairs = list(toolz.partition(2, arrind_pairs))

    assert set(numblocks) == {name for name, ind in argpairs if ind is not None}

    all_indices = {x for _, ind in argpairs if ind for x in ind}
    dummy_indices = all_indices - set(out_indices)

    # Dictionary mapping {i: 3, j: 4, ...} for i, j, ... the dimensions
    dims = broadcast_dimensions(argpairs, numblocks)
    for k in new_axes:
        dims[k] = 1

    # (0, 0), (0, 1), (0, 2), (1, 0), ...
    keytups = list(itertools.product(*[range(dims[i]) for i in out_indices]))
    # {i: 0, j: 0}, {i: 0, j: 1}, ...
    keydicts = [dict(zip(out_indices, tup)) for tup in keytups]

    # {j: [1, 2, 3], ...}  For j a dummy index of dimension 3
    dummies = dict((i, list(range(dims[i]))) for i in dummy_indices)

    dsk = {}

    # Create argument lists
    valtups = []
    for kd in keydicts:
        args = []
        for arg, ind in argpairs:
            if ind is None:
                args.append(arg)
            else:
                tups = lol_tuples((arg,), ind, kd, dummies)
                if any(nb == 1 for nb in numblocks[arg]):
                    tups2 = zero_broadcast_dimensions(tups, numblocks[arg])
                else:
                    tups2 = tups
                if concatenate and isinstance(tups2, list):
                    axes = [n for n, i in enumerate(ind) if i in dummies]
                    tups2 = (concatenate_axes, tups2, axes)
                args.append(tups2)
        valtups.append(args)

    if not kwargs:  # will not be used in an apply, should be a tuple
        valtups = [tuple(vt) for vt in valtups]

    # Add heads to tuples
    keys = [(output,) + kt for kt in keytups]

    # Unpack delayed objects in kwargs
    if kwargs:
        task, dsk2 = to_task_dask(kwargs)
        if dsk2:
            dsk.update(utils.ensure_dict(dsk2))
            kwargs2 = task
        else:
            kwargs2 = kwargs
        vals = [(apply, func, vt, kwargs2) for vt in valtups]
    else:
        vals = [(func,) + vt for vt in valtups]

    dsk.update(dict(zip(keys, vals)))

    return dsk


def atop(func, out_ind, *args, **kwargs):
    """ Tensor operation: Generalized inner and outer products

    A broad class of blocked algorithms and patterns can be specified with a
    concise multi-index notation.  The ``atop`` function applies an in-memory
    function across multiple blocks of multiple inputs in a variety of ways.
    Many dask.array operations are special cases of atop including elementwise,
    broadcasting, reductions, tensordot, and transpose.

    Parameters
    ----------
    func : callable
        Function to apply to individual tuples of blocks
    out_ind : iterable
        Block pattern of the output, something like 'ijk' or (1, 2, 3)
    *args : sequence of Array, index pairs
        Sequence like (x, 'ij', y, 'jk', z, 'i')
    **kwargs : dict
        Extra keyword arguments to pass to function
    dtype : np.dtype
        Datatype of resulting array.
    concatenate : bool, keyword only
        If true concatenate arrays along dummy indices, else provide lists
    adjust_chunks : dict
        Dictionary mapping index to function to be applied to chunk sizes
    new_axes : dict, keyword only
        New indexes and their dimension lengths

    Examples
    --------
    2D embarrassingly parallel operation from two arrays, x, and y.

    >>> z = atop(operator.add, 'ij', x, 'ij', y, 'ij', dtype='f8')  # z = x + y  # doctest: +SKIP

    Outer product multiplying x by y, two 1-d vectors

    >>> z = atop(operator.mul, 'ij', x, 'i', y, 'j', dtype='f8')  # doctest: +SKIP

    z = x.T

    >>> z = atop(np.transpose, 'ji', x, 'ij', dtype=x.dtype)  # doctest: +SKIP

    The transpose case above is illustrative because it does same transposition
    both on each in-memory block by calling ``np.transpose`` and on the order
    of the blocks themselves, by switching the order of the index ``ij -> ji``.

    We can compose these same patterns with more variables and more complex
    in-memory functions

    z = X + Y.T

    >>> z = atop(lambda x, y: x + y.T, 'ij', x, 'ij', y, 'ji', dtype='f8')  # doctest: +SKIP

    Any index, like ``i`` missing from the output index is interpreted as a
    contraction (note that this differs from Einstein convention; repeated
    indices do not imply contraction.)  In the case of a contraction the passed
    function should expect an iterable of blocks on any array that holds that
    index.  To receive arrays concatenated along contracted dimensions instead
    pass ``concatenate=True``.

    Inner product multiplying x by y, two 1-d vectors

    >>> def sequence_dot(x_blocks, y_blocks):
    ...     result = 0
    ...     for x, y in zip(x_blocks, y_blocks):
    ...         result += x.dot(y)
    ...     return result

    >>> z = atop(sequence_dot, '', x, 'i', y, 'i', dtype='f8')  # doctest: +SKIP

    Add new single-chunk dimensions with the ``new_axes=`` keyword, including
    the length of the new dimension.  New dimensions will always be in a single
    chunk.

    >>> def f(x):
    ...     return x[:, None] * np.ones((1, 5))

    >>> z = atop(f, 'az', x, 'a', new_axes={'z': 5}, dtype=x.dtype)  # doctest: +SKIP

    If the applied function changes the size of each chunk you can specify this
    with a ``adjust_chunks={...}`` dictionary holding a function for each index
    that modifies the dimension size in that index.

    >>> def double(x):
    ...     return np.concatenate([x, x])

    >>> y = atop(double, 'ij', x, 'ij',
    ...          adjust_chunks={'i': lambda n: 2 * n}, dtype=x.dtype)  # doctest: +SKIP

    Include literals by indexing with None

    >>> y = atop(add, 'ij', x, 'ij', 1234, None, dtype=x.dtype)  # doctest: +SKIP

    See Also
    --------
    top - dict formulation of this function, contains most logic
    """
    out = kwargs.pop('name', None)      # May be None at this point
    token = kwargs.pop('token', None)
    dtype = kwargs.pop('dtype', None)
    adjust_chunks = kwargs.pop('adjust_chunks', None)
    new_axes = kwargs.get('new_axes', {})

    # Input Validation
    if len(set(out_ind)) != len(out_ind):
        raise ValueError("Repeated elements not allowed in output index",
                         [k for k, v in toolz.frequencies(out_ind).items() if v > 1])
    new = (set(out_ind)
           - {a for arg in args[1::2] if arg is not None for a in arg}
           - set(new_axes or ()))
    if new:
        raise ValueError("Unknown dimension", new)

    from .core import Array, unify_chunks, normalize_arg

    if dtype is None:
        raise ValueError("Must specify dtype of output array")

    chunkss, arrays = unify_chunks(*args)
    for k, v in new_axes.items():
        chunkss[k] = (v,)
    arginds = list(zip(arrays, args[1::2]))

    for arg, ind in arginds:
        if hasattr(arg, 'ndim') and hasattr(ind, '__len__') and arg.ndim != len(ind):
            raise ValueError("Index string %s does not match array dimension %d"
                             % (ind, arg.ndim))

    numblocks = {a.name: a.numblocks for a, ind in arginds if ind is not None}
    argindsstr = list(toolz.concat([(normalize_arg(a) if ind is None else a.name, ind)
                                    for a, ind in arginds]))
    # Finish up the name
    if not out:
        out = '%s-%s' % (token or utils.funcname(func).strip('_'),
                         base.tokenize(func, out_ind, argindsstr, dtype, **kwargs))

    kwargs2 = {k: normalize_arg(v) for k, v in kwargs.items()}
    dsk = _top(func, out, out_ind, *argindsstr, numblocks=numblocks, **kwargs2)
    dsks = [a.dask for a, ind in arginds if ind is not None]

    chunks = [chunkss[i] for i in out_ind]
    if adjust_chunks:
        for i, ind in enumerate(out_ind):
            if ind in adjust_chunks:
                if callable(adjust_chunks[ind]):
                    chunks[i] = tuple(map(adjust_chunks[ind], chunks[i]))
                elif isinstance(adjust_chunks[ind], numbers.Integral):
                    chunks[i] = tuple(adjust_chunks[ind] for _ in chunks[i])
                elif isinstance(adjust_chunks[ind], (tuple, list)):
                    chunks[i] = tuple(adjust_chunks[ind])
                else:
                    raise NotImplementedError(
                        "adjust_chunks values must be callable, int, or tuple")
    chunks = tuple(chunks)

    return Array(sharedict.merge((out, dsk), *dsks,
                                 dependencies={out: {a.name for a, ind in arginds if ind is not None}}),
                 out, chunks, dtype=dtype)


def lol_tuples(head, ind, values, dummies):
    """ List of list of tuple keys

    Parameters
    ----------

    head : tuple
        The known tuple so far
    ind : Iterable
        An iterable of indices not yet covered
    values : dict
        Known values for non-dummy indices
    dummies : dict
        Ranges of values for dummy indices

    Examples
    --------

    >>> lol_tuples(('x',), 'ij', {'i': 1, 'j': 0}, {})
    ('x', 1, 0)

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ij', {'i': 1}, {'j': range(3)})
    [('x', 1, 0), ('x', 1, 1), ('x', 1, 2)]

    >>> lol_tuples(('x',), 'ijk', {'i': 1}, {'j': [0, 1, 2], 'k': [0, 1]}) # doctest: +NORMALIZE_WHITESPACE
    [[('x', 1, 0, 0), ('x', 1, 0, 1)],
     [('x', 1, 1, 0), ('x', 1, 1, 1)],
     [('x', 1, 2, 0), ('x', 1, 2, 1)]]
    """
    if not ind:
        return head
    if ind[0] not in dummies:
        return lol_tuples(head + (values[ind[0]],), ind[1:], values, dummies)
    else:
        return [lol_tuples(head + (v,), ind[1:], values, dummies)
                for v in dummies[ind[0]]]


def optimize_atop(full_graph, keys=()):
    """ High level optimization of stacked TOP layers

    For operations that have multiple TOP operations one after the other, like
    ``x.T + 123`` we can fuse these into a single TOP operation.  This happens
    before any actual tasks are generated, and so can reduce overhead.

    This finds groups of TOP operations that can be safely fused, and then
    passes them to ``rewrite_atop`` for rewriting.

    Parameters
    ----------
    full_graph: ShareDict
    keys: Iterable
        The keys of all outputs of all collections.
        Used to make sure that we don't fuse a layer needed by an output

    Returns
    -------
    sharedict : ShareDict

    See Also
    --------
    rewrite_atop
    """
    keep = {k[0] if type(k) is tuple else k for k in keys}
    layers = full_graph.dicts
    dependents = core.reverse_dict(full_graph.dependencies)
    roots = {k for k in full_graph.dicts
             if not dependents.get(k)}
    stack = list(roots)

    out = {}
    dependencies = {}
    seen = set()

    while stack:
        layer = stack.pop()
        if layer in seen or layer not in layers:
            continue
        seen.add(layer)

        # Outer loop walks through possible output TOP layers
        if isinstance(layers[layer], TOP):
            top_layers = {layer}
            deps = set(top_layers)
            while deps:  # we gather as many sub-layers as we can
                dep = deps.pop()
                if dep not in layers:
                    stack.append(dep)
                    continue
                if not isinstance(layers[dep], TOP):
                    stack.append(dep)
                    continue
                if (dep != layer and dep in keep):
                    stack.append(dep)
                    continue
                if layers[dep].concatenate != layers[layer].concatenate:
                    stack.append(dep)
                    continue

                # passed everything, proceed
                top_layers.add(dep)

                # traverse further to this child's children
                for d in full_graph.dependencies.get(dep, ()):
                    # Don't allow reductions to proceed
                    output_indices = set(layers[dep].output_indices)
                    input_indices = {i for _, ind in layers[dep].indices if ind for i in ind}

                    if len(dependents[d]) <= 1 and output_indices.issuperset(input_indices):
                        deps.add(d)
                    else:
                        stack.append(d)

            # Merge these TOP layers into one
            new_layer = rewrite_atop([layers[l] for l in top_layers])
            out[layer] = new_layer
            dependencies[layer] = {k for k, v in new_layer.indices if v is not None}
        else:
            out[layer] = layers[layer]
            dependencies[layer] = full_graph.dependencies.get(layer, set())
            stack.extend(full_graph.dependencies.get(layer, ()))

    return sharedict.ShareDict(out, dependencies)


def rewrite_atop(inputs):
    """ Rewrite a stack of atop expressions into a single atop expression

    Given a set of TOP layers, combine them into a single layer.  The provided
    layers are expected to fit well together.  That job is handled by
    ``optimize_atop``

    Parameters
    ----------
    inputs : List[TOP]

    Returns
    -------
    top : TOP

    See Also
    --------
    optimize_atop
    """
    inputs = {inp.output: inp for inp in inputs}
    dependencies = {inp.output: {d for d, v in inp.indices
                                 if v is not None and d in inputs}
                    for inp in inputs.values()}
    dependents = core.reverse_dict(dependencies)

    new_index_iter = (c + (str(d) if d else '')  # A, B, ... A1, B1, ...
                      for d in itertools.count()
                      for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    [root] = [k for k, v in dependents.items() if not v]

    # Our final results.  These will change during fusion below
    indices = list(inputs[root].indices)
    new_axes = inputs[root].new_axes
    concatenate = inputs[root].concatenate
    dsk = dict(inputs[root].dsk)

    changed = True
    while changed:
        changed = False
        for i, (dep, ind) in enumerate(indices):
            if ind is None:
                continue
            if dep not in inputs:
                continue

            changed = True

            # Replace _n with dep name in existing tasks
            # (inc, _0) -> (inc, 'b')
            dsk = {k: subs(v, {atop_token(i): dep}) for k, v in dsk.items()}

            # Remove current input from input indices
            # [('a', 'i'), ('b', 'i')] -> [('a', 'i')]
            _, current_dep_indices = indices.pop(i)
            sub = {atop_token(i): atop_token(i - 1) for i in range(i + 1, len(indices) + 1)}
            dsk = subs(dsk, sub)

            # Change new input_indices to match give index from current computation
            # [('c', j')] -> [('c', 'i')]
            new_indices = inputs[dep].indices
            sub = dict(zip(inputs[dep].output_indices, current_dep_indices))
            contracted = {x for _, j in new_indices
                          if j is not None
                          for x in j
                          if x not in inputs[dep].output_indices}
            extra = dict(zip(contracted, new_index_iter))
            sub.update(extra)
            new_indices = [(x, index_subs(j, sub)) for x, j in new_indices]

            # Update new_axes
            for k, v in inputs[dep].new_axes.items():
                new_axes[sub[k]] = v

            # Bump new inputs up in list
            sub = {}
            for i, index in enumerate(new_indices):
                try:
                    contains = index in indices
                except (ValueError, TypeError):
                    contains = False

                if contains:  # use old inputs if available
                    sub[atop_token(i)] = atop_token(indices.index(index))
                else:
                    sub[atop_token(i)] = atop_token(len(indices))
                    indices.append(index)
            new_dsk = subs(inputs[dep].dsk, sub)

            # indices.extend(new_indices)
            dsk.update(new_dsk)

    indices = [(a, tuple(b) if isinstance(b, list) else b)
               for a, b in indices]

    # De-duplicate indices like [(a, ij), (b, i), (a, ij)] -> [(a, ij), (b, i)]
    # Make sure that we map everything else appropriately as we remove inputs
    new_indices = []
    seen = {}
    sub = {}  # like {_0: _0, _1: _0, _2: _1}
    for i, x in enumerate(indices):
        if x[1] is not None and x in seen:
            sub[i] = seen[x]
        else:
            if x[1] is not None:
                seen[x] = len(new_indices)
            sub[i] = len(new_indices)
            new_indices.append(x)

    sub = {atop_token(k): atop_token(v) for k, v in sub.items()}
    dsk = {k: subs(v, sub) for k, v in dsk.items()}

    indices_check = {k for k, v in indices if v is not None}
    numblocks = toolz.merge([inp.numblocks for inp in inputs.values()])
    numblocks = {k: v for k, v in numblocks.items()
                 if v is None or k in indices_check}

    out = TOP(root, inputs[root].output_indices, dsk, new_indices,
              numblocks=numblocks, new_axes=new_axes, concatenate=concatenate)

    return out
