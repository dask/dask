from __future__ import absolute_import, division, print_function

from operator import getitem

import numpy as np
import toolz

from .core import (getter, getter_nofancy, getter_inline, subs, atop_token,
        index_subs)
from ..compatibility import zip_longest
from ..core import flatten, reverse_dict
from ..optimization import cull, fuse, inline_functions
from ..utils import ensure_dict
from ..sharedict import ShareDict

from numbers import Integral

# All get* functions the optimizations know about
GETTERS = (getter, getter_nofancy, getter_inline, getitem)
# These get* functions aren't ever completely removed from the graph,
# even if the index should be a no-op by numpy semantics. Some array-like's
# don't completely follow semantics, making indexing always necessary.
GETNOREMOVE = (getter, getter_nofancy)


def optimize(dsk, keys, fuse_keys=None, fast_functions=None,
             inline_functions_fast_functions=(getter_inline,), rename_fused_keys=True,
             **kwargs):
    """ Optimize dask for array computation

    1.  Cull tasks not necessary to evaluate keys
    2.  Remove full slicing, e.g. x[:]
    3.  Inline fast functions like getitem and np.transpose
    """
    keys = list(flatten(keys))
    key_prefixes = {k[0] if type(k) is tuple else k for k in keys}

    # High level stage optimization
    if isinstance(dsk, ShareDict):
        dsk = optimize_atop(dsk, keep=key_prefixes)

    # Low level task optimizations
    dsk = ensure_dict(dsk)
    if fast_functions is not None:
        inline_functions_fast_functions = fast_functions

    dsk2, dependencies = cull(dsk, keys)
    hold = hold_keys(dsk2, dependencies)

    dsk3, dependencies = fuse(dsk2, hold + keys + (fuse_keys or []),
                              dependencies, rename_keys=rename_fused_keys)
    if inline_functions_fast_functions:
        dsk4 = inline_functions(dsk3, keys, dependencies=dependencies,
                                fast_functions=inline_functions_fast_functions)
    else:
        dsk4 = dsk3
    dsk5 = optimize_slices(dsk4)

    return dsk5


def hold_keys(dsk, dependencies):
    """ Find keys to avoid fusion

    We don't want to fuse data present in the graph because it is easier to
    serialize as a raw value.

    We don't want to fuse chains after getitem/GETTERS because we want to
    move around only small pieces of data, rather than the underlying arrays.
    """
    dependents = reverse_dict(dependencies)
    data = {k for k, v in dsk.items() if type(v) not in (tuple, str)}

    hold_keys = list(data)
    for dat in data:
        deps = dependents[dat]
        for dep in deps:
            task = dsk[dep]
            # If the task is a get* function, we walk up the chain, and stop
            # when there's either more than one dependent, or the dependent is
            # no longer a get* function or an alias. We then add the final
            # key to the list of keys not to fuse.
            if type(task) is tuple and task and task[0] in GETTERS:
                try:
                    while len(dependents[dep]) == 1:
                        new_dep = next(iter(dependents[dep]))
                        new_task = dsk[new_dep]
                        # If the task is a get* or an alias, continue up the
                        # linear chain
                        if new_task[0] in GETTERS or new_task in dsk:
                            dep = new_dep
                        else:
                            break
                except (IndexError, TypeError):
                    pass
                hold_keys.append(dep)
    return hold_keys


def optimize_slices(dsk):
    """ Optimize slices

    1.  Fuse repeated slices, like x[5:][2:6] -> x[7:11]
    2.  Remove full slices, like         x[:] -> x

    See also:
        fuse_slice_dict
    """
    fancy_ind_types = (list, np.ndarray)
    dsk = dsk.copy()
    for k, v in dsk.items():
        if type(v) is tuple and v[0] in GETTERS and len(v) in (3, 5):
            if len(v) == 3:
                get, a, a_index = v
                # getter defaults to asarray=True, getitem is semantically False
                a_asarray = get is not getitem
                a_lock = None
            else:
                get, a, a_index, a_asarray, a_lock = v
            while type(a) is tuple and a[0] in GETTERS and len(a) in (3, 5):
                if len(a) == 3:
                    f2, b, b_index = a
                    b_asarray = f2 is not getitem
                    b_lock = None
                else:
                    f2, b, b_index, b_asarray, b_lock = a

                if a_lock and a_lock is not b_lock:
                    break
                if (type(a_index) is tuple) != (type(b_index) is tuple):
                    break
                if type(a_index) is tuple:
                    indices = b_index + a_index
                    if (len(a_index) != len(b_index) and
                            any(i is None for i in indices)):
                        break
                    if (f2 is getter_nofancy and
                            any(isinstance(i, fancy_ind_types) for i in indices)):
                        break
                elif (f2 is getter_nofancy and
                        (type(a_index) in fancy_ind_types or
                         type(b_index) in fancy_ind_types)):
                    break
                try:
                    c_index = fuse_slice(b_index, a_index)
                    # rely on fact that nested gets never decrease in
                    # strictness e.g. `(getter_nofancy, (getter, ...))` never
                    # happens
                    get = getter if f2 is getter_inline else f2
                except NotImplementedError:
                    break
                a, a_index, a_lock = b, c_index, b_lock
                a_asarray |= b_asarray

            # Skip the get call if not from from_array and nothing to do
            if (get not in GETNOREMOVE and
                ((type(a_index) is slice and not a_index.start and
                  a_index.stop is None and a_index.step is None) or
                 (type(a_index) is tuple and
                  all(type(s) is slice and not s.start and s.stop is None and
                      s.step is None for s in a_index)))):
                dsk[k] = a
            elif get is getitem or (a_asarray and not a_lock):
                # default settings are fine, drop the extra parameters Since we
                # always fallback to inner `get` functions, `get is getitem`
                # can only occur if all gets are getitem, meaning all
                # parameters must be getitem defaults.
                dsk[k] = (get, a, a_index)
            else:
                dsk[k] = (get, a, a_index, a_asarray, a_lock)

    return dsk


def normalize_slice(s):
    """ Replace Nones in slices with integers

    >>> normalize_slice(slice(None, None, None))
    slice(0, None, 1)
    """
    start, stop, step = s.start, s.stop, s.step
    if start is None:
        start = 0
    if step is None:
        step = 1
    if start < 0 or step < 0 or stop is not None and stop < 0:
        raise NotImplementedError()
    return slice(start, stop, step)


def check_for_nonfusible_fancy_indexing(fancy, normal):
    # Check for fancy indexing and normal indexing, where the fancy
    # indexed dimensions != normal indexed dimensions with integers. E.g.:
    # disallow things like:
    # x[:, [1, 2], :][0, :, :] -> x[0, [1, 2], :] or
    # x[0, :, :][:, [1, 2], :] -> x[0, [1, 2], :]
    for f, n in zip_longest(fancy, normal, fillvalue=slice(None)):
        if type(f) is not list and isinstance(n, Integral):
            raise NotImplementedError("Can't handle normal indexing with "
                                      "integers and fancy indexing if the "
                                      "integers and fancy indices don't "
                                      "align with the same dimensions.")


def fuse_slice(a, b):
    """ Fuse stacked slices together

    Fuse a pair of repeated slices into a single slice:

    >>> fuse_slice(slice(1000, 2000), slice(10, 15))
    slice(1010, 1015, None)

    This also works for tuples of slices

    >>> fuse_slice((slice(100, 200), slice(100, 200, 10)),
    ...            (slice(10, 15), [5, 2]))
    (slice(110, 115, None), [150, 120])

    And a variety of other interesting cases

    >>> fuse_slice(slice(1000, 2000), 10)  # integers
    1010

    >>> fuse_slice(slice(1000, 2000, 5), slice(10, 20, 2))
    slice(1050, 1100, 10)

    >>> fuse_slice(slice(1000, 2000, 5), [1, 2, 3])  # lists
    [1005, 1010, 1015]

    >>> fuse_slice(None, slice(None, None))  # doctest: +SKIP
    None
    """
    # None only works if the second side is a full slice
    if a is None and isinstance(b, slice) and b == slice(None, None):
        return None

    # Replace None with 0 and one in start and step
    if isinstance(a, slice):
        a = normalize_slice(a)
    if isinstance(b, slice):
        b = normalize_slice(b)

    if isinstance(a, slice) and isinstance(b, Integral):
        if b < 0:
            raise NotImplementedError()
        return a.start + b * a.step

    if isinstance(a, slice) and isinstance(b, slice):
        start = a.start + a.step * b.start
        if b.stop is not None:
            stop = a.start + a.step * b.stop
        else:
            stop = None
        if a.stop is not None:
            if stop is not None:
                stop = min(a.stop, stop)
            else:
                stop = a.stop
        step = a.step * b.step
        if step == 1:
            step = None
        return slice(start, stop, step)

    if isinstance(b, list):
        return [fuse_slice(a, bb) for bb in b]
    if isinstance(a, list) and isinstance(b, (Integral, slice)):
        return a[b]

    if isinstance(a, tuple) and not isinstance(b, tuple):
        b = (b,)

    # If given two tuples walk through both, being mindful of uneven sizes
    # and newaxes
    if isinstance(a, tuple) and isinstance(b, tuple):

        # Check for non-fusible cases with fancy-indexing
        a_has_lists = any(isinstance(item, list) for item in a)
        b_has_lists = any(isinstance(item, list) for item in b)
        if a_has_lists and b_has_lists:
            raise NotImplementedError("Can't handle multiple list indexing")
        elif a_has_lists:
            check_for_nonfusible_fancy_indexing(a, b)
        elif b_has_lists:
            check_for_nonfusible_fancy_indexing(b, a)

        j = 0
        result = list()
        for i in range(len(a)):
            #  axis ceased to exist  or we're out of b
            if isinstance(a[i], Integral) or j == len(b):
                result.append(a[i])
                continue
            while b[j] is None:  # insert any Nones on the rhs
                result.append(None)
                j += 1
            result.append(fuse_slice(a[i], b[j]))  # Common case
            j += 1
        while j < len(b):  # anything leftover on the right?
            result.append(b[j])
            j += 1
        return tuple(result)
    raise NotImplementedError()


def optimize_atop(sharedict, keep=()):
    from .core import TOP
    layers = sharedict.dicts
    dependents = reverse_dict(sharedict.dependencies)
    roots = {k for k, v in sharedict.dicts.items() if not dependents.get(k)}
    stack = list(roots)

    out = {}
    dependencies = {}
    seen = set()

    while stack:
        layer = stack.pop()
        if layer in seen:
            continue
        seen.add(layer)
        if isinstance(layers[layer], TOP):
            top_layers = {layer}
            deps = set(top_layers)
            while deps:
                dep = deps.pop()
                if (isinstance(layers[dep], TOP) and
                        not (dep != layer and dep in keep) and
                        layers[dep].concatenate == layers[layer].concatenate):
                    top_layers.add(dep)
                    for d in sharedict.dependencies.get(dep, ()):
                        if len(dependents[d]) <= 1:
                            deps.add(d)
                        else:
                            stack.append(d)
                else:
                    stack.append(dep)
            new_layer = rewrite_atop([layers[l] for l in top_layers])
            out[layer] = new_layer
            dependencies[layer] = {k for k, v in new_layer.indices if v is not None}
        else:
            out[layer] = layers[layer]
            dependencies[layer] = sharedict.dependencies.get(layer, set())
            stack.extend(sharedict.dependencies.get(layer, ()))

    return ShareDict(out, dependencies)


def rewrite_atop(inputs):
    """ Rewrite a stack of atop expressions into a single atop expression """
    from dask.core import reverse_dict
    from dask.array.core import TOP
    inputs = {inp.output: inp for inp in inputs}
    dependencies = {inp.output: {d for d, v in inp.indices if v is not None and d in inputs}
                    for inp in inputs.values()}
    dependents = reverse_dict(dependencies)

    new_index_iter = iter('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    [root] = [k for k, v in dependents.items() if not v]

    indices = list(inputs[root].indices)
    new_axes = inputs[root].new_axes
    concatenate = inputs[root].concatenate

    dsk = dict(inputs[root].dsk)

    seen = set()

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
            _, current_dep_indices = indices.pop(i)  # TODO: this screws with current _0, _1, ...
            sub = {atop_token(i): atop_token(i - 1) for i in range(i + 1, len(indices) + 1)}
            dsk = subs(dsk, sub)

            # Change new input_indices to match give index from current computation
            # [('c', j')] -> [('c', 'i')]
            new_indices = inputs[dep].indices
            sub = dict(zip(inputs[dep].output_indices, current_dep_indices))
            contracted = {x for _, j in new_indices if j is not None for x in j if x not in inputs[dep].output_indices}
            extra = dict(zip(contracted, new_index_iter))
            sub.update(extra)
            old_indices = tuple(new_indices)
            new_indices = [(x, index_subs(j, sub)) for x, j in new_indices]

            # TODO: handle new_axes
            # TODO: handle concatenate

            # Bump new inputs up in list
            sub = {}
            for i, index in enumerate(new_indices):
                if index not in indices:  # use old inputs if available
                    sub[atop_token(i)] = atop_token(len(indices))
                    indices.append(index)
                else:
                    sub[atop_token(i)] = atop_token(indices.index(index))
            new_dsk = subs(inputs[dep].dsk, sub)

            # indices.extend(new_indices)
            dsk.update(new_dsk)

    indices = [(a, tuple(b) if isinstance(b, list) else b) for a, b in indices]

    # De-duplicate indices
    new_indices = []
    seen = dict()
    sub = dict()  # like {_0: _0, _1: _0, _2: _1}
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

    numblocks = toolz.merge([inp.numblocks for inp in inputs.values()])
    numblocks = {k: v for k, v in numblocks.items() if k in toolz.pluck(0, indices)}

    out = TOP(root, inputs[root].output_indices, dsk, new_indices,
              numblocks=numblocks, new_axes=new_axes, concatenate=concatenate)

    return out
