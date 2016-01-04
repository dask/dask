from __future__ import absolute_import, division, print_function

from operator import getitem

import numpy as np
from toolz import valmap, partial

from .core import getarray
from ..core import flatten
from ..optimize import cull, fuse, dealias, inline_functions
from ..rewrite import RuleSet, RewriteRule


def optimize(dsk, keys, **kwargs):
    """ Optimize dask for array computation

    1.  Cull tasks not necessary to evaluate keys
    2.  Remove full slicing, e.g. x[:]
    3.  Inline fast functions like getitem and np.transpose
    """
    keys = list(flatten(keys))
    fast_functions = kwargs.get('fast_functions',
                             set([getarray, getitem, np.transpose]))
    dsk2 = cull(dsk, keys)
    dsk3 = remove_full_slices(dsk2, keys)
    dsk4 = fuse(dsk3, keys)
    dsk5 = valmap(rewrite_rules.rewrite, dsk4)
    dsk6 = inline_functions(dsk5, keys, fast_functions=fast_functions)
    return dsk6


def is_full_slice(task):
    """

    >>> is_full_slice((getitem, 'x',
    ...                 (slice(None, None, None), slice(None, None, None))))
    True
    >>> is_full_slice((getitem, 'x',
    ...                 (slice(5, 20, 1), slice(None, None, None))))
    False
    """
    return (isinstance(task, tuple) and
            (task[0] in (getitem, getarray)) and
            (task[2] == slice(None, None, None) or
             isinstance(task[2], tuple) and
             all(ind == slice(None, None, None) for ind in task[2])))


def remove_full_slices(dsk, keys):
    """ Remove full slices from dask

    See Also:
        dask.optimize.inline

    Examples
    --------

    >>> dsk = {'a': (range, 5),
    ...        'b': (getitem, 'a', (slice(None, None, None),)),
    ...        'c': (getitem, 'b', (slice(None, None, None),)),
    ...        'd': (getitem, 'c', (slice(None, 5, None),)),
    ...        'e': (getitem, 'd', (slice(None, None, None),))}

    >>> remove_full_slices(dsk)  # doctest: +SKIP
    {'a': (range, 5),
     'e': (getitem, 'a', (slice(None, 5, None),))}
    """
    keys = set(keys)
    full_slice_keys = set(k for k, task in dsk.items()
                            if is_full_slice(task))
    dsk2 = dict((k, task[1] if k in full_slice_keys and k not in keys else task)
                 for k, task in dsk.items())
    dsk3 = dealias(dsk2, keys)
    return dsk3


a, b, x = '~a', '~b', '~x'


def fuse_slice_dict(match, getter=getarray):
    # Making new dimensions?  Need two getitems
    # TODO: well, we could still optimize the non-None axes
    try:
        c = fuse_slice(match[a], match[b])
        return (getter, match[x], c)
    except NotImplementedError:
        return (getitem, (getter, match[x], match[a]),
                         match[b])


rewrite_rules = RuleSet(
        RewriteRule((getitem, (getitem, x, a), b),
                    partial(fuse_slice_dict, getter=getitem),
                    (a, b, x)),
        RewriteRule((getarray, (getitem, x, a), b),
                    fuse_slice_dict,
                    (a, b, x)),
        RewriteRule((getarray, (getarray, x, a), b),
                    fuse_slice_dict,
                    (a, b, x)),
        RewriteRule((getitem, (getarray, x, a), b),
                    fuse_slice_dict,
                    (a, b, x)))


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
    if a is None and b == slice(None, None):
        return None

    # Replace None with 0 and one in start and step
    if isinstance(a, slice):
        a = normalize_slice(a)
    if isinstance(b, slice):
        b = normalize_slice(b)

    if isinstance(a, slice) and isinstance(b, int):
        if b < 0:
            raise NotImplementedError()
        return a.start + b*a.step

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
            stop = stop
        step = a.step * b.step
        if step == 1:
            step = None
        return slice(start, stop, step)

    if isinstance(b, list):
        return [fuse_slice(a, bb) for bb in b]
    if isinstance(a, list) and isinstance(b, (int, slice)):
        return a[b]

    if isinstance(a, tuple) and not isinstance(b, tuple):
        b = (b,)

    # If given two tuples walk through both, being mindful of uneven sizes
    # and newaxes
    if isinstance(a, tuple) and isinstance(b, tuple):

        if (any(isinstance(item, list) for item in a) and
            any(isinstance(item, list) for item in b)):
            raise NotImplementedError("Can't handle multiple list indexing")

        j = 0
        result = list()
        for i in range(len(a)):
            #  axis ceased to exist  or we're out of b
            if isinstance(a[i], int) or j == len(b):
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
