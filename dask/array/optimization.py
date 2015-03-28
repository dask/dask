from ..optimize import cull, fuse
from ..core import flatten
from ..async import inline_functions
from ..optimize import dealias
from operator import getitem
from dask.rewrite import RuleSet, RewriteRule
from toolz import valmap
import numpy as np


def optimize(dsk, keys, **kwargs):
    """ Optimize dask for array computation

    1.  Cull tasks not necessary to evaluate keys
    2.  Remove full slicing, e.g. x[:]
    3.  Inline fast functions like getitem and np.transpose
    """
    fast_functions=kwargs.get('fast_functions',
                             set([getitem, np.transpose]))
    dsk2 = cull(dsk, list(flatten(keys)))
    dsk3 = remove_full_slices(dsk2)
    dsk4 = fuse(dsk3)
    dsk5 = valmap(rewrite_rules.rewrite, dsk4)
    dsk6 = inline_functions(dsk5, fast_functions=fast_functions)
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
            task[0] == getitem and
            (task[2] == slice(None, None, None) or
             isinstance(task[2], tuple) and
             all(ind == slice(None, None, None) for ind in task[2])))


def remove_full_slices(dsk):
    """ Remove full slices from dask

    See Also:
        dask.optimize.inline

    Example
    -------


    >>> dsk = {'a': (range, 5),
    ...        'b': (getitem, 'a', (slice(None, None, None),)),
    ...        'c': (getitem, 'b', (slice(None, None, None),)),
    ...        'd': (getitem, 'c', (slice(None, 5, None),)),
    ...        'e': (getitem, 'd', (slice(None, None, None),))}

    >>> remove_full_slices(dsk)  # doctest: +SKIP
    {'a': (range, 5),
     'e': (getitem, 'a', (slice(None, 5, None),))}
    """
    full_slice_keys = set(k for k, task in dsk.items() if is_full_slice(task))
    dsk2 = dict((k, task[1] if k in full_slice_keys else task)
                 for k, task in dsk.items())
    dsk3 = dealias(dsk2)
    return dsk3


a, b, x = '~a', '~b', '~x'


def fuse_slice_dict(match):
    # Making new dimensions?  Need two getitems
    # TODO: well, we could still optimize the non-None axes

    try:
        c = fuse_slice(match[a], match[b])
        return (getitem, match[x], c)
    except NotImplementedError:
        return (getitem, (getitem, match[x], match[a]),
                         match[b])


rewrite_rules = RuleSet(RewriteRule((getitem, (getitem, x, a), b),
                                    fuse_slice_dict,
                                    (a, b, x)))


def normalize_slice(s):
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

    >>> fuse_slice(slice(1000, 2000), slice(10, 15))
    slice(1010, 1015, None)

    >>> fuse_slice(slice(1000, 2000), 10)
    1010

    >>> fuse_slice(slice(1000, 2000, 5), 10)
    1050

    >>> fuse_slice(slice(1000, 2000, 5), slice(10, 20, 2))
    slice(1050, 1100, 10)

    >>> fuse_slice(None, slice(None, None))
    None
    """
    if a is None and b == slice(None, None):
        return None
    if isinstance(a, slice):
        a = normalize_slice(a)
    if isinstance(b, slice):
        b = normalize_slice(b)

    if isinstance(a, slice) and isinstance(b, int):
        return a.start + b*(a.step if a.step is not None else 1)
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
    if isinstance(a, tuple) and not isinstance(b, tuple):
        b = (b,)
    if isinstance(a, tuple) and isinstance(b, tuple):
        j = 0
        result = list()
        for i in range(len(a)):
            if isinstance(a[i], int):
                result.append(a[i])
                continue
            while b[j] is None:  # insert Nones on the rhs
                result.append(None)
                j += 1
            result.append(fuse_slice(a[i], b[j]))
            j += 1
        while j < len(b):  # anything leftover on the right?
            assert b[j] is None
            result.append(None)
            j += 1
        return tuple(result)
    raise NotImplementedError()
