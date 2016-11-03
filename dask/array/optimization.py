from __future__ import absolute_import, division, print_function

from operator import getitem

import numpy as np

from .core import getarray, getarray_nofancy
from ..core import flatten
from ..optimize import cull, fuse, inline_functions


def optimize(dsk, keys, fuse_keys=None, fast_functions=None,
             inline_functions_fast_functions=None, **kwargs):
    """ Optimize dask for array computation

    1.  Cull tasks not necessary to evaluate keys
    2.  Remove full slicing, e.g. x[:]
    3.  Inline fast functions like getitem and np.transpose
    """
    keys = list(flatten(keys))
    if fast_functions is not None:
        inline_functions_fast_functions = fast_functions

    if inline_functions_fast_functions is None:
        inline_functions_fast_functions = {getarray, getarray_nofancy,
                                           np.transpose}

    dsk2, dependencies = cull(dsk, keys)
    dsk4, dependencies = fuse(dsk2, keys + (fuse_keys or []), dependencies)
    dsk5 = optimize_slices(dsk4)
    dsk6 = inline_functions(dsk5, keys, dependencies=dependencies,
                            fast_functions=inline_functions_fast_functions)

    return dsk6


def optimize_slices(dsk):
    """ Optimize slices

    1.  Fuse repeated slices, like x[5:][2:6] -> x[7:11]
    2.  Remove full slices, like         x[:] -> x

    See also:
        fuse_slice_dict
    """
    fancy_ind_types = (list, np.ndarray)
    getters = (getarray_nofancy, getarray, getitem)
    dsk = dsk.copy()
    for k, v in dsk.items():
        if type(v) is tuple and v[0] in getters and len(v) == 3:
            f, a, a_index = v
            getter = f
            while type(a) is tuple and a[0] in getters and len(a) == 3:
                f2, b, b_index = a
                if (type(a_index) is tuple) != (type(b_index) is tuple):
                    break
                if type(a_index) is tuple:
                    indices = b_index + a_index
                    if (len(a_index) != len(b_index) and
                            any(i is None for i in indices)):
                        break
                    if (f2 is getarray_nofancy and
                            any(isinstance(i, fancy_ind_types) for i in indices)):
                        break
                elif (f2 is getarray_nofancy and
                        (type(a_index) in fancy_ind_types or
                         type(b_index) in fancy_ind_types)):
                    break
                try:
                    c_index = fuse_slice(b_index, a_index)
                    # rely on fact that nested gets never decrease in
                    # strictness e.g. `(getarray, (getitem, ...))` never
                    # happens
                    getter = f2
                except NotImplementedError:
                    break
                a, a_index = b, c_index
            if getter is not getitem:
                dsk[k] = (getter, a, a_index)
            elif (type(a_index) is slice and
                    not a_index.start and
                    a_index.stop is None and
                    a_index.step is None):
                dsk[k] = a
            elif type(a_index) is tuple and all(type(s) is slice and
                                                not s.start and
                                                s.stop is None and
                                                s.step is None
                                                for s in a_index):
                dsk[k] = a
            else:
                dsk[k] = (getitem, a, a_index)
    return dsk


def _pick_index(seq, ind):
    #Pick an index from the last level of a nested sequence:
    return [_pick_index(x, ind) if isinstance(x, list) else x[ind] for x in seq]


def add_slices_or_lists(slices, axis=0):
    if _count_levels(slices) > 0:
        #Recursively add slices if not at lowest level:
        new_slices = [add_slices_or_lists(slc, axis=axis + 1) for slc in slices]
    else:
        #At lowest level, add slices:
        new_slices = slices

    if len(list(flatten(new_slices))) == 1:
        #If there is only one slice, remove from list:
        return new_slices[0]
    else:
        #Keep track of level by add slices along axis corresponding to current level:
        #Add call to list for 2/3 compatibility:
        other_slices = list(zip(*new_slices))
        slices_to_combine = other_slices.pop(axis)

        output_slices = [slc[0] for slc in other_slices]
        if (all([isinstance(slc, slice) for slc in slices_to_combine]) and
                len(set([slc.step for slc in slices_to_combine])) == 1):

            #All slices, same step:
            possible_slice = slice(slices_to_combine[0].start,
                                   slices_to_combine[-1].stop,
                                   slices_to_combine[0].step)
            if ([slc.stop for slc in slices_to_combine][:-1] ==
                    [slc.start for slc in slices_to_combine][1:]):
                #Continous works:
                output_slices.insert(axis, possible_slice)
            else:
                #Check if continuous works:
                list_of_indices = []
                for slc in slices_to_combine:
                    list_of_indices.extend(range(slc.stop)[slc])
                if list_of_indices == range(possible_slice.stop)[possible_slice]:
                    #It works, insert:
                    output_slices.insert(axis, possible_slice)
                else:
                    #It does not work. Insert list of indices:
                    output_slices.insert(axis, list_of_indices)
            return tuple(output_slices)
        elif all([(isinstance(slc, slice) or isinstance(slc, list))
                  for slc in slices_to_combine]):
            #Mixed slicing:
            output_slices.insert(axis, [])
            for slc in slices_to_combine:
                if isinstance(slc, slice):
                    output_slices[axis].extend(range(slc.start, slc.stop)[slc])
                else:
                    output_slices[axis].extend(slc)
            return tuple(output_slices)
        else:
            return new_slices


def _count_levels(l):
    #Count levels in nested lists:
    return sum(1 + _count_levels(i) for i in l if isinstance(i, list))


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
