""" A set of NumPy functions to apply per chunk """

from __future__ import absolute_import, division, print_function

from collections import Container, Iterable, Sequence
from functools import wraps
from inspect import getargspec

from toolz import concat
import numpy as np

from ..compatibility import builtins
from ..utils import ignoring


def keepdims_wrapper(a_callable):
    """
    A wrapper for functions that don't provide keepdims to ensure that they do.
    """

    if "keepdims" in getargspec(a_callable).args:
        return a_callable

    @wraps(a_callable)
    def keepdims_wrapped_callable(x, axis=None, keepdims=None, *args, **kwargs):
        r = a_callable(x, axis=axis, *args, **kwargs)

        if not keepdims:
            return r

        axes = axis

        if axes is None:
            axes = range(x.ndim)

        if not isinstance(axes, (Container, Iterable, Sequence)):
            axes = [axes]

        r_slice = tuple()
        for each_axis in range(x.ndim):
            if each_axis in axes:
                r_slice += (None,)
            else:
                r_slice += (slice(None),)

        r = r[r_slice]

        return r

    return keepdims_wrapped_callable


# Wrap NumPy functions to ensure they provide keepdims.
sum = keepdims_wrapper(np.sum)
prod = keepdims_wrapper(np.prod)
min = keepdims_wrapper(np.min)
max = keepdims_wrapper(np.max)
argmin = keepdims_wrapper(np.argmin)
nanargmin = keepdims_wrapper(np.nanargmin)
argmax = keepdims_wrapper(np.argmax)
nanargmax = keepdims_wrapper(np.nanargmax)
any = keepdims_wrapper(np.any)
all = keepdims_wrapper(np.all)
nansum = keepdims_wrapper(np.nansum)

with ignoring(AttributeError):
    nanprod = keepdims_wrapper(np.nanprod)

nanmin = keepdims_wrapper(np.nanmin)
nanmax = keepdims_wrapper(np.nanmax)
mean = keepdims_wrapper(np.mean)

with ignoring(AttributeError):
    nanmean = keepdims_wrapper(np.nanmean)

var = keepdims_wrapper(np.var)

with ignoring(AttributeError):
    nanvar = keepdims_wrapper(np.nanvar)

std = keepdims_wrapper(np.std)

with ignoring(AttributeError):
    nanstd = keepdims_wrapper(np.nanstd)


def coarsen(reduction, x, axes):
    """ Coarsen array by applying reduction to fixed size neighborhoods

    Parameters
    ----------

    reduction: function
        Function like np.sum, np.mean, etc...
    x: np.ndarray
        Array to be coarsened
    axes: dict
        Mapping of axis to coarsening factor

    Example
    -------

    >>> x = np.array([1, 2, 3, 4, 5, 6])
    >>> coarsen(np.sum, x, {0: 2})
    array([ 3,  7, 11])
    >>> coarsen(np.max, x, {0: 3})
    array([3, 6])

    Provide dictionary of scale per dimension

    >>> x = np.arange(24).reshape((4, 6))
    >>> x
    array([[ 0,  1,  2,  3,  4,  5],
           [ 6,  7,  8,  9, 10, 11],
           [12, 13, 14, 15, 16, 17],
           [18, 19, 20, 21, 22, 23]])

    >>> coarsen(np.min, x, {0: 2, 1: 3})
    array([[ 0,  3],
           [12, 15]])
    """
    # Insert singleton dimensions if they don't exist already
    for i in range(x.ndim):
        if i not in axes:
            axes[i] = 1

    # (10, 10) -> (5, 2, 5, 2)
    newshape = tuple(concat([(x.shape[i] / axes[i], axes[i])
                                for i in range(x.ndim)]))

    return reduction(x.reshape(newshape), axis=tuple(range(1, x.ndim*2, 2)))


def trim(x, axes=None):
    """ Trim boundaries off of array

    >>> x = np.arange(24).reshape((4, 6))
    >>> trim(x, axes={0: 0, 1: 1})
    array([[ 1,  2,  3,  4],
           [ 7,  8,  9, 10],
           [13, 14, 15, 16],
           [19, 20, 21, 22]])

    >>> trim(x, axes={0: 1, 1: 1})
    array([[ 7,  8,  9, 10],
           [13, 14, 15, 16]])
    """
    if isinstance(axes, int):
        axes = [axes] * x.ndim
    if isinstance(axes, dict):
        axes = [axes.get(i, 0) for i in range(x.ndim)]

    return x[tuple(slice(ax, -ax if ax else None) for ax in axes)]


try:
    from numpy import broadcast_to
except ImportError:  # pragma: no cover
    # broadcast_to will arrive in numpy v1.10. Until then, it is duplicated
    # here:

    # Copyright (c) 2005-2015, NumPy Developers.
    # All rights reserved.

    # Redistribution and use in source and binary forms, with or without
    # modification, are permitted provided that the following conditions are
    # met:

    #     * Redistributions of source code must retain the above copyright
    #        notice, this list of conditions and the following disclaimer.

    #     * Redistributions in binary form must reproduce the above
    #        copyright notice, this list of conditions and the following
    #        disclaimer in the documentation and/or other materials provided
    #        with the distribution.

    #     * Neither the name of the NumPy Developers nor the names of any
    #        contributors may be used to endorse or promote products derived
    #        from this software without specific prior written permission.

    # THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    # "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    # LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    # A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    # OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    # SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    # LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    # DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    # THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    # (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    # OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    def _maybe_view_as_subclass(original_array, new_array):
        if type(original_array) is not type(new_array):
            # if input was an ndarray subclass and subclasses were OK,
            # then view the result as that subclass.
            new_array = new_array.view(type=type(original_array))
            # Since we have done something akin to a view from original_array, we
            # should let the subclass finalize (if it has it implemented, i.e., is
            # not None).
            if new_array.__array_finalize__:
                new_array.__array_finalize__(original_array)
        return new_array


    def _broadcast_to(array, shape, subok, readonly):
        shape = tuple(shape) if np.iterable(shape) else (shape,)
        array = np.array(array, copy=False, subok=subok)
        if not shape and array.shape:
            raise ValueError('cannot broadcast a non-scalar to a scalar array')
        if builtins.any(size < 0 for size in shape):
            raise ValueError('all elements of broadcast shape must be non-'
                             'negative')
        broadcast = np.nditer(
            (array,), flags=['multi_index', 'zerosize_ok', 'refs_ok'],
            op_flags=['readonly'], itershape=shape, order='C').itviews[0]
        result = _maybe_view_as_subclass(array, broadcast)
        if not readonly and array.flags.writeable:
            result.flags.writeable = True
        return result


    def broadcast_to(array, shape, subok=False):
        """Broadcast an array to a new shape.

        Parameters
        ----------
        array : array_like
            The array to broadcast.
        shape : tuple
            The shape of the desired array.
        subok : bool, optional
            If True, then sub-classes will be passed-through, otherwise
            the returned array will be forced to be a base-class array (default).

        Returns
        -------
        broadcast : array
            A readonly view on the original array with the given shape. It is
            typically not contiguous. Furthermore, more than one element of a
            broadcasted array may refer to a single memory location.

        Raises
        ------
        ValueError
            If the array is not compatible with the new shape according to NumPy's
            broadcasting rules.

        Examples
        --------
        >>> x = np.array([1, 2, 3])
        >>> np.broadcast_to(x, (3, 3))  # doctest: +SKIP
        array([[1, 2, 3],
               [1, 2, 3],
               [1, 2, 3]])
        """
        return _broadcast_to(array, shape, subok=subok, readonly=True)
