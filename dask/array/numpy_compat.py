from __future__ import absolute_import, division, print_function

from ..compatibility import builtins
import numpy as np
import warnings

try:
    isclose = np.isclose
except AttributeError:
    def isclose(*args, **kwargs):
        raise RuntimeError("You need numpy version 1.7 or greater to use "
                           "isclose.")

try:
    full = np.full
except AttributeError:
    def full(shape, fill_value, dtype=None, order=None):
        """Our implementation of numpy.full because your numpy is old."""
        if order is not None:
            raise NotImplementedError("`order` kwarg is not supported upgrade "
                                      "to Numpy 1.8 or greater for support.")
        return np.multiply(fill_value, np.ones(shape, dtype=dtype),
                           dtype=dtype)


# Taken from scikit-learn:
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/utils/fixes.py#L84
try:
    with warnings.catch_warnings():
        if (not np.allclose(np.divide(.4, 1, casting="unsafe"),
                            np.divide(.4, 1, casting="unsafe", dtype=np.float))
                or not np.allclose(np.divide(1, .5, dtype='i8'), 2)
                or not np.allclose(np.divide(.4, 1), .4)):
            raise TypeError('Divide not working with dtype: '
                            'https://github.com/numpy/numpy/issues/3484')
        divide = np.divide

except TypeError:
    # Divide with dtype doesn't work on Python 3
    def divide(x1, x2, out=None, dtype=None):
        """Implementation of numpy.divide that works with dtype kwarg.

        Temporary compatibility fix for a bug in numpy's version. See
        https://github.com/numpy/numpy/issues/3484 for the relevant issue."""
        x = np.divide(x1, x2, out)
        if dtype is not None:
            x = x.astype(dtype)
        return x

# functions copied from numpy
try:
    from numpy import broadcast_to
except ImportError:  # pragma: no cover
    # these functions should arrive in numpy v1.10 to v1.12.  Until then,
    # they are duplicated here

    # See https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory
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
