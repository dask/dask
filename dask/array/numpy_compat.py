from __future__ import absolute_import, division, print_function

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
