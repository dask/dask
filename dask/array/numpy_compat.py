from distutils.version import LooseVersion

import numpy as np
import warnings

from ..utils import derived_from

_numpy_116 = LooseVersion(np.__version__) >= "1.16.0"
_numpy_117 = LooseVersion(np.__version__) >= "1.17.0"
_numpy_118 = LooseVersion(np.__version__) >= "1.18.0"
_numpy_120 = LooseVersion(np.__version__) >= "1.20.0"


# Taken from scikit-learn:
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/utils/fixes.py#L84
try:
    with warnings.catch_warnings():
        if (
            not np.allclose(
                np.divide(0.4, 1, casting="unsafe"),
                np.divide(0.4, 1, casting="unsafe", dtype=float),
            )
            or not np.allclose(np.divide(1, 0.5, dtype="i8"), 2)
            or not np.allclose(np.divide(0.4, 1), 0.4)
        ):
            raise TypeError(
                "Divide not working with dtype: "
                "https://github.com/numpy/numpy/issues/3484"
            )
        divide = np.divide
        ma_divide = np.ma.divide

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

    ma_divide = np.ma.core._DomainedBinaryOperation(
        divide, np.ma.core._DomainSafeDivide(), 0, 1
    )


def _make_sliced_dtype_np_ge_16(dtype, index):
    # This was briefly added in 1.14.0
    # https://github.com/numpy/numpy/pull/6053, NumPy >= 1.14
    # which was then reverted in 1.14.1 with
    # https://github.com/numpy/numpy/pull/10411
    # And then was finally released with
    # https://github.com/numpy/numpy/pull/12447
    # in version 1.16.0
    new = {
        "names": index,
        "formats": [dtype.fields[name][0] for name in index],
        "offsets": [dtype.fields[name][1] for name in index],
        "itemsize": dtype.itemsize,
    }
    return np.dtype(new)


def _make_sliced_dtype_np_lt_14(dtype, index):
    # For numpy < 1.14
    dt = np.dtype([(name, dtype[name]) for name in index])
    return dt


if LooseVersion(np.__version__) >= LooseVersion("1.16.0"):
    _make_sliced_dtype = _make_sliced_dtype_np_ge_16
else:
    _make_sliced_dtype = _make_sliced_dtype_np_lt_14


class _Recurser:
    """
    Utility class for recursing over nested iterables
    """

    # This was copied almost verbatim from numpy.core.shape_base._Recurser
    # See numpy license at https://github.com/numpy/numpy/blob/master/LICENSE.txt
    # or NUMPY_LICENSE.txt within this directory

    def __init__(self, recurse_if):
        self.recurse_if = recurse_if

    def map_reduce(
        self,
        x,
        f_map=lambda x, **kwargs: x,
        f_reduce=lambda x, **kwargs: x,
        f_kwargs=lambda **kwargs: kwargs,
        **kwargs
    ):
        """
        Iterate over the nested list, applying:
        * ``f_map`` (T -> U) to items
        * ``f_reduce`` (Iterable[U] -> U) to mapped items

        For instance, ``map_reduce([[1, 2], 3, 4])`` is::

            f_reduce([
              f_reduce([
                f_map(1),
                f_map(2)
              ]),
              f_map(3),
              f_map(4)
            ]])


        State can be passed down through the calls with `f_kwargs`,
        to iterables of mapped items. When kwargs are passed, as in
        ``map_reduce([[1, 2], 3, 4], **kw)``, this becomes::

            kw1 = f_kwargs(**kw)
            kw2 = f_kwargs(**kw1)
            f_reduce([
              f_reduce([
                f_map(1), **kw2)
                f_map(2,  **kw2)
              ],      **kw1),
              f_map(3, **kw1),
              f_map(4, **kw1)
            ]],     **kw)
        """

        def f(x, **kwargs):
            if not self.recurse_if(x):
                return f_map(x, **kwargs)
            else:
                next_kwargs = f_kwargs(**kwargs)
                return f_reduce((f(xi, **next_kwargs) for xi in x), **kwargs)

        return f(x, **kwargs)

    def walk(self, x, index=()):
        """
        Iterate over x, yielding (index, value, entering), where

        * ``index``: a tuple of indices up to this point
        * ``value``: equal to ``x[index[0]][...][index[-1]]``. On the first iteration, is
                     ``x`` itself
        * ``entering``: bool. The result of ``recurse_if(value)``
        """
        do_recurse = self.recurse_if(x)
        yield index, x, do_recurse

        if not do_recurse:
            return
        for i, xi in enumerate(x):
            # yield from ...
            for v in self.walk(xi, index + (i,)):
                yield v


if _numpy_116:
    _unravel_index_keyword = "shape"
else:
    _unravel_index_keyword = "dims"


# Implementation taken directly from numpy:
# https://github.com/numpy/numpy/blob/d9b1e32cb8ef90d6b4a47853241db2a28146a57d/numpy/core/numeric.py#L1336-L1405
@derived_from(np)
def moveaxis(a, source, destination):
    source = np.core.numeric.normalize_axis_tuple(source, a.ndim, "source")
    destination = np.core.numeric.normalize_axis_tuple(
        destination, a.ndim, "destination"
    )
    if len(source) != len(destination):
        raise ValueError(
            "`source` and `destination` arguments must have "
            "the same number of elements"
        )

    order = [n for n in range(a.ndim) if n not in source]

    for dest, src in sorted(zip(destination, source)):
        order.insert(dest, src)

    result = a.transpose(order)
    return result


# Implementation adapted directly from numpy:
# https://github.com/numpy/numpy/blob/v1.17.0/numpy/core/numeric.py#L1107-L1204
def rollaxis(a, axis, start=0):
    n = a.ndim
    axis = np.core.numeric.normalize_axis_index(axis, n)
    if start < 0:
        start += n
    msg = "'%s' arg requires %d <= %s < %d, but %d was passed in"
    if not (0 <= start < n + 1):
        raise ValueError(msg % ("start", -n, "start", n + 1, start))
    if axis < start:
        # it's been removed
        start -= 1
    if axis == start:
        return a[...]
    axes = list(range(0, n))
    axes.remove(axis)
    axes.insert(start, axis)
    return a.transpose(axes)
