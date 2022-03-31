import warnings

import numpy as np
from packaging.version import parse as parse_version

from dask.utils import derived_from

_np_version = parse_version(np.__version__)
_numpy_120 = _np_version >= parse_version("1.20.0")
_numpy_121 = _np_version >= parse_version("1.21.0")
_numpy_122 = _np_version >= parse_version("1.22.0")


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
    def divide(x1, x2, out=None, dtype=None):  # type: ignore
        """Implementation of numpy.divide that works with dtype kwarg.

        Temporary compatibility fix for a bug in numpy's version. See
        https://github.com/numpy/numpy/issues/3484 for the relevant issue."""
        x = np.divide(x1, x2, out)
        if dtype is not None:
            x = x.astype(dtype)
        return x

    ma_divide = np.ma.core._DomainedBinaryOperation(  # type: ignore
        divide, np.ma.core._DomainSafeDivide(), 0, 1  # type: ignore
    )


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
        **kwargs,
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
            yield from self.walk(xi, index + (i,))


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


if _numpy_120:
    sliding_window_view = np.lib.stride_tricks.sliding_window_view
else:
    # copied from numpy.lib.stride_tricks
    # https://github.com/numpy/numpy/blob/0721406ede8b983b8689d8b70556499fc2aea28a/numpy/lib/stride_tricks.py#L122-L336
    def sliding_window_view(
        x, window_shape, axis=None, *, subok=False, writeable=False
    ):
        """
        Create a sliding window view into the array with the given window shape.
        Also known as rolling or moving window, the window slides across all
        dimensions of the array and extracts subsets of the array at all window
        positions.

        .. versionadded:: 1.20.0
        Parameters
        ----------
        x : array_like
            Array to create the sliding window view from.
        window_shape : int or tuple of int
            Size of window over each axis that takes part in the sliding window.
            If `axis` is not present, must have same length as the number of input
            array dimensions. Single integers `i` are treated as if they were the
            tuple `(i,)`.
        axis : int or tuple of int, optional
            Axis or axes along which the sliding window is applied.
            By default, the sliding window is applied to all axes and
            `window_shape[i]` will refer to axis `i` of `x`.
            If `axis` is given as a `tuple of int`, `window_shape[i]` will refer to
            the axis `axis[i]` of `x`.
            Single integers `i` are treated as if they were the tuple `(i,)`.
        subok : bool, optional
            If True, sub-classes will be passed-through, otherwise the returned
            array will be forced to be a base-class array (default).
        writeable : bool, optional
            When true, allow writing to the returned view. The default is false,
            as this should be used with caution: the returned view contains the
            same memory location multiple times, so writing to one location will
            cause others to change.
        Returns
        -------
        view : ndarray
            Sliding window view of the array. The sliding window dimensions are
            inserted at the end, and the original dimensions are trimmed as
            required by the size of the sliding window.
            That is, ``view.shape = x_shape_trimmed + window_shape``, where
            ``x_shape_trimmed`` is ``x.shape`` with every entry reduced by one less
            than the corresponding window size.
        """
        from numpy.core.numeric import normalize_axis_tuple

        window_shape = (
            tuple(window_shape) if np.iterable(window_shape) else (window_shape,)
        )
        # first convert input to array, possibly keeping subclass
        x = np.array(x, copy=False, subok=subok)

        window_shape_array = np.array(window_shape)
        if np.any(window_shape_array < 0):
            raise ValueError("`window_shape` cannot contain negative values")

        if axis is None:
            axis = tuple(range(x.ndim))
            if len(window_shape) != len(axis):
                raise ValueError(
                    f"Since axis is `None`, must provide "
                    f"window_shape for all dimensions of `x`; "
                    f"got {len(window_shape)} window_shape elements "
                    f"and `x.ndim` is {x.ndim}."
                )
        else:
            axis = normalize_axis_tuple(axis, x.ndim, allow_duplicate=True)
            if len(window_shape) != len(axis):
                raise ValueError(
                    f"Must provide matching length window_shape and "
                    f"axis; got {len(window_shape)} window_shape "
                    f"elements and {len(axis)} axes elements."
                )

        out_strides = x.strides + tuple(x.strides[ax] for ax in axis)

        # note: same axis can be windowed repeatedly
        x_shape_trimmed = list(x.shape)
        for ax, dim in zip(axis, window_shape):
            if x_shape_trimmed[ax] < dim:
                raise ValueError("window shape cannot be larger than input array shape")
            x_shape_trimmed[ax] -= dim - 1
        out_shape = tuple(x_shape_trimmed) + window_shape
        return np.lib.stride_tricks.as_strided(
            x, strides=out_strides, shape=out_shape, subok=subok, writeable=writeable
        )


# kwarg is renamed in numpy 1.22.0
def percentile(a, q, method="linear"):
    if _numpy_122:
        return np.percentile(a, q, method=method)
    else:
        return np.percentile(a, q, interpolation=method)


if _numpy_120:
    from numpy.typing import ArrayLike, DTypeLike
else:
    from typing import Any

    ArrayLike = DTypeLike = Any  # type: ignore
