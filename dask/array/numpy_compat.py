from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion

import numpy as np
import warnings

_numpy_116 = LooseVersion(np.__version__) >= "1.16.0"


# Taken from scikit-learn:
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/utils/fixes.py#L84
try:
    with warnings.catch_warnings():
        if (
            not np.allclose(
                np.divide(0.4, 1, casting="unsafe"),
                np.divide(0.4, 1, casting="unsafe", dtype=np.float),
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


if LooseVersion(np.__version__) < "1.15.0":
    # These functions were added in numpy 1.15.0. For previous versions they
    # are duplicated here

    def _make_along_axis_idx(arr_shape, indices, axis):
        # compute dimensions to iterate over
        if not np.issubdtype(indices.dtype, np.integer):
            raise IndexError("`indices` must be an integer array")
        if len(arr_shape) != indices.ndim:
            raise ValueError(
                "`indices` and `arr` must have the same number of dimensions"
            )
        shape_ones = (1,) * indices.ndim
        dest_dims = list(range(axis)) + [None] + list(range(axis + 1, indices.ndim))

        # build a fancy index, consisting of orthogonal aranges, with the
        # requested index inserted at the right location
        fancy_index = []
        for dim, n in zip(dest_dims, arr_shape):
            if dim is None:
                fancy_index.append(indices)
            else:
                ind_shape = shape_ones[:dim] + (-1,) + shape_ones[dim + 1 :]
                fancy_index.append(np.arange(n).reshape(ind_shape))

        return tuple(fancy_index)

    def take_along_axis(arr, indices, axis):
        """
        Take values from the input array by matching 1d index and data slices.
        This iterates over matching 1d slices oriented along the specified axis in
        the index and data arrays, and uses the former to look up values in the
        latter. These slices can be different lengths.
        Functions returning an index along an axis, like `argsort` and
        `argpartition`, produce suitable indices for this function.
        .. versionadded:: 1.15.0
        Parameters
        ----------
        arr: ndarray (Ni..., M, Nk...)
            Source array
        indices: ndarray (Ni..., J, Nk...)
            Indices to take along each 1d slice of `arr`. This must match the
            dimension of arr, but dimensions Ni and Nj only need to broadcast
            against `arr`.
        axis: int
            The axis to take 1d slices along. If axis is None, the input array is
            treated as if it had first been flattened to 1d, for consistency with
            `sort` and `argsort`.
        Returns
        -------
        out: ndarray (Ni..., J, Nk...)
            The indexed result.
        Notes
        -----
        This is equivalent to (but faster than) the following use of `ndindex` and
        `s_`, which sets each of ``ii`` and ``kk`` to a tuple of indices::
            Ni, M, Nk = a.shape[:axis], a.shape[axis], a.shape[axis+1:]
            J = indices.shape[axis]  # Need not equal M
            out = np.empty(Nk + (J,) + Nk)
            for ii in ndindex(Ni):
                for kk in ndindex(Nk):
                    a_1d       = a      [ii + s_[:,] + kk]
                    indices_1d = indices[ii + s_[:,] + kk]
                    out_1d     = out    [ii + s_[:,] + kk]
                    for j in range(J):
                        out_1d[j] = a_1d[indices_1d[j]]
        Equivalently, eliminating the inner loop, the last two lines would be::
                    out_1d[:] = a_1d[indices_1d]
        See Also
        --------
        take : Take along an axis, using the same indices for every 1d slice
        put_along_axis :
            Put values into the destination array by matching 1d index and data slices
        Examples
        --------
        For this sample array
        >>> a = np.array([[10, 30, 20], [60, 40, 50]])

        We can sort either by using sort directly, or argsort and this function
        >>> np.sort(a, axis=1)
        array([[10, 20, 30],
               [40, 50, 60]])
        >>> ai = np.argsort(a, axis=1); ai
        array([[0, 2, 1],
               [1, 2, 0]])
        >>> take_along_axis(a, ai, axis=1)
        array([[10, 20, 30],
               [40, 50, 60]])

        The same works for max and min, if you expand the dimensions:
        >>> np.expand_dims(np.max(a, axis=1), axis=1)
        array([[30],
               [60]])
        >>> ai = np.expand_dims(np.argmax(a, axis=1), axis=1)
        >>> ai
        array([[1],
               [0]])
        >>> take_along_axis(a, ai, axis=1)
        array([[30],
               [60]])

        If we want to get the max and min at the same time,
        we can stack the indices first:
        >>> ai_min = np.expand_dims(np.argmin(a, axis=1), axis=1)
        >>> ai_max = np.expand_dims(np.argmax(a, axis=1), axis=1)
        >>> ai = np.concatenate([ai_min, ai_max], axis=1)
        >>> ai
        array([[0, 1],
               [1, 0]])
        >>> take_along_axis(a, ai, axis=1)
        array([[10, 30],
               [40, 60]])
        """
        # normalize inputs
        if axis is None:
            arr = arr.flat
            arr_shape = (len(arr),)  # flatiter has no .shape
            axis = 0
        else:
            if axis < 0:
                axis = arr.ndim + axis
            arr_shape = arr.shape

        # use the fancy index
        return arr[_make_along_axis_idx(arr_shape, indices, axis)]


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


if LooseVersion(np.__version__) >= LooseVersion("1.16.0") or LooseVersion(
    np.__version__
) == LooseVersion("1.14.0"):
    _make_sliced_dtype = _make_sliced_dtype_np_ge_16
else:
    _make_sliced_dtype = _make_sliced_dtype_np_lt_14


class _Recurser(object):
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
