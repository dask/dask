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
                            np.divide(.4, 1, casting="unsafe", dtype=np.float)) or
            not np.allclose(np.divide(1, .5, dtype='i8'), 2) or
           not np.allclose(np.divide(.4, 1), .4)):
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
    from numpy import broadcast_to, nanprod, nancumsum, nancumprod
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

    def _replace_nan(a, val):
        """
        If `a` is of inexact type, make a copy of `a`, replace NaNs with
        the `val` value, and return the copy together with a boolean mask
        marking the locations where NaNs were present. If `a` is not of
        inexact type, do nothing and return `a` together with a mask of None.

        Note that scalars will end up as array scalars, which is important
        for using the result as the value of the out argument in some
        operations.

        Parameters
        ----------
        a : array-like
            Input array.
        val : float
            NaN values are set to val before doing the operation.

        Returns
        -------
        y : ndarray
            If `a` is of inexact type, return a copy of `a` with the NaNs
            replaced by the fill value, otherwise return `a`.
        mask: {bool, None}
            If `a` is of inexact type, return a boolean mask marking locations of
            NaNs, otherwise return None.

        """
        is_new = not isinstance(a, np.ndarray)
        if is_new:
            a = np.array(a)
        if not issubclass(a.dtype.type, np.inexact):
            return a, None
        if not is_new:
            # need copy
            a = np.array(a, subok=True)

        mask = np.isnan(a)
        np.copyto(a, val, where=mask)
        return a, mask

    def nanprod(a, axis=None, dtype=None, out=None, keepdims=0):
        """
        Return the product of array elements over a given axis treating Not a
        Numbers (NaNs) as zero.

        One is returned for slices that are all-NaN or empty.

        .. versionadded:: 1.10.0

        Parameters
        ----------
        a : array_like
            Array containing numbers whose sum is desired. If `a` is not an
            array, a conversion is attempted.
        axis : int, optional
            Axis along which the product is computed. The default is to compute
            the product of the flattened array.
        dtype : data-type, optional
            The type of the returned array and of the accumulator in which the
            elements are summed.  By default, the dtype of `a` is used.  An
            exception is when `a` has an integer type with less precision than
            the platform (u)intp. In that case, the default will be either
            (u)int32 or (u)int64 depending on whether the platform is 32 or 64
            bits. For inexact inputs, dtype must be inexact.
        out : ndarray, optional
            Alternate output array in which to place the result.  The default
            is ``None``. If provided, it must have the same shape as the
            expected output, but the type will be cast if necessary.  See
            `doc.ufuncs` for details. The casting of NaN to integer can yield
            unexpected results.
        keepdims : bool, optional
            If True, the axes which are reduced are left in the result as
            dimensions with size one. With this option, the result will
            broadcast correctly against the original `arr`.

        Returns
        -------
        y : ndarray or numpy scalar

        See Also
        --------
        numpy.prod : Product across array propagating NaNs.
        isnan : Show which elements are NaN.

        Notes
        -----
        Numpy integer arithmetic is modular. If the size of a product exceeds
        the size of an integer accumulator, its value will wrap around and the
        result will be incorrect. Specifying ``dtype=double`` can alleviate
        that problem.

        Examples
        --------
        >>> np.nanprod(1)
        1
        >>> np.nanprod([1])
        1
        >>> np.nanprod([1, np.nan])
        1.0
        >>> a = np.array([[1, 2], [3, np.nan]])
        >>> np.nanprod(a)
        6.0
        >>> np.nanprod(a, axis=0)
        array([ 3.,  2.])

        """
        a, mask = _replace_nan(a, 1)
        return np.prod(a, axis=axis, dtype=dtype, out=out, keepdims=keepdims)

    def nancumsum(a, axis=None, dtype=None, out=None):
        """
        Return the cumulative sum of array elements over a given axis treating Not a
        Numbers (NaNs) as zero.  The cumulative sum does not change when NaNs are
        encountered and leading NaNs are replaced by zeros.

        Zeros are returned for slices that are all-NaN or empty.

        .. versionadded:: 1.12.0

        Parameters
        ----------
        a : array_like
            Input array.
        axis : int, optional
            Axis along which the cumulative sum is computed. The default
            (None) is to compute the cumsum over the flattened array.
        dtype : dtype, optional
            Type of the returned array and of the accumulator in which the
            elements are summed.  If `dtype` is not specified, it defaults
            to the dtype of `a`, unless `a` has an integer dtype with a
            precision less than that of the default platform integer.  In
            that case, the default platform integer is used.
        out : ndarray, optional
            Alternative output array in which to place the result. It must
            have the same shape and buffer length as the expected output
            but the type will be cast if necessary. See `doc.ufuncs`
            (Section "Output arguments") for more details.

        Returns
        -------
        nancumsum : ndarray.
            A new array holding the result is returned unless `out` is
            specified, in which it is returned. The result has the same
            size as `a`, and the same shape as `a` if `axis` is not None
            or `a` is a 1-d array.

        See Also
        --------
        numpy.cumsum : Cumulative sum across array propagating NaNs.
        isnan : Show which elements are NaN.

        Examples
        --------
        >>> np.nancumsum(1) #doctest: +SKIP
        array([1])
        >>> np.nancumsum([1]) #doctest: +SKIP
        array([1])
        >>> np.nancumsum([1, np.nan]) #doctest: +SKIP
        array([ 1.,  1.])
        >>> a = np.array([[1, 2], [3, np.nan]])
        >>> np.nancumsum(a) #doctest: +SKIP
        array([ 1.,  3.,  6.,  6.])
        >>> np.nancumsum(a, axis=0) #doctest: +SKIP
        array([[ 1.,  2.],
               [ 4.,  2.]])
        >>> np.nancumsum(a, axis=1) #doctest: +SKIP
        array([[ 1.,  3.],
               [ 3.,  3.]])

        """
        a, mask = _replace_nan(a, 0)
        return np.cumsum(a, axis=axis, dtype=dtype, out=out)

    def nancumprod(a, axis=None, dtype=None, out=None):
        """
        Return the cumulative product of array elements over a given axis treating Not a
        Numbers (NaNs) as one.  The cumulative product does not change when NaNs are
        encountered and leading NaNs are replaced by ones.

        Ones are returned for slices that are all-NaN or empty.

        .. versionadded:: 1.12.0

        Parameters
        ----------
        a : array_like
            Input array.
        axis : int, optional
            Axis along which the cumulative product is computed.  By default
            the input is flattened.
        dtype : dtype, optional
            Type of the returned array, as well as of the accumulator in which
            the elements are multiplied.  If *dtype* is not specified, it
            defaults to the dtype of `a`, unless `a` has an integer dtype with
            a precision less than that of the default platform integer.  In
            that case, the default platform integer is used instead.
        out : ndarray, optional
            Alternative output array in which to place the result. It must
            have the same shape and buffer length as the expected output
            but the type of the resulting values will be cast if necessary.

        Returns
        -------
        nancumprod : ndarray
            A new array holding the result is returned unless `out` is
            specified, in which case it is returned.

        See Also
        --------
        numpy.cumprod : Cumulative product across array propagating NaNs.
        isnan : Show which elements are NaN.

        Examples
        --------
        >>> np.nancumprod(1) #doctest: +SKIP
        array([1])
        >>> np.nancumprod([1]) #doctest: +SKIP
        array([1])
        >>> np.nancumprod([1, np.nan]) #doctest: +SKIP
        array([ 1.,  1.])
        >>> a = np.array([[1, 2], [3, np.nan]])
        >>> np.nancumprod(a) #doctest: +SKIP
        array([ 1.,  2.,  6.,  6.])
        >>> np.nancumprod(a, axis=0) #doctest: +SKIP
        array([[ 1.,  2.],
               [ 3.,  2.]])
        >>> np.nancumprod(a, axis=1) #doctest: +SKIP
        array([[ 1.,  2.],
               [ 3.,  3.]])

        """
        a, mask = _replace_nan(a, 1)
        return np.cumprod(a, axis=axis, dtype=dtype, out=out)
