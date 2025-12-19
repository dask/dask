"""Array conversion functions: from_array, asarray, asanyarray, array."""

from __future__ import annotations

import uuid
import warnings
from collections.abc import Iterable

import numpy as np

from dask.array.core import getter_inline, normalize_chunks
from dask.array.utils import meta_from_array
from dask.base import is_dask_collection, tokenize
from dask.utils import SerializableLock


def _as_dtype(a, dtype):
    """Apply dtype conversion if needed."""
    if dtype is None:
        return a
    else:
        return a.astype(dtype)


def from_array(
    x,
    chunks="auto",
    lock=False,
    asarray=None,
    fancy=True,
    getitem=None,
    meta=None,
    inline_array=False,
    name=None,
):
    """Create dask array from something that looks like an array.

    Input must have a ``.shape``, ``.ndim``, ``.dtype`` and support numpy-style slicing.

    Parameters
    ----------
    x : array_like
    chunks : int, tuple
        How to chunk the array. Must be one of the following forms:

        - A blocksize like 1000.
        - A blockshape like (1000, 1000).
        - Explicit sizes of all blocks along all dimensions like
          ((1000, 1000, 500), (400, 400)).
        - A size in bytes, like "100 MiB" which will choose a uniform
          block-like shape
        - The word "auto" which acts like the above, but uses a configuration
          value ``array.chunk-size`` for the chunk size

        -1 or None as a blocksize indicate the size of the corresponding
        dimension.
    name : str or bool, optional
        The key name to use for the array. Defaults to a hash of ``x``.

        Hashing is useful if the same value of ``x`` is used to create multiple
        arrays, as Dask can then recognise that they're the same and
        avoid duplicate computations. However, it can also be slow, and if the
        array is not contiguous it is copied for hashing. If the array uses
        stride tricks (such as :func:`numpy.broadcast_to` or
        :func:`skimage.util.view_as_windows`) to have a larger logical
        than physical size, this copy can cause excessive memory usage.

        If you don't need the deduplication provided by hashing, use
        ``name=False`` to generate a random name instead of hashing, which
        avoids the pitfalls described above. Using ``name=True`` is
        equivalent to the default.

        By default, hashing uses python's standard sha1. This behaviour can be
        changed by installing cityhash, xxhash or murmurhash. If installed,
        a large-factor speedup can be obtained in the tokenisation step.

        .. note::

           Because this ``name`` is used as the key in task graphs, you should
           ensure that it uniquely identifies the data contained within. If
           you'd like to provide a descriptive name that is still unique, combine
           the descriptive name with :func:`dask.base.tokenize` of the
           ``array_like``. See :ref:`graphs` for more.

    lock : bool or Lock, optional
        If ``x`` doesn't support concurrent reads then provide a lock here, or
        pass in True to have dask.array create one for you.
    asarray : bool, optional
        If True then call np.asarray on chunks to convert them to numpy arrays.
        If False then chunks are passed through unchanged.
        If None (default) then we use True if the ``__array_function__`` method
        is undefined.

        .. note::

            Dask does not preserve the memory layout of the original array when
            the array is created using Fortran rather than C ordering.

    fancy : bool, optional
        If ``x`` doesn't support fancy indexing (e.g. indexing with lists or
        arrays) then set to False. Default is True.
    meta : Array-like, optional
        The metadata for the resulting dask array.  This is the kind of array
        that will result from slicing the input array.
        Defaults to the input array.
    inline_array : bool, default False
        How to include the array in the task graph. By default
        (``inline_array=False``) the array is included in a task by itself,
        and each chunk refers to that task by its key.

        With ``inline_array=True``, Dask will instead inline the array directly
        in the values of the task graph.

        The right choice for ``inline_array`` depends on several factors,
        including the size of ``x``, how expensive it is to create, which
        scheduler you're using, and the pattern of downstream computations.
        As a heuristic, ``inline_array=True`` may be the right choice when
        the array ``x`` is cheap to serialize and deserialize (since it's
        done for every task), and if you expect the scheduler to need to
        move around the inputs relative to the workers. For example, HDF5
        files would be a bad fit for ``inline_array=True``, while small
        NumPy arrays would be a good fit.
    getitem : callable, optional
        Callable with signature (a, index) -> value to use for indexing the
        array. Defaults to :func:`operator.getitem`.

    Examples
    --------
    >>> x = h5py.File('...')['/data/path']  # doctest: +SKIP
    >>> a = da.from_array(x, chunks=(1000, 1000))  # doctest: +SKIP

    If your underlying datastore does not support concurrent reads then include
    the ``lock=True`` keyword argument or ``lock=mylock`` if you want multiple
    arrays to coordinate around the same lock.

    >>> a = da.from_array(x, chunks=(1000, 1000), lock=True)  # doctest: +SKIP

    If your underlying datastore has a ``.chunks`` attribute (as h5py and zarr
    datasets do) then a multiple of that chunk shape will be used if you
    do not provide a chunk shape.

    >>> a = da.from_array(x, chunks='auto')  # doctest: +SKIP
    >>> a = da.from_array(x, chunks='100 MiB')  # doctest: +SKIP
    >>> a = da.from_array(x)  # doctest: +SKIP

    If providing a name, ensure that it is unique

    >>> import dask.base
    >>> token = dask.base.tokenize(x)  # doctest: +SKIP
    >>> a = da.from_array('myarray-' + token)  # doctest: +SKIP

    NumPy ndarrays are chunked into a single chunk

    >>> a = da.from_array(np.array([[1, 2], [3, 4]]))  # doctest: +SKIP
    >>> a.chunks  # doctest: +SKIP
    ((2,), (2,))
    """
    # Lazy imports to avoid circular dependencies
    from dask._collections import new_collection
    from dask.array._array_expr._io import FromArray
    from dask.array.chunk_types import is_valid_chunk_type
    from dask.utils import is_arraylike

    # Import Array for isinstance check
    from dask.array._array_expr._collection import Array

    if isinstance(x, Array):
        raise ValueError(
            "Array is already a dask array. Use 'asarray' or 'rechunk' instead."
        )

    elif is_dask_collection(x):
        warnings.warn(
            "Passing an object to dask.array.from_array which is already a "
            "Dask collection. This can lead to unexpected behavior."
        )

    if isinstance(x, (list, tuple, memoryview) + np.ScalarType):
        x = np.array(x)

    if is_arraylike(x) and hasattr(x, "copy"):
        x = x.copy()

    # Normalize chunks early to validate and catch errors
    normalized_chunks = normalize_chunks(
        chunks, x.shape, dtype=x.dtype
    )

    # Generate name/token for the expression
    # We tokenize all the user-provided parameters to get a consistent hash
    # Note: we tokenize the original lock (before normalization) for consistency
    if name in (None, True):
        token = tokenize(x, chunks, lock, asarray, fancy, getitem, inline_array)
        final_name = f"array-{token}"
    elif name is False:
        final_name = f"array-{uuid.uuid1()}"
    else:
        final_name = name

    # Normalize lock=True to SerializableLock() for actual use
    if lock is True:
        lock = SerializableLock()

    return new_collection(
        FromArray(
            x,
            normalized_chunks,
            lock=lock,
            asarray=asarray,
            fancy=fancy,
            getitem=getitem,
            meta=meta,
            inline_array=inline_array,
            _name_override=final_name,
        )
    )


def asarray(
    a, allow_unknown_chunksizes=False, dtype=None, order=None, *, like=None, **kwargs
):
    """Convert the input to a dask array.

    Parameters
    ----------
    a : array-like
        Input data, in any form that can be converted to a dask array. This
        includes lists, lists of tuples, tuples, tuples of tuples, tuples of
        lists and ndarrays.
    allow_unknown_chunksizes: bool
        Allow unknown chunksizes, such as come from converting from dask
        dataframes.  Dask.array is unable to verify that chunks line up.  If
        data comes from differently aligned sources then this can cause
        unexpected results.
    dtype : data-type, optional
        By default, the data-type is inferred from the input data.
    order : {'C', 'F', 'A', 'K'}, optional
        Memory layout. 'A' and 'K' depend on the order of input array a.
        'C' row-major (C-style), 'F' column-major (Fortran-style) memory
        representation. 'A' (any) means 'F' if a is Fortran contiguous, 'C'
        otherwise 'K' (keep) preserve input order. Defaults to 'C'.
    like: array-like
        Reference object to allow the creation of Dask arrays with chunks
        that are not NumPy arrays. If an array-like passed in as ``like``
        supports the ``__array_function__`` protocol, the chunk type of the
        resulting array will be defined by it. In this case, it ensures the
        creation of a Dask array compatible with that passed in via this
        argument. If ``like`` is a Dask array, the chunk type of the
        resulting array will be defined by the chunk type of ``like``.
        Requires NumPy 1.20.0 or higher.

    Returns
    -------
    out : dask array
        Dask array interpretation of a.

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> x = np.arange(3)
    >>> da.asarray(x)
    dask.array<array, shape=(3,), dtype=int64, chunksize=(3,), chunktype=numpy.ndarray>

    >>> y = [[1, 2, 3], [4, 5, 6]]
    >>> da.asarray(y)
    dask.array<array, shape=(2, 3), dtype=int64, chunksize=(2, 3), chunktype=numpy.ndarray>

    .. warning::
        `order` is ignored if `a` is an `Array`, has the attribute ``to_dask_array``,
        or is a list or tuple of `Array`'s.
    """
    # Lazy imports to avoid circular dependencies
    from dask.array._array_expr._collection import Array

    if like is None:
        if isinstance(a, Array):
            return _as_dtype(a, dtype)
        elif hasattr(a, "to_dask_array"):
            return _as_dtype(a.to_dask_array(), dtype)
        elif type(a).__module__.split(".")[0] == "xarray" and hasattr(a, "data"):
            return _as_dtype(asarray(a.data, order=order), dtype)
        elif isinstance(a, (list, tuple)) and any(isinstance(i, Array) for i in a):
            # Lazy import to avoid circular dependency
            from dask.array._array_expr.stacking import stack

            return _as_dtype(
                stack(a, allow_unknown_chunksizes=allow_unknown_chunksizes), dtype
            )
        elif not isinstance(getattr(a, "shape", None), Iterable):
            a = np.asarray(a, dtype=dtype, order=order)
    else:
        from functools import partial

        from dask.array.utils import asarray_safe

        like_meta = meta_from_array(like)
        if isinstance(a, Array):
            # Use partial to pass dtype to asarray_safe, not to map_blocks
            # (map_blocks' dtype parameter controls output metadata, not the function call)
            return a.map_blocks(
                partial(asarray_safe, like=like_meta, dtype=dtype, order=order)
            )
        else:
            a = asarray_safe(a, like=like_meta, dtype=dtype, order=order)

    a = from_array(a, getitem=getter_inline, **kwargs)
    return _as_dtype(a, dtype)


def asanyarray(a, dtype=None, order=None, *, like=None, inline_array=False):
    """Convert the input to a dask array.

    Subclasses of ``np.ndarray`` will be passed through as chunks unchanged.

    Parameters
    ----------
    a : array-like
        Input data, in any form that can be converted to a dask array. This
        includes lists, lists of tuples, tuples, tuples of tuples, tuples of
        lists and ndarrays.
    dtype : data-type, optional
        By default, the data-type is inferred from the input data.
    order : {'C', 'F', 'A', 'K'}, optional
        Memory layout. 'A' and 'K' depend on the order of input array a.
        'C' row-major (C-style), 'F' column-major (Fortran-style) memory
        representation. 'A' (any) means 'F' if a is Fortran contiguous, 'C'
        otherwise 'K' (keep) preserve input order. Defaults to 'C'.
    like: array-like
        Reference object to allow the creation of Dask arrays with chunks
        that are not NumPy arrays. If an array-like passed in as ``like``
        supports the ``__array_function__`` protocol, the chunk type of the
        resulting array will be defined by it. In this case, it ensures the
        creation of a Dask array compatible with that passed in via this
        argument. If ``like`` is a Dask array, the chunk type of the
        resulting array will be defined by the chunk type of ``like``.
        Requires NumPy 1.20.0 or higher.
    inline_array:
        Whether to inline the array in the resulting dask graph. For more information,
        see the documentation for ``dask.array.from_array()``.

    Returns
    -------
    out : dask array
        Dask array interpretation of a.

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> x = np.arange(3)
    >>> da.asanyarray(x)
    dask.array<array, shape=(3,), dtype=int64, chunksize=(3,), chunktype=numpy.ndarray>

    >>> y = [[1, 2, 3], [4, 5, 6]]
    >>> da.asanyarray(y)
    dask.array<array, shape=(2, 3), dtype=int64, chunksize=(2, 3), chunktype=numpy.ndarray>

    .. warning::
        `order` is ignored if `a` is an `Array`, has the attribute ``to_dask_array``,
        or is a list or tuple of `Array`'s.
    """
    # Lazy imports to avoid circular dependencies
    from dask.array._array_expr._collection import Array

    if like is None:
        if isinstance(a, Array):
            return _as_dtype(a, dtype)
        elif hasattr(a, "to_dask_array"):
            return _as_dtype(a.to_dask_array(), dtype)
        elif type(a).__module__.split(".")[0] == "xarray" and hasattr(a, "data"):
            return _as_dtype(asarray(a.data, order=order), dtype)
        elif isinstance(a, (list, tuple)) and any(isinstance(i, Array) for i in a):
            # Lazy import to avoid circular dependency
            from dask.array._array_expr.stacking import stack

            return _as_dtype(stack(a), dtype)
        elif not isinstance(getattr(a, "shape", None), Iterable):
            a = np.asanyarray(a, dtype=dtype, order=order)
    else:
        from functools import partial

        from dask.array.utils import asanyarray_safe

        like_meta = meta_from_array(like)
        if isinstance(a, Array):
            # Use partial to pass dtype to asanyarray_safe, not to map_blocks
            # (map_blocks' dtype parameter controls output metadata, not the function call)
            return a.map_blocks(
                partial(asanyarray_safe, like=like_meta, dtype=dtype, order=order)
            )
        else:
            a = asanyarray_safe(a, like=like_meta, dtype=dtype, order=order)

    a = from_array(
        a,
        chunks=a.shape,
        getitem=getter_inline,
        asarray=False,
        inline_array=inline_array,
    )
    return _as_dtype(a, dtype)


def array(x, dtype=None, ndmin=None, *, like=None):
    """Create a dask array from an array-like object.

    See Also
    --------
    numpy.array
    """
    x = asarray(x, like=like)
    while ndmin is not None and x.ndim < ndmin:
        x = x[None, :]
    if dtype is not None and x.dtype != dtype:
        x = x.astype(dtype)
    return x
