from __future__ import absolute_import, division, print_function

import re
from functools import partial
from itertools import count
from operator import itemgetter
try:
    from cytoolz import concat, groupby, valmap, unique, compose
except ImportError:
    from toolz import concat, groupby, valmap, unique, compose

from .core import asarray, asanyarray, broadcast_to


_DIMENSION_NAME = r'\w+'
_DIMENSION_LIST = '(?:{0:}(?:,{0:})*,?)?'.format(_DIMENSION_NAME)
_ARGUMENT = r'\({}\)'.format(_DIMENSION_LIST)
_ARGUMENTS = '(?:{0:}(?:,{0:})*,?)?'.format(_ARGUMENT)
_SIGNATURE = '^{0:}->{1:}$'.format(_ARGUMENTS, _ARGUMENTS)


def _parse_signature(signature):
    """
    Parse string signatures for broadcast arrays.

    Arguments
    ---------
    signature : String

    Returns
    -------
    Tuple of input and output core dimensions parsed from the signature, each
    of the form List[Tuple[str, ...]].
    """
    signature = signature.replace(' ', '')
    signature = signature if '->' in signature else signature + '->'
    if not re.match(_SIGNATURE, signature):
        raise ValueError(
            'not a valid signature: {}'.format(signature))
    in_txt, out_txt = signature.split('->')
    ins = [tuple(re.findall(_DIMENSION_NAME, arg))
           for arg in re.findall(_ARGUMENT, in_txt)]
    outs = [tuple(re.findall(_DIMENSION_NAME, arg))
            for arg in re.findall(_ARGUMENT, out_txt)]
    return ins, outs


def _where(seq, elem):
    """
    Returns first occurrence of ``elem`` in ``seq``
    or ``None``, if ``elem`` is not found
    """
    try:
        return [i for i, e in enumerate(seq) if e == elem][0]
    except IndexError:
        return None


def _inverse(seq):
    """
    Returns the inverse of a sequence of int.
    ``None`` is ignored.

    Examples
    --------
    >>> _inverse([0, 1])
    [0, 1]
    >>> _inverse([2, 1])
    [None, 1, 0]
    >>> _inverse([None, 1, 0])
    [2, 1]
    """
    if not seq:
        return []
    n = max(filter(lambda e: e is not None, seq))
    return [_where(seq, i) for i in range(n + 1)]


def _argsort(list_):
    """
    Simple argsort in pure python
    """
    return sorted(range(len(list_)), key=list_.__getitem__)


def broadcast_arrays(*args, **kwargs):
    """
    Broadcasts arrays against each other.


    Parameters
    ----------
    *args: Arrays
        Arrays to be broadcast against each other.

    signature: Optional; String, Iterable of Iterable, or None
        Specifies loop and core dimensions within each array, where only loop
        dimensions are broadcast. E.g. ``"(i,K),(j)->(K),()"`` is the signature
        for two arrays, where the first array has ``"K"`` as core dimension and
        the two loop dimensions are ``"i"`` and ``"j"``. Specification of core
        dimensions could also be omitted, e.g. ``"(i),(j)"``. Then only loop
        dimensions could be specified and the signature is left aligned,
        meaning trailing array dimensions are considered mutual unique core
        dimensions.

        Either side of signature could be omitted, e.g. ``"->(K),(K)"``
        specifies to broadcast all but the last dimension automatically
        against each other according to numpy broadcasting rules [1]_.

        If required, the arrays are also transposed to achieve the right
        output order dimensions.

        Dimension sizes for same loop dimensions must match or be of length
        ``1``. Dimension sizes of same core dimensions must match.

        Chunk sizes for same loop or core dimensions must be same,
        except where a loop dimension is sparse and of length ``1``.

        Defaults to ``None``.

        Note: while the syntax is similar to numpy generalized
        ufuncs [2]_, its meaning here is different.

    sparse: Optional; Bool
        Specifies if a broadcast should be sparse, i.e. new broadcast
        dimensions are of length 1. If not sparse existing sparse
        dimensions are broadcast to their full size (as stated by
        the other passed arguments). Defaults to ``False``.

    subok: Bool
        If True, then sub-classes will be passed-through, otherwise
        the returned array will be forced to be a base-class array
        (default).

    Returns
    -------
     : Arrays
        Broadcast arrays


    Examples
    --------
    >>> import numpy as np
    >>> a = np.random.randn(3, 1)
    >>> b = np.random.randn(1, 4)
    >>> A, B = broadcast_arrays(a, b)
    >>> A.shape, B.shape
    ((3, 4), (3, 4))

    Broadcast two scalars with distinct loop dims against each other
    Equivalent signatures:
        - ``"(i),(j)"``
        - ``"(i),(j)->(),()"``
        - ``"(i),(j)->(i,j),(i,j)"``
    >>> a = np.random.randn(3)
    >>> b = np.random.randn(4)
    >>> A, B = broadcast_arrays(a, b, signature="(i),(j)")
    >>> A.shape, B.shape
    ((3, 4), (3, 4))

    Broadcast two vectors with distinct loop dims against each other
    Equivalent signatures:
        - ``"(i),(j)"``
        - ``"(i),(j)->(U),(V)"`` (advanced usage, not recommended)
        - ``"(i,U),(j,V)->(U),(V)"``
        - ``"(i,U),(j,V)->(i,j,U),(i,j,V)"``
    >>> a = np.random.randn(3, 5)
    >>> b = np.random.randn(4, 6)
    >>> A, B = broadcast_arrays(a, b, signature="(i),(j)")
    >>> A.shape, B.shape
    ((3, 4, 5), (3, 4, 6))

    Broadcast two scalars with same loop dim against each other
    Equivalent signatures:
        - ``None``
        - ``"(i),(i)"``
        - ``"->(),()"``
        - ``"(i),(i)->(),()"``
        - ``"(i),(i)->(i),(i)"``
    >>> a = np.random.randn(3)
    >>> b = np.random.randn(3)
    >>> A, B = broadcast_arrays(a, b, signature="(i),(i)")
    >>> A.shape, B.shape
    ((3,), (3,))

    Broadcast two vectors each with same loop dim against each other
    Equivalent signatures:
        - ``"(i),(i)",``
        - ``"->(U),(V)"``
        - ``"(i),(i)->(U),(V)"`` (advanced usage, not recommended)
        - ``"(i,U),(i,V)->(U),(V)"``
        - ``"(i,U),(i,V)->(i,U),(i,V)"``
    >>> a = np.random.randn(3, 5)
    >>> b = np.random.randn(3, 6)
    >>> A, B = broadcast_arrays(a, b, signature="(i),(i)")
    >>> A.shape, B.shape
    ((3, 5), (3, 6))

    Broadcast two vectors each with partially same loop dim against each other no proposed loop dim order
    Equivalent signatures:
        - ``"(j),(i,j)"``
        - ``"->(U),(V)"``
        - ``"(j),(i,j)->(U),(V)"`` (advanced usage, not recommended)
        - ``"(j,U),(i,j,V)->(U),(V)"``
        - ``"(j,U),(i,j,V)->(i,j,U),(i,j,V)"``
    >>> a = np.random.randn(4, 5)
    >>> b = np.random.randn(3, 4, 6)
    >>> A, B = broadcast_arrays(a, b, signature="(j),(i,j)")
    >>> A.shape, B.shape
    ((3, 4, 5), (3, 4, 6))

    Broadcast two vectors each with partially same loop dim against each other no proposed loop dim order
    Equivalent signatures:
        - ``"(i),(i,j)"``
        - ``"(i),(i,j)->(U),(V)"`` (advanced usage, not recommended)
        - ``"(i,U),(i,j,V)->(U),(V)"``
        - ``"(i,U),(i,j,V)->(i,j,U),(i,j,V)"``
    >>> a = np.random.randn(3, 5)
    >>> b = np.random.randn(3, 4, 6)
    >>> A, B = broadcast_arrays(a, b, signature="(i),(i,j)")
    >>> A.shape, B.shape
    ((3, 4, 5), (3, 4, 6))

    Broadcast many core and loop dimensions against each other that are passed in random order
    and also set ``sparse=True``
    >>> import dask.array as da
    >>> x = da.random.normal(size=(20, 30, 3), chunks=(5, 6, 3))
    >>> y = da.random.normal(size=(3, 20, 2), chunks=(3, 5, 2))
    >>> X, Y = broadcast_arrays(x, y, signature="(i,j,U),(U,i,V)->(U),(V,U)", sparse=True)
    >>> X.shape, Y.shape
    ((20, 30, 3), (20, 1, 2, 3))


    References
    ----------
    .. [1] https://docs.scipy.org/doc/numpy-1.14.0/user/basics.broadcasting.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    signature = kwargs.pop('signature', None)
    sparse = kwargs.pop('sparse', False)
    subok = bool(kwargs.pop('subok', False))
    if kwargs:
        raise TypeError("Unsupported keyword argument(s) provided")

    # Input processing
    to_array = asanyarray if subok else asarray
    args = tuple(to_array(e) for e in args)
    nargs = len(args)
    shapes = [e.shape for e in args]
    chunkss = [tuple(i[0] for i in e.chunks) for e in args]
    ndimss = [len(s) for s in shapes]

    # Parse signature
    if isinstance(signature, str):
        lhs_dimss, rhs_dimss = _parse_signature(signature)
    elif isinstance(signature, tuple):
        lhs_dimss, rhs_dimss = signature
    elif isinstance(signature, list):
        lhs_dimss = signature
        rhs_dimss = []
    elif signature is None:
        # Inject default behavior for signature
        lhs_dimss = [tuple(reversed(range(n))) for n in ndimss]
        rhs_dimss = []
    else:
        raise ValueError("``signature`` is invalid")
    rhs_dimss = rhs_dimss if rhs_dimss else nargs * [tuple()]
    lhs_dimss = lhs_dimss if lhs_dimss else [tuple(reversed(range(n - len(rhs_dims))))
                                             for n, rhs_dims in zip(ndimss, rhs_dimss)]

    # Check consistency of passed arguments
    if len(lhs_dimss) != len(args):
        raise ValueError("``signature`` does not match number of input arrays")
    if len(rhs_dimss) != len(args):
        raise ValueError("``signature`` does not match number of input arrays on right hand side")

    # Check consistency of passed dimensions
    for idx, ndims, lhs_dims, rhs_dims in zip(count(1), ndimss, lhs_dimss, rhs_dimss):
        if (len(set(lhs_dims)) != len(lhs_dims)) or \
           (len(set(rhs_dims)) != len(rhs_dims)):
            raise ValueError("Repeated dimension name for array #{} in signature".format(idx))
        if len(lhs_dims) > ndims:
            raise ValueError("Too many dimension(s) for input array #{} in signature given".format(idx))

    # Construct complete dimension names of passed arrays
    in_dimss = []
    auto_core_dimss = []
    for idx, ndims, lhs_dims, rhs_dims in zip(count(), ndimss, lhs_dimss, rhs_dimss):
        in_dims = list(lhs_dims)
        auto_core_dims = tuple()
        ndiff = ndims - len(in_dims)
        assert ndiff >= 0  # Should not occur
        if ndiff == 0:
            # Mode 1) LHS provided all dimension names
            pass
        elif len(rhs_dims) == 0:
            # Mode 2) No RHS, we automatically create dimension names for them and consider them core dims, i.e.
            # unique from dimensions at same positions in other passed arrays
            auto_core_dims = tuple('__broadcast_arrays_coredim_{}_{}'.format(idx, j) for j in range(ndiff))
            in_dims.extend(auto_core_dims)
        else:
            # Mode 3) We can attach the missing dim names from RHS if they won't lead to repeated
            # dimensions names
            in_dims.extend(rhs_dims[-ndiff:])
            if len(in_dims) != ndims:
                raise ValueError("Signature for array #{} cannot be created easily (clarify dimensions)".format(idx))
        in_dimss.append(in_dims)
        auto_core_dimss.append(auto_core_dims)
        # Check consistency of in_dimss construction
        if len(set(in_dims)) != len(in_dims):
            raise ValueError("Repeated dimension name for array #{} in signature".format(idx))

    # Check that the arrays have same length for same dimensions or dimension `1`
    _temp = groupby(0, concat(zip(ad, s) for ad, s in zip(in_dimss, shapes)))
    dimsizess = valmap(compose(set, partial(map, itemgetter(1))), _temp)
    for dim, sizes in dimsizess.items():
        if sizes.union({1}) != {1, max(sizes)}:
            raise ValueError("Dimension ``{}`` with different lengths in arrays".format(dim))
    dimsizes = valmap(max, dimsizess)

    # Check if arrays have same chunk size for the same dimension
    _temp = groupby(0, concat(zip(ad, s, c) for ad, s, c in zip(lhs_dimss, shapes, chunkss)))
    dimchunksizess = valmap(compose(set,
                                    partial(map, itemgetter(1)),
                                    partial(filter, lambda e: e != (1, 1)),
                                    partial(map, lambda tpl: tpl[1:])),
                            _temp)
    for dim, dimchunksizes in dimchunksizess.items():
        if len(dimchunksizes) > 1:
            raise ValueError('Dimension ``{}`` with different chunksize present'.format(dim))
    dimchunksizes = valmap(max, dimchunksizess)

    # Find loop dims and union of all loop dims in order of appearance
    auto_loop_dimss = [tuple(i for i in id_ if i not in rd)
                       for id_, rd in zip(lhs_dimss, rhs_dimss)]

    # According to https://docs.scipy.org/doc/numpy-1.14.0/user/basics.broadcasting.html automatic broadcasting
    # is right aligned. Therefore to determine the correct order of dim names for auto broadcasting, we sort the
    # arrays from many loop dims to few loop dims and then then order of total loop dims will be in order of
    # their appearance
    _auto_loop_dimss_sorted = sorted(auto_loop_dimss, key=len, reverse=True)
    auto_total_loop_dims = tuple(unique(concat(_auto_loop_dimss_sorted)))

    out_dimss = [auto_total_loop_dims + rd + acd
                 for id_, rd, acd in zip(in_dimss, rhs_dimss, auto_core_dimss)]

    # Find order of transposition for each array and perform transformations
    new_args = []
    for arg, out_dims, in_dims, shape, chunks in zip(args, out_dimss, in_dimss, shapes, chunkss):

        # Find new position of given dimension and maybe indicate dimensions which have to be created
        in2out_poss = []
        for idx, out_dim in enumerate(out_dims):
            oidx = _where(in_dims, out_dim)
            if oidx is None:
                oidx = -idx - 1
            in2out_poss.append(oidx)  # Insert new dim if not present
        # Determine order for later transposition
        _temp = _argsort(in2out_poss)
        transpose_idcs = _inverse(_temp)
        sorted_in2out_poss = sorted(in2out_poss)

        # Determine the new shape size by pre-pending newly created dimensions
        if sparse is True:
            new_shape = tuple(1 for i in in2out_poss if i < 0) + shape
        else:
            new_shape = tuple(dimsizes[out_dims[-i - 1]] for i in sorted_in2out_poss if i < 0) \
                + tuple(dimsizes[in_dims[i]] for i in sorted_in2out_poss if i >= 0)  # Extend size `1`

        # Chunks can be original size, in case of `sparse=True` it will be cut back to `1` by new_shape
        new_chunks = tuple(dimchunksizes[out_dims[-i - 1]] for i in sorted_in2out_poss if i < 0) + chunks

        # Apply old `dask.array.broadcast_to` and `transpose`
        new_arg = broadcast_to(arg, shape=new_shape, chunks=new_chunks)
        new_arg = new_arg.transpose(transpose_idcs)

        new_args.append(new_arg)

    return new_args
