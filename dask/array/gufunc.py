from __future__ import absolute_import, division, print_function

from itertools import count
from operator import getitem
import re
from numpy import vectorize as np_vectorize

try:
    from cytoolz import concat, merge, curry
except ImportError:
    from toolz import concat, merge, curry
from functools import partial

from .core import (asarray, Array, atop)
from ..core import flatten
from .. import sharedict


_valid_name_re = re.compile("^[_a-zA-Z][_a-zA-Z0-9]*$")


def _parse_dim(dim):
    if not dim:
        return None
    assert len(_valid_name_re.findall(dim)) == 1
    return dim


def _parse_arg(sig):
    if not sig:
        return None
    assert len(re.findall("^\([_a-zA-Z0-9,]*\)$", sig)) == 1
    sig = sig[1:-1]
    dims = sig.split(',')
    ret = [_parse_dim(d) for d in dims]
    return tuple(r for r in ret if r is not None)


def _parse_args(sig):
    if not sig:
        return None
    assert len(re.findall("^(\([_a-zA-Z0-9,]*\)),?(,?\([_a-zA-Z0-9,]*\))*$", sig)) == 1
    sigs = re.findall("\([_a-zA-Z0-9,]*\)*", sig)
    ret = [_parse_arg(s) for s in sigs]
    return list(r for r in ret if r is not None)


def parse_signature(signature):
    signature = signature.replace(' ', '')
    assert "->" in signature
    _in, _out = signature.split("->")
    _ins = _parse_args(_in)
    if _ins is None:
        _ins = []
    _ins = [i for i in _ins if i is not None]
    _outs = _parse_args(_out)
    if _outs and (len(_outs) == 1) and ('),' not in _out):
        _outs = _outs[0]
    return _ins, _outs


def compile_signature(ins, outs):
    for e in concat(ins):
        if len(_valid_name_re.findall(e)) != 1:
            raise ValueError('`"{0}"` is not a valid name.'.format(e))
    ins = ",".join(("({0})".format(",".join(dims)) for dims in ins))

    if outs is None:
        outs = ""
    elif isinstance(outs, list):
        for e in concat(outs):
            if len(_valid_name_re.findall(e)) != 1:
                raise ValueError('`"{0}"` is not a valid name.'.format(e))
        _flag = len(outs) == 1
        outs = ",".join(("({0})".format(",".join(dims)) for dims in outs))
        if _flag:
            outs += ","
    else:
        for e in outs:
            if len(_valid_name_re.findall(e)) != 1:
                raise ValueError('`"{0}"` is not a valid name.'.format(e))
        outs = "({0})".format(",".join(outs))
    return "{0}->{1}".format(ins, outs)


def apply_gufunc(func, signature, *args, **kwargs):
    """
    Apply a generalized ufunc [2]_ to arrays. The function is
    mapped to the input arguments. The ``signature`` determines
    if the function consumes or produces core dimensions. The
    remaining dimensions in given input arrays (``*args``) are
    considered loop dimensions and are required to broadcast
    naturally against each other.

    In other terms, this function is like np.vectorize, but for
    the blocks of dask arrays. If the function itself shall also
    be vectorized, ``vectorize=True`` can be used for convenience..

    Parameters
    ----------
    func : callable
        Function to call like ``func(*args, **kwargs)`` on input arrays
        (``*args``) that returns an array or tuple of arrays. If multiple
        arguments with non-matching dimensions are supplied, this function is
        expected to vectorize (broadcast) over axes of positional arguments in
        the style of NumPy universal functions [1]_ (if this is not the case,
        set ``vectorize=True``). If this function returns multiple outputs,
        ``output_core_dims`` has to be set as well.
    signature: String
        Specifies what core dimensions are consumed and produced by ``func``.
        According to the specification of numpy.gufunc signature [2]_
    output_dtypes : dtype or list of dtypes
        dtype or list of output dtypes.
    output_sizes : dict, optional
        Optional mapping from dimension names to sizes for outputs. Only used if
        new core dimensions (not found on inputs) appear on outputs.
    vectorize: bool
        If set to ``True``, ``np.vectorize`` is applied to ``func`` for
        convenience. Defaults to ``False``.

    Returns
    -------
    Single dask.array.Array or tuple of dask.array.Array

    Examples
    --------
    >>> import dask.array as da
    ... import numpy as np
    ... def stats(x):
    ...     return np.mean(x, axis=-1), np.std(x, axis=-1)
    ... a = da.random.normal(size=(10,20,30), chunks=5)
    ... mean, std = da.apply_gufunc(stats, "(i)->(),()", a, output_dtypes=2*(a.dtype,))
    ... mean.compute().shape
    (10, 20)

    >>> import dask.array as da
    ... import numpy as np
    ... def outer_product(x, y):
    ...     return np.einsum("...i,...j->...ij", x, y)
    ... a = da.random.normal(size=(   20,30), chunks=5)
    ... b = da.random.normal(size=(10, 1,40), chunks=10)
    ... c = da.apply_gufunc(outer_product, "(i),(j)->(i,j)", a, b, output_dtypes=a.dtype)
    ... c.compute().shape
    (10, 20, 30, 40)

    References
    ----------
    .. [1] http://docs.scipy.org/doc/numpy/reference/ufuncs.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    output_dtypes = kwargs.pop("output_dtypes", None)
    output_sizes = kwargs.pop("output_sizes", None)
    vectorize = kwargs.pop("vectorize", None)
    concatenate = kwargs.pop("concatenate", True)
    if output_dtypes is None:
        raise ValueError("Must specify `output_dtypes` of output array(s)")

    # Input processing:
    ## Signature
    if isinstance(signature, str):
        core_input_dimss, core_output_dimss = parse_signature(signature)
    else:
        core_input_dimss, core_output_dimss = signature

    ## Determine nout
    nout = None if not isinstance(core_output_dimss, list) else len(core_output_dimss)

    ## Assert output_dtypes
    if nout is not None and not (isinstance(output_dtypes, tuple) or isinstance(output_dtypes, list)):
        raise ValueError("Must specify tuple of dtypes for `output_dtypes` for function with multiple outputs")
    if nout is None and (isinstance(output_dtypes, tuple) or isinstance(output_dtypes, list)):
        raise ValueError("Must specify single dtype for `output_dtypes` for function with one output")

    ## Use top to apply func
    if vectorize:
        signature = compile_signature(core_input_dimss, core_output_dimss)
        func = np_vectorize(func, signature=signature)

    ## Miscellaneous
    if output_sizes is None:
        output_sizes = {}

    # Main code:
    ## Cast all input arrays to dask
    args = [asarray(a) for a in args]

    if len(core_input_dimss) != len(args):
        ValueError("According to `signature`, `func` requires %d arguments, but %s given" % (len(core_output_dimss), len(args)))

    ## Assess input args for loop dims
    input_shapes = [a.shape for a in args]
    num_loopdims = [len(s) - len(cd) for s, cd in zip(input_shapes, core_input_dimss)]
    max_loopdims = max(num_loopdims) if num_loopdims else None
    _core_input_shapes = [dict(zip(cid, s[n:])) for s, n, cid in zip(input_shapes, num_loopdims, core_input_dimss)]
    core_shapes = merge(output_sizes, *_core_input_shapes)

    loop_input_dimss = [tuple("__loopdim%d__"%d for d in range(max_loopdims-n, max_loopdims)) for n in num_loopdims]

    input_dimss = [l+c for l, c in zip(loop_input_dimss, core_input_dimss)]

    loop_output_dims = max(loop_input_dimss, key=len) if loop_input_dimss else set()

    ## Apply function - use atop here
    arginds = list(concat(zip(args, input_dimss)))

    ### Treat direct output
    if nout is None:
        core_output_dimss = [core_output_dimss]
        output_dtypes = [output_dtypes]

    ### Use existing `atop` but only with loopdims to enforce
    ### concatenation, for coredims, that appear also at the output
    ### Modifying `atop` could improve things here.
    tmp = atop(func, loop_output_dims, *arginds,
               dtype=int,  # Only dummy dtype, anyone will do
               concatenate=concatenate,
               **kwargs)

    ## Prepare output shapes
    loop_output_shape = tmp.shape
    loop_output_chunks = tmp.chunks
    dsk = tmp.__dask_graph__()
    keys = list(flatten(tmp.__dask_keys__()))
    _anykey = keys[0]
    name, token = _anykey[0].split('-')

    ## Split output
    leaf_arrs = []
    for i, cod, odt in zip(count(0), core_output_dimss, output_dtypes):
        core_output_shape = tuple(core_shapes[d] for d in cod)
        core_chunkinds = len(cod)*(0,)
        output_shape = loop_output_shape + core_output_shape
        output_chunks = loop_output_chunks + core_output_shape
        leaf_name = "%s_%d-%s" % (name, i, token)
        leaf_dsk = {(leaf_name,) + key[1:] + core_chunkinds: ((getitem, key, i) if nout else key) for key in keys}
        leaf_arr = Array(sharedict.merge((leaf_name, leaf_dsk), dsk),
                         leaf_name,
                         chunks=output_chunks,
                         shape=output_shape,
                         dtype=odt)
        leaf_arrs.append(leaf_arr)

    return leaf_arrs if nout else leaf_arrs[0]


@curry
def gufunc(signature, func, **kwargs):
    """
    Apply a generalized ufunc [2]_ to arrays. The ``signature``
    determines if the function consumes or produces core dimensions.
    This method returns a wrapped function, which then can be bound
    to arguments.

    In other terms, this function is like np.vectorize, but for
    the blocks of dask arrays. If the function itself shall also
    be vectorized, ``vectorize=True`` can be used for convenience..

    Parameters
    ----------
    signature: String
        Specifies what core dimensions are consumed and produced by ``func``.
        According to the specification of numpy.gufunc signature [2]_
    func : callable
        Function to call like ``func(*args, **kwargs)`` on input arrays
        (``*args``) that returns an array or tuple of arrays. If multiple
        arguments with non-matching dimensions are supplied, this function is
        expected to vectorize (broadcast) over axes of positional arguments in
        the style of NumPy universal functions [1]_ (if this is not the case,
        set ``vectorize=True``). If this function returns multiple outputs,
        ``output_core_dims`` has to be set as well.
    *args : numpy/dask arrays or scalars
        Arrays to which to apply the function. Core dimensions as specified in
        ``signature`` need to come last.
    output_dtypes : dtype or list of dtypes
        dtype or list of output dtypes.
    output_sizes : dict, optional
        Optional mapping from dimension names to sizes for outputs. Only used if
        new core dimensions (not found on inputs) appear on outputs.
    vectorize: bool
        If set to ``True``, ``np.vectorize`` is applied to ``func`` for
        convenience. Defaults to ``False``.

    Returns
    -------
    Wrapped function

    Examples
    --------
    >>> import dask.array as da
    ... import numpy as np
    ... @da.gufunc("(i)->(),()", output_dtypes=2*(a.dtype,))
    ... def stats(x):
    ...     return np.mean(x, axis=-1), np.std(x, axis=-1)
    ... a = da.random.normal(size=(10,20,30), chunks=5)
    ... mean, std = stats(a)
    ... mean.compute().shape
    (10, 20)

    >>> import dask.array as da
    ... import numpy as np
    ... @da.gufunc("(i),(j)->(i,j)", output_dtypes=a.dtype)
    ... def outer_product(x, y):
    ...     return np.einsum("...i,...j->...ij", x, y)
    ... a = da.random.normal(size=(   20,30), chunks=5)
    ... b = da.random.normal(size=(10, 1,40), chunks=10)
    ... c = outer_product(a, b)
    ... c.compute().shape
    (10, 20, 30, 40)

    References
    ----------
    .. [1] http://docs.scipy.org/doc/numpy/reference/ufuncs.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    _gufunc = partial(apply_gufunc, func, signature, **kwargs)
    _gufunc.__doc__ = """
        Bound ``dask.array.gufunc``
        func: {func}
        signature: '{signature}'

        Parameters
        ----------
        *args : numpy/dask arrays or scalars
            Arrays to which to apply the function. Core dimensions as specified in
            ``signature`` need to come last.

        Returns
        -------
        Single dask.array.Array or tuple of dask.array.Array
        """.format(func=str(func), signature=signature)

    return _gufunc
