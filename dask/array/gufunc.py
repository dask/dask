from __future__ import absolute_import, division, print_function

import numpy as np
from itertools import count
import re

try:
    from cytoolz import concat, merge
except ImportError:
    from toolz import concat, merge

from .core import Array, asarray, atop, flatten, getitem
from .. import sharedict

# Modified version of `numpy.lib.function_base._parse_gufunc_signature`
# Modifications:
#   - Allow for zero input arguments
# See http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
_DIMENSION_NAME = r'\w+'
_CORE_DIMENSION_LIST = '(?:{0:}(?:,{0:})*,?)?'.format(_DIMENSION_NAME)
_ARGUMENT = r'\({}\)'.format(_CORE_DIMENSION_LIST)
_INPUT_ARGUMENTS = '(?:{0:}(?:,{0:})*,?)?'.format(_ARGUMENT)
_OUTPUT_ARGUMENTS = '{0:}(?:,{0:})*'.format(_ARGUMENT) # Use `'{0:}(?:,{0:})*,?'` if gufunc signature should be allowed for length 1 tuple returns
_SIGNATURE = '^{0:}->{1:}$'.format(_INPUT_ARGUMENTS, _OUTPUT_ARGUMENTS)


def _parse_gufunc_signature(signature):
    """
    Parse string signatures for a generalized universal function.

    Arguments
    ---------
    signature : string
        Generalized universal function signature, e.g., ``(m,n),(n,p)->(m,p)``
        for ``np.matmul``.

    Returns
    -------
    Tuple of input and output core dimensions parsed from the signature, each
    of the form List[Tuple[str, ...]], except for one output. For one  output
    core dimension is not a list, but of the form Tuple[str, ...]
    """
    signature = signature.replace(' ', '')
    if not re.match(_SIGNATURE, signature):
        raise ValueError(
            'not a valid gufunc signature: {}'.format(signature))
    in_txt, out_txt = signature.split('->')
    ins = [tuple(re.findall(_DIMENSION_NAME, arg))
           for arg in re.findall(_ARGUMENT, in_txt)]
    outs = [tuple(re.findall(_DIMENSION_NAME, arg))
            for arg in re.findall(_ARGUMENT, out_txt)]
    outs = outs[0] if ((len(outs) == 1) and (out_txt[-1] != ',')) else outs
    return ins, outs


def _compile_gufunc_signature(ins, outs):
    """"
    Compile gufunc signature from nested tuples/lists structures
    of the dimension names.

    Parameters
    ----------
    ins: List[Tuple[str, ...]]
        Each input is a tuple of dimension names
    outs: List[Tuple[str, ...]] or Tuple[str, ...]
        For one output: tuple of dimension names;
        For many outputs: list of tuple of dimension names
    Returns
    -------
    Signature according to the specification of numpy.gufunc signature [2]_

    Examples
    --------
    >>> _compile_gufunc_signature([('i',), ('i', 'j')], ('i', 'j'))
    '(i),(i,j)->(i,j)'
    >>> _compile_gufunc_signature([('i', 'j')], [('i',), ('j',)])
    '(i,j)->(i),(j)'
    """
    in_txt = ",".join(("({0})".format(",".join(dims)) for dims in ins))
    out_txt = ",".join(("({0})".format(",".join(dims)) for dims in
                        (outs if isinstance(outs, list) else [outs])))
    return "{0}->{1}".format(in_txt, out_txt)


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
    *args : numeric
        Input arrays or scalars to the callable function.
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
        core_input_dimss, core_output_dimss = _parse_gufunc_signature(signature)
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
        signature = _compile_gufunc_signature(core_input_dimss, core_output_dimss)
        func = np.vectorize(func, signature=signature)

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
    ### concatenation for coredims that appear also at the output
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


class gufunc(object):
    """
    Binds `pyfunc` into `dask.array.apply_gufunc` when called.

    Parameters
    ----------
    pyfunc : callable
        Function to call like ``func(*args, **kwargs)`` on input arrays
        (``*args``) that returns an array or tuple of arrays. If multiple
        arguments with non-matching dimensions are supplied, this function is
        expected to vectorize (broadcast) over axes of positional arguments in
        the style of NumPy universal functions [1]_ (if this is not the case,
        set ``vectorize=True``). If this function returns multiple outputs,
        ``output_core_dims`` has to be set as well.
    signature : String
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
    Wrapped function

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> a = da.random.normal(size=(10,20,30), chunks=5)
    >>> def stats(x):
    ...     return np.mean(x, axis=-1), np.std(x, axis=-1)
    >>> gustats = da.gufunc(stats, "(i)->(),()")
    ... mean, std = gustats(a)
    ... mean.compute().shape
    (10, 20)

    >>> import dask.array as da
    >>> import numpy as np
    >>> a = da.random.normal(size=(   20,30), chunks=5)
    >>> b = da.random.normal(size=(10, 1,40), chunks=10)
    ... def outer_product(x, y):
    ...     return np.einsum("...i,...j->...ij", x, y)
    >>> gustats = da.gufunc(stats, "(i),(j)->(i,j)")
    >>> c = outer_product(a, b)
    >>> c.compute().shape
    (10, 20, 30, 40)

    References
    ----------
    .. [1] http://docs.scipy.org/doc/numpy/reference/ufuncs.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    def __init__(self, pyfunc, **kwargs):
        self.pyfunc = pyfunc
        self.signature = kwargs.pop("signature", None)
        self.vectorize = kwargs.pop("vectorize", False)
        self.output_sizes = kwargs.pop("output_sizes", None)
        self.output_dtypes = kwargs.pop("output_dtypes", None)
        if kwargs:
            raise TypeError("Unsupported keyword argument(s) provided")

        self.__doc__ = """
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
        """.format(func=str(self.pyfunc), signature=self.signature)

    def __call__(self, *args, **kwargs):
        return apply_gufunc(self.pyfunc,
                            self.signature,
                            *args,
                            vectorize=self.vectorize,
                            output_sizes=self.output_sizes,
                            output_dtypes=self.output_dtypes,
                            **kwargs)


# Decorator, which calls `apply_gufunc` directly
def asgufunc(signature=None, **kwargs):
    """
    Decorator for ``dask.array.gufunc``.

    Parameters
    ----------
    signature : String
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
    Decorator for `pyfunc` that itself returns a `gufunc`.

    Examples
    --------
    >>> import dask.array as da
    >>> import numpy as np
    >>> a = da.random.normal(size=(10,20,30), chunks=5)
    >>> @da.asgufunc("(i)->(),()")
    ... def stats(x):
    ...     return np.mean(x, axis=-1), np.std(x, axis=-1)
    >>> mean, std = stats(a)
    >>> mean.compute().shape
    (10, 20)

    >>> import dask.array as da
    >>> import numpy as np
    >>> a = da.random.normal(size=(   20,30), chunks=5)
    >>> b = da.random.normal(size=(10, 1,40), chunks=10)
    >>> @da.asgufunc("(i),(j)->(i,j)")
    ... def outer_product(x, y):
    ...     return np.einsum("...i,...j->...ij", x, y)
    >>> c = outer_product(a, b)
    >>> c.compute().shape
    (10, 20, 30, 40)

    References
    ----------
    .. [1] http://docs.scipy.org/doc/numpy/reference/ufuncs.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    _allowedkeys = {"vectorize", "output_sizes", "output_dtypes"}
    if set(_allowedkeys).issubset(kwargs.keys()):
        raise TypeError("Unsupported keyword argument(s) provided")
    def _asgufunc(pyfunc):
        return gufunc(pyfunc, signature=signature, **kwargs)
    _asgufunc.__doc__ = """
        Decorator to make ``dask.array.gufunc``
        signature: ``'{signature}'``

        Parameters
        ----------
        pyfunc : callable
            Function matching signature ``'{signature}'``.

        Returns
        -------
        ``dask.array.gufunc``
        """.format(signature=signature)
    return _asgufunc


# # Unfinished trial to create a wrapper for Python functions to
# # mimic a generlized ufunc, i.e. that calls `__array_ufunc__` API.
# # Major problem are recursive calls
# class gufunc(object):
#     def __init__(self, pyfunc, **kwargs):
#         # Convenience to vectorize could take place here already, but then needed to be removed from apply_gufunc
#         # signature = kwargs.pop("signature", None)
#         # vectorize = kwargs.pop("vectorize", False)
#         # self._pyfunc = pyfunc if not vectorize else np_vectorize(pyfunc, signature=signature)
#         # self.signature = signature
#         # instead we do it in `apply_gufunc` now:
#         self.signature = kwargs.pop("signature", None)
#         self.vectorize = kwargs.pop("vectorize", False)
#         self.pyfunc = pyfunc
#
#         self.output_sizes = kwargs.pop("output_sizes", None)
#         self.output_dtypes = kwargs.pop("output_dtypes", None)
#
#     def __call__(self, *inputs, **kwargs):
#         for input in inputs:
#             try:
#                 return input.__array_ufunc__(self,
#                                              '__call__',
#                                              *inputs,
#                                              vectorize=self.vectorize,
#                                              output_sizes=self.output_sizes,
#                                              output_dtypes=self.output_dtypes,
#                                              **kwargs)
#             except AttributeError:
#                pass
#             except NotImplementedError:
#                pass
#
#         return apply_gufunc(self.pyfunc,
#                             self.signature,
#                             *inputs,
#                             vectorize=self.vectorize,
#                             output_sizes=self.output_sizes,
#                             output_dtypes=self.output_dtypes,
#                             **kwargs