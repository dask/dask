from __future__ import absolute_import, division, print_function

from functools import partial
from itertools import count
from operator import itemgetter, getitem
import re
from numpy import vectorize as np_vectorize

try:
    from cytoolz import unique, concat, groupby, valmap, compose, partition
except ImportError:
    from toolz import unique, concat, groupby, valmap, compose, partition

from .core import (asarray, top, Array,
                   broadcast_shapes, broadcast_chunks,
                   broadcast_dimensions)
from .rechunk import rechunk
from ..base import tokenize
from ..utils import funcname
from .. import sharedict


_valid_name_re = re.compile("^[_a-zA-Z][_a-zA-Z0-9]*$")


def _parse_dim(dim):
    if not dim:
        return None
    #assert len(re.findall("^[_a-zA-Z][_a-zA-Z0-9]*$", dim)) == 1
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


def _pprint(**kwargs):
    txt = ("%s: %s"% (k, v) for k, v in kwargs.items())
    print("\n".join(txt))


def apply_gufunc(func, signature, *args, **kwargs):
    """
    Apply a generalized ufunc [2]_ to arrays. The function is
    mapped to the input arguments. The ``signature`` determines
    if the function consumes or produces core dimensions. The
    remainng dimensions in given input arrays (``*args``) are
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
    Single dask.array.Array or tuple of dask.array.Array

    Examples
    --------
    >>> import dask.array as da
    ... import numpy as np
    ... def stats(x):
    ...     return np.mean(x, axis=-1), np.std(x, axis=-1)
    ... a = da.random.normal(size=(10,20,30), chunks=5)
    ... mean, std = apply_gufunc(stats, "(i)->(),()", a, output_dtypes=2*(a.dtype,))
    ... mean.compute().shape
    (10, 20)

    >>> import dask.array as da
    ... import numpy as np
    ... def outer_product(x, y):
    ...     return np.einsum("...i,...j->...ij", x, y)
    ... a = da.random.normal(size=(   20,30), chunks=5)
    ... b = da.random.normal(size=(10, 1,40), chunks=10)
    ... c = apply_gufunc(outer_product, "(i),(j)->(i,j)", a, b, output_dtypes=a.dtype)
    ... c.compute().shape
    (10, 20, 30, 40)

    References
    ----------
    .. [1] http://docs.scipy.org/doc/numpy/reference/ufuncs.html
    .. [2] http://docs.scipy.org/doc/numpy/reference/c-api.generalized-ufuncs.html
    """
    output_dtypes = kwargs.pop("output_dtypes", None)
    output_sizes = kwargs.pop("output_sizes", None)
    out = kwargs.pop("out", None)
    token = kwargs.pop("token", None)
    vectorize = kwargs.pop("vectorize", None)
    if output_dtypes is None:
        raise ValueError("Must specify `output_dtypes` of output array(s)")

    # Input processing:
    ## Signature
    if isinstance(signature, str):
        core_input_dims, core_output_dims = parse_signature(signature)
        signature = compile_signature(core_input_dims, core_output_dims)
    else:
        core_input_dims, core_output_dims = signature
        signature = compile_signature(core_input_dims, core_output_dims)
    _pprint(signature=signature)

    ## Determine nout
    nout = None if not isinstance(core_output_dims, list) else len(core_output_dims)

    ## Assert output_dtypes
    if nout is not None and not (isinstance(output_dtypes, tuple) or isinstance(output_dtypes, list)):
        raise ValueError("Must specify tuple of dtypes for `output_dtypes` for function with multiple outputs")
    if nout is None and (isinstance(output_dtypes, tuple) or isinstance(output_dtypes, list)):
        raise ValueError("Must specify single dtype for `output_dtypes` for function with one output")

    _pprint(core_input_dims=core_input_dims,
            core_output_dims=core_output_dims,
            nout=nout)

    ## Use top to apply func
    if vectorize:
        func = np_vectorize(func, signature=signature)

    ## Cast all input arrays to dask
    args = [asarray(a) for a in args]
    _pprint(args=args)

    ## Miscellaneous
    if output_sizes is None:
        output_sizes = {}

    # Main code:
    all_core_input_dims = tuple(unique(concat(core_input_dims)))
    all_core_output_dims = tuple(unique(concat(core_output_dims) if nout else core_output_dims))
    all_core_dims = tuple(unique(concat((all_core_input_dims, all_core_output_dims))))
    _pprint(all_core_input_dims=all_core_input_dims,
            all_core_output_dims=all_core_output_dims,
            all_core_dims=all_core_dims)

    ## Get dims and chunks of all input arguments
    shapes = [a.shape for a in args]
    chunkss = [list(c[0] for c in a.chunks) for a in args]
    chunkss2 = [a.chunks for a in args]
    _pprint(shapes=shapes, chunkss=chunkss)

    shapes_coredims = []
    shapes_loopdims = []
    chunkss_coredims = []
    chunkss_loopdims = []
    chunkss2_loopdims = []
    for s, c, c2, cid in zip(shapes, chunkss, chunkss2, core_input_dims):
        n_cid = len(cid)
        shape_coredims = s[-n_cid:] if n_cid > 0 else tuple()
        shape_loopdims = s[:-n_cid] if n_cid > 0 else s
        chunks_coredims = c[-n_cid:] if n_cid > 0 else tuple()
        chunks_loopdims = c[:-n_cid] if n_cid > 0 else c
        chunks2_loopdims = c2[:-n_cid] if n_cid > 0 else c2

        shapes_coredims.append(dict(zip(cid, shape_coredims)))
        chunkss_coredims.append(shape_coredims)  # Used later for new chunksize in coredims
        shapes_loopdims.append(shape_loopdims)
        chunkss_loopdims.append(chunks_loopdims)
        chunkss2_loopdims.append(chunks2_loopdims)
    _pprint(shapes_coredims=shapes_coredims,
            chunkss_coredims=chunkss_coredims,
            shapes_loopdims=shapes_loopdims,
            chunkss_loopdims=chunkss_loopdims,
            chunkss2_loopdims=chunkss2_loopdims)

    ## Assert same len of same coredims
    coredim_sizes = valmap(compose(tuple, partial(map, itemgetter(1))),
                           groupby(itemgetter(0), concat(d.items() for d in shapes_coredims)))
    _pprint(coredim_sizes=coredim_sizes)
    for k, v in coredim_sizes.items():
        assert len(set(v)) == 1
    coredim_sizes = {k: v[0] for k, v in coredim_sizes.items()}
    _pprint(coredim_sizes=coredim_sizes)
    coredim_sizes.update(output_sizes)
    _pprint(coredim_sizes=coredim_sizes)

    ## Rechunk
    ### Figure out proper chunksize for loopdims
    num_loopdims = list(map(len, shapes_loopdims))
    max_num_loopdims = max(num_loopdims)
    _pprint(num_loopdims=num_loopdims, max_num_loopdims=max_num_loopdims)

    f = zip(*map(concat, zip(map(reversed, shapes_loopdims), ((max_num_loopdims - i) * (None,) for i in num_loopdims))))
    g = zip(
        *map(concat, zip(map(reversed, chunkss_loopdims), ((max_num_loopdims - i) * (None,) for i in num_loopdims))))
    #_pprint(f=list(f), g=list(g))

    new_chunkss = []
    for dimsizes, chunksizes in zip(f, g):
        _pprint(dimsizes=dimsizes, chunksizes=chunksizes)
        ## Assert same len (or 1) for loopdims
        s = tuple(d for d in dimsizes if d is not None)
        assert {max(s), 1} == set(s + (1,))

        _chunksizes = tuple(c for c, d in zip(chunksizes, dimsizes) if d is not None and d > 1)
        _pprint(_chunksizes=_chunksizes)
        new_chunk = min(_chunksizes)
        _pprint(new_chunk=new_chunk)
        new_chunks = tuple(new_chunk if c else None for c in chunksizes)
        new_chunkss.append(new_chunks)

    _pprint(new_chunkss=new_chunkss)
    new_chunkss = list(map(compose(tuple, partial(filter, lambda x: x is not None), reversed), zip(*new_chunkss)))
    _pprint(new_chunkss=new_chunkss)

    ### Append coredims to one chunk
    new_chunkss = [loop + core for loop, core in zip(new_chunkss, chunkss_coredims)]
    _pprint(new_chunkss=new_chunkss)

    ### Finally do the actual rechunk
    args = [rechunk(arg, chunks=new_chunks) for arg, new_chunks in zip(args, new_chunkss)]
    _pprint(args=args)

    ## Apply function
    ### collect new numblocks
    numblockss = {a.name: a.numblocks for a in args}
    _pprint(numblockss=numblockss)
    _pprint(set_numblockss=set(numblockss))

    ### Find out final shape of loopdims
    shape_loopdims = broadcast_shapes(*shapes_loopdims)
    _pprint(shape_loopdims=shape_loopdims)

    ### Create some dimension names for top
    #### Output loop dims
    make_loopdims = compose(tuple, partial(map, lambda i: "__loopdim{0}__".format(i)), reversed, range, len)
    outinds = make_loopdims(shape_loopdims)
    _pprint(outinds=outinds)

    #### Input loop dims + individual core dims
    ininds = list(map(make_loopdims, shapes_loopdims))
    ininds = [l + c for l, c in zip(ininds, core_input_dims)]
    _pprint(ininds=ininds)

    #### Names for inputs
    names = [a.name for a in args]
    _pprint(names=names)

    argindsstr = list(concat(zip(names, ininds)))
    _pprint(argindsstr=argindsstr)

    if not out:
        token = token or funcname(func).strip('_')
        _hash = tokenize(func, outinds, argindsstr, **kwargs)
        out = '_%s-%s' % (token, _hash)
    _pprint(out=out)

    #### Finally create new dask with top
    dsk = top(func, out, outinds, *argindsstr, numblocks=numblockss, concatenate=True)
    dsks = [dict(a.dask) for a in args]
    _pprint(dsk=dsk) #, dsks=dsks)

    arginds = list(partition(2, argindsstr))
    _pprint(arginds=arginds)
    outchunkss = broadcast_dimensions(arginds, numblockss)  # , consolidate=da.core.common_blockdim)
    _pprint(outchunkss=outchunkss)
    outnumblocks = {out: tuple(outchunkss[k] for k in outinds)}
    _pprint(outnumblocks=outnumblocks)

    outchunks_loopdims = broadcast_chunks(*chunkss2_loopdims)
    outshape_loopdims = broadcast_shapes(*shapes_loopdims)
    _pprint(outchunks_loopdims=outchunks_loopdims,
            outshape_loopdims=outshape_loopdims)

    ## Now only output to be split, if applicable. Also add new core dimensions, if there are
    # #(also passed through core dimensions should work)
    if nout is None:
        hash_ = tokenize(_hash, core_output_dims, output_dtypes)
        outs = ['%s-%s' % (token, hash_)]
        core_output_dims = [core_output_dims]
        output_dtypes = [output_dtypes]
    else:
        outs = ['%s_%d-%s' % (token,
                              i,
                              tokenize(_hash, cod, od))
                for i, cod, od in zip(count(0), core_output_dims, output_dtypes)]
    _pprint(outs=outs)

    ##Now ... for each output attach new dimensions, run top and create output array
    odsks = []
    oarrs = []
    for i, _out, core_output_dim, dtype in zip(count(0), outs, core_output_dims, output_dtypes):
        _outinds = outinds + core_output_dim
        coredim_shape = tuple(coredim_sizes[k] for k in core_output_dim)
        new_axes = dict(zip(core_output_dim, coredim_shape))
        odsk = top(itemgetter(i), _out, _outinds, out, outinds,
                   numblocks=outnumblocks,
                   new_axes=new_axes,
                   concatenate=True)
        if nout is None:
            odsk = valmap(itemgetter(1), odsk)
        odsks.append(odsk)
        chunks = outchunks_loopdims + tuple(
            (c,) for c in coredim_shape)
        shape = outshape_loopdims + coredim_shape

        _pprint(chunks=chunks,
                shape=shape)
        _pprint(odsk=odsk)

        oarr = Array(sharedict.merge((_out, odsk), dsk, *dsks), _out, chunks, dtype=dtype)
        oarrs.append(oarr)

    if nout is None:
        return oarrs[0]
    return oarrs
