from __future__ import absolute_import, division, print_function

from operator import getitem

import numpy as np

from toolz.curried import merge
from .core import Array, elemwise
from .. import core
from ..utils import skip_doctest


def __array_wrap__(numpy_ufunc, x, *args, **kwargs):
    return x.__array_wrap__(numpy_ufunc(x, *args, **kwargs))


def wrap_elemwise(numpy_ufunc, array_wrap=False):
    """ Wrap up numpy function into dask.array """

    def wrapped(*args, **kwargs):
        dsk = [arg for arg in args if hasattr(arg, '_elemwise')]
        if len(dsk) > 0:
            if array_wrap:
                return dsk[0]._elemwise(__array_wrap__, numpy_ufunc,
                                        *args, **kwargs)
            else:
                return dsk[0]._elemwise(numpy_ufunc, *args, **kwargs)
        else:
            return numpy_ufunc(*args, **kwargs)

    # functools.wraps cannot wrap ufunc in Python 2.x
    wrapped.__name__ = numpy_ufunc.__name__
    wrapped.__doc__ = skip_doctest(numpy_ufunc.__doc__)
    return wrapped


# ufuncs, copied from this page:
# http://docs.scipy.org/doc/numpy/reference/ufuncs.html

# math operations
logaddexp = wrap_elemwise(np.logaddexp)
logaddexp2 = wrap_elemwise(np.logaddexp2)
conj = wrap_elemwise(np.conj)
exp = wrap_elemwise(np.exp)
log = wrap_elemwise(np.log)
log2 = wrap_elemwise(np.log2)
log10 = wrap_elemwise(np.log10)
log1p = wrap_elemwise(np.log1p)
expm1 = wrap_elemwise(np.expm1)
sqrt = wrap_elemwise(np.sqrt)
square = wrap_elemwise(np.square)

# trigonometric functions
sin = wrap_elemwise(np.sin)
cos = wrap_elemwise(np.cos)
tan = wrap_elemwise(np.tan)
arcsin = wrap_elemwise(np.arcsin)
arccos = wrap_elemwise(np.arccos)
arctan = wrap_elemwise(np.arctan)
arctan2 = wrap_elemwise(np.arctan2)
hypot = wrap_elemwise(np.hypot)
sinh = wrap_elemwise(np.sinh)
cosh = wrap_elemwise(np.cosh)
tanh = wrap_elemwise(np.tanh)
arcsinh = wrap_elemwise(np.arcsinh)
arccosh = wrap_elemwise(np.arccosh)
arctanh = wrap_elemwise(np.arctanh)
deg2rad = wrap_elemwise(np.deg2rad)
rad2deg = wrap_elemwise(np.rad2deg)

# comparison functions
logical_and = wrap_elemwise(np.logical_and)
logical_or = wrap_elemwise(np.logical_or)
logical_xor = wrap_elemwise(np.logical_xor)
logical_not = wrap_elemwise(np.logical_not)
maximum = wrap_elemwise(np.maximum)
minimum = wrap_elemwise(np.minimum)
fmax = wrap_elemwise(np.fmax)
fmin = wrap_elemwise(np.fmin)

# floating functions
isreal = wrap_elemwise(np.isreal, array_wrap=True)
iscomplex = wrap_elemwise(np.iscomplex, array_wrap=True)
isfinite = wrap_elemwise(np.isfinite)
isinf = wrap_elemwise(np.isinf)
isnan = wrap_elemwise(np.isnan)
signbit = wrap_elemwise(np.signbit)
copysign = wrap_elemwise(np.copysign)
nextafter = wrap_elemwise(np.nextafter)
# modf: see below
ldexp = wrap_elemwise(np.ldexp)
# frexp: see below
fmod = wrap_elemwise(np.fmod)
floor = wrap_elemwise(np.floor)
ceil = wrap_elemwise(np.ceil)
trunc = wrap_elemwise(np.trunc)

# more math routines, from this page:
# http://docs.scipy.org/doc/numpy/reference/routines.math.html
degrees = wrap_elemwise(np.degrees)
radians = wrap_elemwise(np.radians)

rint = wrap_elemwise(np.rint)
fix = wrap_elemwise(np.fix, array_wrap=True)

angle = wrap_elemwise(np.angle, array_wrap=True)
real = wrap_elemwise(np.real, array_wrap=True)
imag = wrap_elemwise(np.imag, array_wrap=True)

clip = wrap_elemwise(np.clip)
fabs = wrap_elemwise(np.fabs)
sign = wrap_elemwise(np.sign)
absolute = wrap_elemwise(np.absolute)


def frexp(x):
    # Not actually object dtype, just need to specify something
    tmp = elemwise(np.frexp, x, dtype=object)
    left = 'mantissa-' + tmp.name
    right = 'exponent-' + tmp.name
    ldsk = dict(((left,) + key[1:], (getitem, key, 0))
                for key in core.flatten(tmp._keys()))
    rdsk = dict(((right,) + key[1:], (getitem, key, 1))
                for key in core.flatten(tmp._keys()))

    a = np.empty((1, ), dtype=x.dtype)
    l, r = np.frexp(a)
    ldt = l.dtype
    rdt = r.dtype

    L = Array(merge(tmp.dask, ldsk), left, chunks=tmp.chunks, dtype=ldt)
    R = Array(merge(tmp.dask, rdsk), right, chunks=tmp.chunks, dtype=rdt)
    return L, R


frexp.__doc__ = skip_doctest(np.frexp.__doc__)


def modf(x):
    # Not actually object dtype, just need to specify something
    tmp = elemwise(np.modf, x, dtype=object)
    left = 'modf1-' + tmp.name
    right = 'modf2-' + tmp.name
    ldsk = dict(((left,) + key[1:], (getitem, key, 0))
                for key in core.flatten(tmp._keys()))
    rdsk = dict(((right,) + key[1:], (getitem, key, 1))
                for key in core.flatten(tmp._keys()))

    a = np.empty((1,), dtype=x.dtype)
    l, r = np.modf(a)
    ldt = l.dtype
    rdt = r.dtype

    L = Array(merge(tmp.dask, ldsk), left, chunks=tmp.chunks, dtype=ldt)
    R = Array(merge(tmp.dask, rdsk), right, chunks=tmp.chunks, dtype=rdt)
    return L, R


modf.__doc__ = skip_doctest(np.modf.__doc__)
