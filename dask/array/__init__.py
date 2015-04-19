from __future__ import absolute_import, division, print_function

from ..utils import ignoring
from .core import (Array, stack, concatenate, take, tensordot, transpose,
        from_array, choose, where, coarsen, insert, broadcast_to,
        fromfunction, compute, unique, store, squeeze)
from .core import (logaddexp, logaddexp2, conj, exp, log, log2, log10, log1p,
        expm1, sqrt, square, sin, cos, tan, arcsin, arccos, arctan, arctan2,
        hypot, sinh, cosh, tanh, arcsinh, arccosh, arctanh, deg2rad, rad2deg,
        logical_and, logical_or, logical_xor, logical_not, maximum, minimum,
        fmax, fmin, isreal, iscomplex, isfinite, isinf, isnan, signbit,
        copysign, nextafter, ldexp, fmod, floor, ceil, trunc, degrees, radians,
        rint, fix, angle, real, imag, clip, fabs, sign, frexp, modf, around,
        isnull, notnull, isclose)
from .reductions import (sum, prod, mean, std, var, any, all, min, max, vnorm,
                         argmin, argmax,
                         nansum, nanmean, nanstd, nanvar, nanmin,
                         nanmax, nanargmin, nanargmax)
from .percentile import percentile
with ignoring(ImportError):
    from .reductions import nanprod
from . import random, linalg, ghost, learn
from .wrap import ones, zeros, empty, full
from .rechunk import rechunk
from ..context import set_options
from .optimization import optimize
from .creation import arange, linspace
