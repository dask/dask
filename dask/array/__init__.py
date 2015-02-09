from __future__ import absolute_import, division, print_function
from multipledispatch import halt_ordering, restart_ordering
import blaze

halt_ordering()
from .core import Array, stack, concatenate, tensordot, transpose, from_array

from .core import (arccos, arcsin, arctan, arctanh, arccosh, arcsinh, arctan2,
        ceil, copysign, cos, cosh, degrees, exp, expm1, fabs, floor, fmod,
        frexp, hypot, isinf, isnan, ldexp, log, log10, log1p, modf, radians,
        sin, sinh, sqrt, tan, tanh, trunc)
from .blaze import np  # need to go through import process here
from .into import into # Otherwise someone might import later
                       # without ordering halted
from . import random
from .wrap import ones, zeros, empty
restart_ordering()
