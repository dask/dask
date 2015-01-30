from itertools import count, product
from toolz import curry
from .core import Array
import numpy as np


names = ('rand_%d' % i for i in count(1))


def dims_from_size(size, blocksize):
    """

    >>> list(dims_from_size(30, 8))
    [8, 8, 8, 6]
    """
    while size - blocksize > 0:
        yield blocksize
        size -= blocksize
    yield size


def blockdims_from_blockshape(shape, blockshape):
    """
    Convert blockshape to dimensions along each axis

    >>> blockdims_from_blockshape((30, 30), (10, 10))
    ((10, 10, 10), (10, 10, 10))
    >>> blockdims_from_blockshape((30, 30), (12, 12))
    ((12, 12, 6), (12, 12, 6))
    """
    return tuple(map(tuple, map(dims_from_size, shape, blockshape)))


def generic(func, *args, **kwargs):
    """
    Transform np.random function into blocked version
    """
    if 'size' not in kwargs:
        size, args = args[-1], args[:-1]
    else:
        size = kwargs.pop('size')
    blockshape = kwargs.pop('blockshape', None)
    blockdims = kwargs.pop('blockdims', None)
    name = kwargs.pop('name', None)
    if not blockdims and blockshape:
        blockdims = blockdims_from_blockshape(size, blockshape)

    name = name or next(names)

    keys = product([name], *[range(len(bd)) for bd in blockdims])
    sizes = product(*blockdims)
    vals = ((curry(func, *args, size=size, **kwargs),) for size in sizes)

    dsk = dict(zip(keys, vals))
    return Array(dsk, name, shape=size, blockdims=blockdims)


from functools import wraps

"""
Univariate distributions
"""

def f(func):
    return wraps(func)(curry(generic, func))


random = f(np.random.random)
beta = f(np.random.beta)
binomial = f(np.random.binomial)
chisquare = f(np.random.chisquare)
exponential = f(np.random.exponential)
f = f(np.random.f)
gamma = f(np.random.gamma)
geometric = f(np.random.geometric)
gumbel = f(np.random.gumbel)
hypergeometric = f(np.random.hypergeometric)
laplace = f(np.random.laplace)
logistic = f(np.random.logistic)
lognormal = f(np.random.lognormal)
logseries = f(np.random.logseries)
negative_binomial = f(np.random.negative_binomial)
noncentral_chisquare = f(np.random.noncentral_chisquare)
noncentral_f = f(np.random.noncentral_f)
normal = f(np.random.normal)
pareto = f(np.random.pareto)
poisson = f(np.random.poisson)
power = f(np.random.power)
rayleigh = f(np.random.rayleigh)
triangular = f(np.random.triangular)
uniform = f(np.random.uniform)
vonmises = f(np.random.vonmises)
wald = f(np.random.wald)
weibull = f(np.random.weibull)
zipf = f(np.random.zipf)

"""
Standard distributions
"""

standard_cauchy = f(np.random.standard_cauchy)
standard_exponential = f(np.random.standard_exponential)
standard_gamma = f(np.random.standard_gamma)
standard_normal = f(np.random.standard_normal)
standard_t = f(np.random.standard_t)

"""
TODO: Multivariate distributions

dirichlet =
multinomial =
"""
