from __future__ import absolute_import, division, print_function

import numpy as np
from .wrap import wrap, wrap_func_size_as_kwarg

"""
Univariate distributions
"""

wrap = wrap(wrap_func_size_as_kwarg)

random = wrap(np.random.random)
beta = wrap(np.random.beta)
binomial = wrap(np.random.binomial)
chisquare = wrap(np.random.chisquare)
exponential = wrap(np.random.exponential)
f = wrap(np.random.f)
gamma = wrap(np.random.gamma)
geometric = wrap(np.random.geometric)
gumbel = wrap(np.random.gumbel)
hypergeometric = wrap(np.random.hypergeometric)
laplace = wrap(np.random.laplace)
logistic = wrap(np.random.logistic)
lognormal = wrap(np.random.lognormal)
logseries = wrap(np.random.logseries)
negative_binomial = wrap(np.random.negative_binomial)
noncentral_chisquare = wrap(np.random.noncentral_chisquare)
noncentral_f = wrap(np.random.noncentral_f)
normal = wrap(np.random.normal)
pareto = wrap(np.random.pareto)
poisson = wrap(np.random.poisson)
power = wrap(np.random.power)
rayleigh = wrap(np.random.rayleigh)
triangular = wrap(np.random.triangular)
uniform = wrap(np.random.uniform)
vonmises = wrap(np.random.vonmises)
wald = wrap(np.random.wald)
weibull = wrap(np.random.weibull)
zipf = wrap(np.random.zipf)

"""
Standard distributions
"""

standard_cauchy = wrap(np.random.standard_cauchy)
standard_exponential = wrap(np.random.standard_exponential)
standard_gamma = wrap(np.random.standard_gamma)
standard_normal = wrap(np.random.standard_normal)
standard_t = wrap(np.random.standard_t)

"""
TODO: Multivariate distributions

dirichlet =
multinomial =
"""

from .core import normalize_chunks, Array, names
from itertools import product
from toolz import curry

def doc_wraps(func):
    def _(func2):
        func2.__doc__ = func.__doc__
        return func2
    return _


class RandomState(object):
    def __init__(self, seed=None):
        self.state = np.random.RandomState(seed)

    def wrap(self, func, *args, **kwargs):
        size = kwargs.pop('size')
        chunks = kwargs.pop('chunks')

        if not isinstance(size, (tuple, list)):
            size = (size,)

        chunks = normalize_chunks(chunks, size)
        name = next(names)

        # Get dtype
        kw = kwargs.copy()
        kw['size'] = (0,)
        dtype = func(np.random.RandomState(), *args, **kw).dtype

        # Build graph
        keys = product([name], *[range(len(bd)) for bd in chunks])
        sizes = product(*chunks)
        vals = ((apply_random, func, self.state.randint(2**31),
                               size, args, kwargs)
                  for size in sizes)
        dsk = dict(zip(keys, vals))

        return Array(dsk, name, chunks, dtype=dtype)

    @doc_wraps(np.random.RandomState.beta)
    def beta(self, a, b, size=None, chunks=None):
        return self.wrap(np.random.RandomState.beta, a, b,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.binomial)
    def binomial(self, n, p, size=None, chunks=None):
        return self.wrap(np.random.RandomState.binomial, n, p,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.chisquare)
    def chisquare(self, df, size=None, chunks=None):
        return self.wrap(np.random.RandomState.chisquare, df,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.choice)
    def choice(self, a, size=None, replace=True, p=None, chunks=None):
        return self.wrap(np.random.RandomState.choice, a,
                         size=size, replace=True, p=None, chunks=chunks)

    # @doc_wraps(np.random.RandomState.dirichlet)
    # def dirichlet(self, alpha, size=None, chunks=None):

    @doc_wraps(np.random.RandomState.exponential)
    def exponential(self, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.exponential, scale=1.0,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.f)
    def f(self, dfnum, dfden, size=None, chunks=None):
        return self.wrap(np.random.RandomState.f, dfnum, dfden,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.gamma)
    def gamma(self, shape, scale=1.0, chunks=None):
        return self.wrap(np.random.RandomState.gamma, scale,
                         size=shape, chunks=chunks)

    @doc_wraps(np.random.RandomState.geometric)
    def geometric(self, p, size=None, chunks=None):
        return self.wrap(np.random.RandomState.geometric, p,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.gumbel)
    def gumbel(self, loc=0.0, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.gumbel, loc, scale,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.hypergeometric)
    def hypergeometric(self, ngood, nbad, nsample, size=None, chunks=None):
        return self.wrap(np.random.RandomState.hypergeometric,
                         ngood, nbad, nsample,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.laplace)
    def laplace(self, loc=0.0, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.laplace, loc=0.0, scale=1.0,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.logistic)
    def logistic(self, loc=0.0, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.logistic, loc=0.0, scale=1.0,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.lognormal)
    def lognormal(self, mean=0.0, sigma=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.lognormal, mean=0.0, sigma=1.0,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.logseries)
    def logseries(self, p, size=None, chunks=None):
        return self.wrap(np.random.RandomState.logseries, p,
                         size=size, chunks=chunks)

    # multinomial

    @doc_wraps(np.random.RandomState.negative_binomial)
    def negative_binomial(self, n, p, size=None, chunks=None):
        return self.wrap(np.random.RandomState.negative_binomial, n, p,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.noncentral_chisquare)
    def noncentral_chisquare(self, df, nonc, size=None, chunks=None):
        return self.wrap(np.random.RandomState.noncentral_chisquare, df, nonc,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.noncentral_f)
    def noncentral_f(self, dfnum, dfden, nonc,  size=None, chunks=None):
        return self.wrap(np.random.RandomState.noncentral_f,
                         dfnum, dfden, nonc,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.normal)
    def normal(self, loc=0.0, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.normal, loc, scale,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.pareto)
    def pareto(self, a, size=None, chunks=None):
        return self.wrap(np.random.RandomState.pareto, a,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.poisson)
    def poisson(self, lam=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.poisson, lam,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.power)
    def power(self, a, size=None, chunks=None):
        return self.wrap(np.random.RandomState.power, a,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.randint)
    def randint(self, low, high=None, size=None, chunks=None):
        return self.wrap(np.random.RandomState.randint, low, high,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.random_integers)
    def random_integers(self, low, high=None, size=None, chunks=None):
        return self.wrap(np.random.RandomState.random_integers, low, high,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.random_sample)
    def random_sample(self, size=None, chunks=None):
        return self.wrap(np.random.RandomState.random_sample,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.rayleigh)
    def rayleigh(self, scale=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.rayleigh, scale=1.0,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.standard_cauchy)
    def standard_cauchy(self, size=None, chunks=None):
        return self.wrap(np.random.RandomState.standard_cauchy,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.standard_exponential)
    def standard_exponential(self, size=None, chunks=None):
        return self.wrap(np.random.RandomState.standard_exponential,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.standard_gamma)
    def standard_gamma(self, shape, size=None, chunks=None):
        return self.wrap(np.random.RandomState.standard_gamma, shape,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.standard_normal)
    def standard_normal(self, size=None, chunks=None):
        return self.wrap(np.random.RandomState.standard_normal,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.standard_t)
    def standard_t(self, df, size=None, chunks=None):
        return self.wrap(np.random.RandomState.standard_t, df,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.tomaxint)
    def tomaxint(self, size=None, chunks=None):
        return self.wrap(np.random.RandomState.tomaxint,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.triangular)
    def triangular(self, left, mode, right, size=None, chunks=None):
        return self.wrap(np.random.RandomState.triangular, left, mode, right,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.uniform)
    def uniform(self, low=0.0, high=1.0, size=None, chunks=None):
        return self.wrap(np.random.RandomState.uniform, low, high,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.vonmises)
    def vonmises(self, mu, kappa, size=None, chunks=None):
        return self.wrap(np.random.RandomState.vonmises, mu, kappa,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.wald)
    def wald(self, mean, scale, size=None, chunks=None):
        return self.wrap(np.random.RandomState.wald, mean, scale,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.weibull)
    def weibull(self, a, size=None, chunks=None):
        return self.wrap(np.random.RandomState.weibull, a,
                         size=size, chunks=chunks)

    @doc_wraps(np.random.RandomState.zipf)
    def zipf(self, a, size=None, chunks=None):
        return self.wrap(np.random.RandomState.zipf, a,
                         size=size, chunks=chunks)


def apply_random(func, seed, size, args, kwargs):
    state = np.random.RandomState(seed)
    return func(state, *args, size=size, **kwargs)
