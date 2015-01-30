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
