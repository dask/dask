from __future__ import annotations

from threading import Lock

from ._generator import Generator, default_rng
from ._random_state import RandomState

__all__ = ["Generator", "RandomState", "default_rng"]


# Lazy RNG-state machinery
#
# Many of the RandomState methods are exported as functions in da.random for
# backward compatibility reasons. Their usage is discouraged.
# Use da.random.default_rng() to get a Generator based rng and use its
# methods instead.

_cached_states: dict[str, RandomState] = {}
_cached_states_lock = Lock()


def _make_api(attr):
    def wrapper(*args, **kwargs):
        from dask.array.backends import array_creation_dispatch

        key = array_creation_dispatch.backend
        with _cached_states_lock:
            try:
                state = _cached_states[key]
            except KeyError:
                _cached_states[key] = state = RandomState()
        return getattr(state, attr)(*args, **kwargs)

    wrapper.__name__ = getattr(RandomState, attr).__name__
    wrapper.__doc__ = getattr(RandomState, attr).__doc__
    return wrapper


# RandomState only

seed = _make_api("seed")

beta = _make_api("beta")
binomial = _make_api("binomial")
chisquare = _make_api("chisquare")
choice = _make_api("choice")
exponential = _make_api("exponential")
f = _make_api("f")
gamma = _make_api("gamma")
geometric = _make_api("geometric")
gumbel = _make_api("gumbel")
hypergeometric = _make_api("hypergeometric")
laplace = _make_api("laplace")
logistic = _make_api("logistic")
lognormal = _make_api("lognormal")
logseries = _make_api("logseries")
multinomial = _make_api("multinomial")
negative_binomial = _make_api("negative_binomial")
noncentral_chisquare = _make_api("noncentral_chisquare")
noncentral_f = _make_api("noncentral_f")
normal = _make_api("normal")
pareto = _make_api("pareto")
permutation = _make_api("permutation")
poisson = _make_api("poisson")
power = _make_api("power")
random_sample = _make_api("random_sample")
random = _make_api("random_sample")
randint = _make_api("randint")
random_integers = _make_api("random_integers")
rayleigh = _make_api("rayleigh")
standard_cauchy = _make_api("standard_cauchy")
standard_exponential = _make_api("standard_exponential")
standard_gamma = _make_api("standard_gamma")
standard_normal = _make_api("standard_normal")
standard_t = _make_api("standard_t")
triangular = _make_api("triangular")
uniform = _make_api("uniform")
vonmises = _make_api("vonmises")
wald = _make_api("wald")
weibull = _make_api("weibull")
zipf = _make_api("zipf")
