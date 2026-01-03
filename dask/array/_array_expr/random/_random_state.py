from __future__ import annotations

import contextlib
import importlib
import numbers

import numpy as np

from dask._collections import new_collection
from dask.array.backends import array_creation_dispatch
from dask.array.creation import arange
from dask.utils import derived_from, typename

from ._utils import _wrap_func


class RandomState:
    """
    Mersenne Twister pseudo-random number generator

    This object contains state to deterministically generate pseudo-random
    numbers from a variety of probability distributions.  It is identical to
    ``np.random.RandomState`` except that all functions also take a ``chunks=``
    keyword argument.

    Parameters
    ----------
    seed: Number
        Object to pass to RandomState to serve as deterministic seed
    RandomState: Callable[seed] -> RandomState
        A callable that, when provided with a ``seed`` keyword provides an
        object that operates identically to ``np.random.RandomState`` (the
        default).  This might also be a function that returns a
        ``mkl_random``, or ``cupy.random.RandomState`` object.

    Examples
    --------
    >>> import dask.array as da
    >>> state = da.random.RandomState(1234)  # a seed
    >>> x = state.normal(10, 0.1, size=3, chunks=(2,))
    >>> x.compute()
    array([10.01867852, 10.04812289,  9.89649746])

    See Also
    --------
    np.random.RandomState
    """

    def __init__(self, seed=None, RandomState=None):
        self._numpy_state = np.random.RandomState(seed)
        self._RandomState = (
            array_creation_dispatch.RandomState if RandomState is None else RandomState
        )

    @property
    def _backend(self):
        # Assumes typename(self._RandomState) starts with
        # an importable array-library name (e.g. "numpy" or "cupy")
        _backend_name = typename(self._RandomState).split(".")[0]
        return importlib.import_module(_backend_name)

    def seed(self, seed=None):
        self._numpy_state.seed(seed)

    @derived_from(np.random.RandomState, skipblocks=1)
    def beta(self, a, b, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "beta", a, b, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def binomial(self, n, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "binomial", n, p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def chisquare(self, df, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "chisquare", df, size=size, chunks=chunks, **kwargs)

    with contextlib.suppress(AttributeError):

        @derived_from(np.random.RandomState, skipblocks=1)
        def choice(self, a, size=None, replace=True, p=None, chunks="auto"):
            from ._choice import RandomChoice, _choice_validate_params

            (
                a_val,
                a_expr,
                size,
                replace,
                p_expr,
                axis,  # np.random.RandomState.choice does not use axis
                chunks,
                meta,
            ) = _choice_validate_params(self, a, size, replace, p, 0, chunks)

            return new_collection(
                RandomChoice(
                    a_val, a_expr, chunks, meta, self._numpy_state, replace, p_expr
                )
            )

    @derived_from(np.random.RandomState, skipblocks=1)
    def exponential(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "exponential", scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def f(self, dfnum, dfden, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "f", dfnum, dfden, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def gamma(self, shape, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "gamma", shape, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def geometric(self, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "geometric", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def gumbel(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "gumbel", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def hypergeometric(self, ngood, nbad, nsample, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self,
            "hypergeometric",
            ngood,
            nbad,
            nsample,
            size=size,
            chunks=chunks,
            **kwargs,
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def laplace(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "laplace", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def logistic(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "logistic", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def lognormal(self, mean=0.0, sigma=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "lognormal", mean, sigma, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def logseries(self, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "logseries", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def multinomial(self, n, pvals, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self,
            "multinomial",
            n,
            pvals,
            size=size,
            chunks=chunks,
            extra_chunks=((len(pvals),),),
            **kwargs,
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def negative_binomial(self, n, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "negative_binomial", n, p, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def noncentral_chisquare(self, df, nonc, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "noncentral_chisquare", df, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def noncentral_f(self, dfnum, dfden, nonc, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "noncentral_f", dfnum, dfden, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def normal(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "normal", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def pareto(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "pareto", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def permutation(self, x):
        from dask.array.slicing import shuffle_slice

        if isinstance(x, numbers.Number):
            x = arange(x, chunks="auto")

        index = np.arange(len(x))
        self._numpy_state.shuffle(index)
        return shuffle_slice(x, index)

    @derived_from(np.random.RandomState, skipblocks=1)
    def poisson(self, lam=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "poisson", lam, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def power(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "power", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def randint(self, low, high=None, size=None, chunks="auto", dtype="l", **kwargs):
        return _wrap_func(
            self, "randint", low, high, size=size, chunks=chunks, dtype=dtype, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def random_integers(self, low, high=None, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "random_integers", low, high, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def random_sample(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "random_sample", size=size, chunks=chunks, **kwargs)

    random = random_sample

    @derived_from(np.random.RandomState, skipblocks=1)
    def rayleigh(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "rayleigh", scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def standard_cauchy(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_cauchy", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def standard_exponential(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "standard_exponential", size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def standard_gamma(self, shape, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "standard_gamma", shape, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def standard_normal(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_normal", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def standard_t(self, df, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_t", df, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def tomaxint(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "tomaxint", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def triangular(self, left, mode, right, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "triangular", left, mode, right, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def uniform(self, low=0.0, high=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "uniform", low, high, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def vonmises(self, mu, kappa, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "vonmises", mu, kappa, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.RandomState, skipblocks=1)
    def wald(self, mean, scale, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "wald", mean, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def weibull(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "weibull", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.RandomState, skipblocks=1)
    def zipf(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "zipf", a, size=size, chunks=chunks, **kwargs)
