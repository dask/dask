import contextlib
import importlib
import numbers
from itertools import chain, product
from numbers import Integral
from operator import getitem

import numpy as np

from dask.array.backends import array_creation_dispatch
from dask.array.core import (
    Array,
    asarray,
    broadcast_shapes,
    broadcast_to,
    normalize_chunks,
    slices_from_chunks,
)
from dask.array.creation import arange
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.utils import derived_from, random_state_data, typename


class Generator:
    """
    Container for the BitGenerators.

    ``Generator`` exposes a number of methods for generating random
    numbers drawn from a variety of probability distributions. In addition to
    the distribution-specific arguments, each method takes a keyword argument
    `size` that defaults to ``None``. If `size` is ``None``, then a single
    value is generated and returned. If `size` is an integer, then a 1-D
    array filled with generated values is returned. If `size` is a tuple,
    then an array with that shape is filled and returned.

    The function :func:`dask.array.random.default_rng` will instantiate
    a `Generator` with numpy's default `BitGenerator`.

    **No Compatibility Guarantee**

    ``Generator`` does not provide a version compatibility guarantee. In
    particular, as better algorithms evolve the bit stream may change.

    This object is identical to ``np.random.Generator`` except that all
    functions also take a ``chunks=`` keyword argument.

    Parameters
    ----------
    bit_generator : BitGenerator
        BitGenerator to use as the core generator.

    Notes
    -----
    The Python stdlib module `random` contains pseudo-random number generator
    with a number of methods that are similar to the ones available in
    ``Generator``. It uses Mersenne Twister, and this bit generator can
    be accessed using ``MT19937``. ``Generator``, besides being
    Dask-aware, has the advantage that it provides a much larger number
    of probability distributions to choose from.

    Examples
    --------
    >>> from numpy.random import PCG64
    >>> from dask.array.random import Generator
    >>> rng = Generator(PCG64())
    >>> rng.standard_normal().compute() # doctest: +SKIP
    array(0.44595957)  # random

    See Also
    --------
    default_rng : Recommended constructor for `Generator`.
    np.random.Generator
    """

    def __init__(self, bit_generator):
        self._bit_generator = bit_generator

    def __str__(self):
        _str = self.__class__.__name__
        _str += "(" + self._bit_generator.__class__.__name__ + ")"
        return _str

    @property
    def _backend_name(self):
        # Assumes typename(self._RandomState) starts with an
        # array-library name (e.g. "numpy" or "cupy")
        return typename(self._bit_generator).split(".")[0]

    @property
    def _backend(self):
        # Assumes `self._backend_name` is an importable
        # array-library name (e.g. "numpy" or "cupy")
        return importlib.import_module(self._backend_name)

    @derived_from(np.random.Generator, skipblocks=1)
    def beta(self, a, b, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "beta", a, b, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def binomial(self, n, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "binomial", n, p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def chisquare(self, df, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "chisquare", df, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def choice(
        self,
        a,
        size=None,
        replace=True,
        p=None,
        axis=0,
        shuffle=True,
        chunks="auto",
    ):

        (
            a,
            size,
            replace,
            p,
            axis,
            chunks,
            meta,
            dependencies,
        ) = _choice_validate_params(self, a, size, replace, p, axis, chunks)

        sizes = list(product(*chunks))
        bitgens = _spawn_bitgens(self._bit_generator, len(sizes))

        name = "da.random.choice-%s" % tokenize(
            bitgens, size, chunks, a, replace, p, axis, shuffle
        )
        keys = product([name], *(range(len(bd)) for bd in chunks))
        dsk = {
            k: (_choice_rng, bitgen, a, size, replace, p, axis, shuffle)
            for k, bitgen, size in zip(keys, bitgens, sizes)
        }

        graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
        return Array(graph, name, chunks, meta=meta)

    @derived_from(np.random.Generator, skipblocks=1)
    def exponential(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "exponential", scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def f(self, dfnum, dfden, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "f", dfnum, dfden, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def gamma(self, shape, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "gamma", shape, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def geometric(self, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "geometric", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def gumbel(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "gumbel", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
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

    @derived_from(np.random.Generator, skipblocks=1)
    def integers(
        self,
        low,
        high=None,
        size=None,
        dtype=np.int64,
        endpoint=False,
        chunks="auto",
        **kwargs,
    ):
        return _wrap_func(
            self,
            "integers",
            low,
            high=high,
            size=size,
            dtype=dtype,
            endpoint=endpoint,
            chunks=chunks,
            **kwargs,
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def laplace(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "laplace", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def logistic(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "logistic", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def lognormal(self, mean=0.0, sigma=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "lognormal", mean, sigma, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def logseries(self, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "logseries", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def multinomial(self, n, pvals, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self,
            "multinomial",
            n,
            pvals,
            size=size,
            chunks=chunks,
            extra_chunks=((len(pvals),),),
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def multivariate_hypergeometric(
        self, colors, nsample, size=None, method="marginals", chunks="auto", **kwargs
    ):
        return _wrap_func(
            self,
            "multivariate_hypergeometric",
            colors,
            nsample,
            size=size,
            method=method,
            chunks=chunks,
            **kwargs,
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def negative_binomial(self, n, p, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "negative_binomial", n, p, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def noncentral_chisquare(self, df, nonc, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "noncentral_chisquare", df, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def noncentral_f(self, dfnum, dfden, nonc, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "noncentral_f", dfnum, dfden, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def normal(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "normal", loc, scale, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def pareto(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "pareto", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def permutation(self, x):
        from dask.array.slicing import shuffle_slice

        if self._backend_name == "cupy":
            raise NotImplementedError(
                "`Generator.permutation` not supported for cupy-backed "
                "Generator objects. Use the 'numpy' array backend to "
                "call `dask.array.random.default_rng`, or pass in "
                " `numpy.random.PCG64()`."
            )

        if isinstance(x, numbers.Number):
            x = arange(x, chunks="auto")

        index = self._backend.arange(len(x))
        _shuffle(self._bit_generator, index)
        return shuffle_slice(x, index)

    @derived_from(np.random.Generator, skipblocks=1)
    def poisson(self, lam=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "poisson", lam, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def power(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "power", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def random(self, size=None, dtype=np.float64, out=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "random", size=size, dtype=dtype, out=out, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def rayleigh(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "rayleigh", scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_cauchy(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_cauchy", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_exponential(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "standard_exponential", size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_gamma(self, shape, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "standard_gamma", shape, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_normal(self, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_normal", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_t(self, df, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "standard_t", df, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def triangular(self, left, mode, right, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "triangular", left, mode, right, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def uniform(self, low=0.0, high=1.0, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "uniform", low, high, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def vonmises(self, mu, kappa, size=None, chunks="auto", **kwargs):
        return _wrap_func(
            self, "vonmises", mu, kappa, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def wald(self, mean, scale, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "wald", mean, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def weibull(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "weibull", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def zipf(self, a, size=None, chunks="auto", **kwargs):
        return _wrap_func(self, "zipf", a, size=size, chunks=chunks, **kwargs)


def default_rng(seed=None):
    """
    Construct a new Generator with the default BitGenerator (PCG64).

    Parameters
    ----------
    seed : {None, int, array_like[ints], SeedSequence, BitGenerator, Generator}, optional
        A seed to initialize the `BitGenerator`. If None, then fresh,
        unpredictable entropy will be pulled from the OS. If an ``int`` or
        ``array_like[ints]`` is passed, then it will be passed to
        `SeedSequence` to derive the initial `BitGenerator` state. One may also
        pass in a `SeedSequence` instance.
        Additionally, when passed a `BitGenerator`, it will be wrapped by
        `Generator`. If passed a `Generator`, it will be returned unaltered.

    Returns
    -------
    Generator
        The initialized generator object.

    Notes
    -----
    If ``seed`` is not a `BitGenerator` or a `Generator`, a new `BitGenerator`
    is instantiated. This function does not manage a default global instance.

    Examples
    --------
    ``default_rng`` is the recommended constructor for the random number class
    ``Generator``. Here are several ways we can construct a random
    number generator using ``default_rng`` and the ``Generator`` class.

    Here we use ``default_rng`` to generate a random float:

    >>> import dask.array as da
    >>> rng = da.random.default_rng(12345)
    >>> print(rng)
    Generator(PCG64)
    >>> rfloat = rng.random().compute()
    >>> rfloat
    array(0.86999885)
    >>> type(rfloat)
    <class 'numpy.ndarray'>

    Here we use ``default_rng`` to generate 3 random integers between 0
    (inclusive) and 10 (exclusive):

    >>> import dask.array as da
    >>> rng = da.random.default_rng(12345)
    >>> rints = rng.integers(low=0, high=10, size=3).compute()
    >>> rints
    array([2, 8, 7])
    >>> type(rints[0])
    <class 'numpy.int64'>

    Here we specify a seed so that we have reproducible results:

    >>> import dask.array as da
    >>> rng = da.random.default_rng(seed=42)
    >>> print(rng)
    Generator(PCG64)
    >>> arr1 = rng.random((3, 3)).compute()
    >>> arr1
    array([[0.91674416, 0.91098667, 0.8765925 ],
           [0.30931841, 0.95465607, 0.17509458],
           [0.99662814, 0.75203348, 0.15038118]])

    If we exit and restart our Python interpreter, we'll see that we
    generate the same random numbers again:

    >>> import dask.array as da
    >>> rng = da.random.default_rng(seed=42)
    >>> arr2 = rng.random((3, 3)).compute()
    >>> arr2
    array([[0.91674416, 0.91098667, 0.8765925 ],
           [0.30931841, 0.95465607, 0.17509458],
           [0.99662814, 0.75203348, 0.15038118]])

    See Also
    --------
    np.random.default_rng
    """
    if hasattr(seed, "capsule"):
        # We are passed a BitGenerator, so just wrap it
        return Generator(seed)
    elif isinstance(seed, Generator):
        # Pass through a Generator
        return seed
    elif hasattr(seed, "bit_generator"):
        # a Generator. Just not ours
        return Generator(seed.bit_generator)
    # Otherwise, use the backend-default BitGenerator
    return Generator(array_creation_dispatch.default_bit_generator(seed))


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
        ``randomgen.RandomState``, ``mkl_random``, or
        ``cupy.random.RandomState`` object.

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
            (
                a,
                size,
                replace,
                p,
                axis,  # np.random.RandomState.choice does not use axis
                chunks,
                meta,
                dependencies,
            ) = _choice_validate_params(self, a, size, replace, p, 0, chunks)

            sizes = list(product(*chunks))
            state_data = random_state_data(len(sizes), self._numpy_state)

            name = "da.random.choice-%s" % tokenize(
                state_data, size, chunks, a, replace, p
            )
            keys = product([name], *(range(len(bd)) for bd in chunks))
            dsk = {
                k: (_choice_rs, state, a, size, replace, p)
                for k, state, size in zip(keys, state_data, sizes)
            }

            graph = HighLevelGraph.from_collections(
                name, dsk, dependencies=dependencies
            )
            return Array(graph, name, chunks, meta=meta)

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


def _rng_from_bitgen(bitgen):
    # Assumes typename(bitgen) starts with importable
    # library name (e.g. "numpy" or "cupy")
    backend_name = typename(bitgen).split(".")[0]
    backend_lib = importlib.import_module(backend_name)
    return backend_lib.random.default_rng(bitgen)


def _shuffle(bit_generator, x, axis=0):
    state_data = bit_generator.state
    bit_generator = type(bit_generator)()
    bit_generator.state = state_data
    state = _rng_from_bitgen(bit_generator)
    return state.shuffle(x, axis=axis)


def _spawn_bitgens(bitgen, n_bitgens):
    seeds = bitgen._seed_seq.spawn(n_bitgens)
    bitgens = [type(bitgen)(seed) for seed in seeds]
    return bitgens


def _apply_random_func(rng, funcname, bitgen, size, args, kwargs):
    """Apply random module method with seed"""
    if isinstance(bitgen, np.random.SeedSequence):
        bitgen = rng(bitgen)
    rng = _rng_from_bitgen(bitgen)
    func = getattr(rng, funcname)
    return func(*args, size=size, **kwargs)


def _apply_random(RandomState, funcname, state_data, size, args, kwargs):
    """Apply RandomState method with seed"""
    if RandomState is None:
        RandomState = array_creation_dispatch.RandomState
    state = RandomState(state_data)
    func = getattr(state, funcname)
    return func(*args, size=size, **kwargs)


def _choice_rng(state_data, a, size, replace, p, axis, shuffle):
    state = _rng_from_bitgen(state_data)
    return state.choice(a, size=size, replace=replace, p=p, axis=axis, shuffle=shuffle)


def _choice_rs(state_data, a, size, replace, p):
    state = array_creation_dispatch.RandomState(state_data)
    return state.choice(a, size=size, replace=replace, p=p)


def _choice_validate_params(state, a, size, replace, p, axis, chunks):
    dependencies = []
    # Normalize and validate `a`
    if isinstance(a, Integral):
        if isinstance(state, Generator):
            if state._backend_name == "cupy":
                raise NotImplementedError(
                    "`choice` not supported for cupy-backed `Generator`."
                )
            meta = state._backend.random.default_rng().choice(1, size=(), p=None)
        elif isinstance(state, RandomState):
            # On windows the output dtype differs if p is provided or
            # # absent, see https://github.com/numpy/numpy/issues/9867
            dummy_p = state._backend.array([1]) if p is not None else p
            meta = state._backend.random.RandomState().choice(1, size=(), p=dummy_p)
        else:
            raise ValueError("Unknown generator class")
        len_a = a
        if a < 0:
            raise ValueError("a must be greater than 0")
    else:
        a = asarray(a)
        a = a.rechunk(a.shape)
        meta = a._meta
        if a.ndim != 1:
            raise ValueError("a must be one dimensional")
        len_a = len(a)
        dependencies.append(a)
        a = a.__dask_keys__()[0]

    # Normalize and validate `p`
    if p is not None:
        if not isinstance(p, Array):
            # If p is not a dask array, first check the sum is close
            # to 1 before converting.
            p = np.asarray(p)
            if not np.isclose(p.sum(), 1, rtol=1e-7, atol=0):
                raise ValueError("probabilities do not sum to 1")
            p = asarray(p)
        else:
            p = p.rechunk(p.shape)

        if p.ndim != 1:
            raise ValueError("p must be one dimensional")
        if len(p) != len_a:
            raise ValueError("a and p must have the same size")

        dependencies.append(p)
        p = p.__dask_keys__()[0]

    if size is None:
        size = ()
    elif not isinstance(size, (tuple, list)):
        size = (size,)

    if axis != 0:
        raise ValueError("axis must be 0 since a is one dimensinal")

    chunks = normalize_chunks(chunks, size, dtype=np.float64)
    if not replace and len(chunks[0]) > 1:
        err_msg = (
            "replace=False is not currently supported for "
            "dask.array.choice with multi-chunk output "
            "arrays"
        )
        raise NotImplementedError(err_msg)

    return a, size, replace, p, axis, chunks, meta, dependencies


def _wrap_func(
    rng, funcname, *args, size=None, chunks="auto", extra_chunks=(), **kwargs
):
    """Wrap numpy random function to produce dask.array random function
    extra_chunks should be a chunks tuple to append to the end of chunks
    """
    if size is not None and not isinstance(size, (tuple, list)):
        size = (size,)

    shapes = list(
        {
            ar.shape
            for ar in chain(args, kwargs.values())
            if isinstance(ar, (Array, np.ndarray))
        }
    )
    if size is not None:
        shapes.append(size)
    # broadcast to the final size(shape)
    size = broadcast_shapes(*shapes)
    chunks = normalize_chunks(
        chunks,
        size,  # ideally would use dtype here
        dtype=kwargs.get("dtype", np.float64),
    )
    slices = slices_from_chunks(chunks)

    def _broadcast_any(ar, shape, chunks):
        if isinstance(ar, Array):
            return broadcast_to(ar, shape).rechunk(chunks)
        if isinstance(ar, np.ndarray):
            return np.ascontiguousarray(np.broadcast_to(ar, shape))

    # Broadcast all arguments, get tiny versions as well
    # Start adding the relevant bits to the graph
    dsk = {}
    lookup = {}
    small_args = []
    dependencies = []
    for i, ar in enumerate(args):
        if isinstance(ar, (np.ndarray, Array)):
            res = _broadcast_any(ar, size, chunks)
            if isinstance(res, Array):
                dependencies.append(res)
                lookup[i] = res.name
            elif isinstance(res, np.ndarray):
                name = f"array-{tokenize(res)}"
                lookup[i] = name
                dsk[name] = res
            small_args.append(ar[tuple(0 for _ in ar.shape)])
        else:
            small_args.append(ar)

    small_kwargs = {}
    for key, ar in kwargs.items():
        if isinstance(ar, (np.ndarray, Array)):
            res = _broadcast_any(ar, size, chunks)
            if isinstance(res, Array):
                dependencies.append(res)
                lookup[key] = res.name
            elif isinstance(res, np.ndarray):
                name = f"array-{tokenize(res)}"
                lookup[key] = name
                dsk[name] = res
            small_kwargs[key] = ar[tuple(0 for _ in ar.shape)]
        else:
            small_kwargs[key] = ar

    sizes = list(product(*chunks))
    if isinstance(rng, Generator):
        bitgens = _spawn_bitgens(rng._bit_generator, len(sizes))
        bitgen_token = tokenize(bitgens)
        bitgens = [_bitgen._seed_seq for _bitgen in bitgens]
        func_applier = _apply_random_func
        gen = type(rng._bit_generator)
    elif isinstance(rng, RandomState):
        bitgens = random_state_data(len(sizes), rng._numpy_state)
        bitgen_token = tokenize(bitgens)
        func_applier = _apply_random
        gen = rng._RandomState
    else:
        raise AttributeError("Not a Generator and Not a RandomState")
    token = tokenize(bitgen_token, size, chunks, args, kwargs)
    name = f"{funcname}-{token}"

    keys = product(
        [name], *([range(len(bd)) for bd in chunks] + [[0]] * len(extra_chunks))
    )
    blocks = product(*[range(len(bd)) for bd in chunks])

    vals = []
    for bitgen, size, slc, block in zip(bitgens, sizes, slices, blocks):
        arg = []
        for i, ar in enumerate(args):
            if i not in lookup:
                arg.append(ar)
            else:
                if isinstance(ar, Array):
                    arg.append((lookup[i],) + block)
                else:  # np.ndarray
                    arg.append((getitem, lookup[i], slc))
        kwrg = {}
        for k, ar in kwargs.items():
            if k not in lookup:
                kwrg[k] = ar
            else:
                if isinstance(ar, Array):
                    kwrg[k] = (lookup[k],) + block
                else:  # np.ndarray
                    kwrg[k] = (getitem, lookup[k], slc)
        vals.append((func_applier, gen, funcname, bitgen, size, arg, kwrg))

    meta = func_applier(
        gen,
        funcname,
        bitgen,
        (0,) * len(size),
        small_args,
        small_kwargs,
    )

    dsk.update(dict(zip(keys, vals)))

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
    return Array(graph, name, chunks + extra_chunks, meta=meta)


"""
Lazy RNG-state machinery
"""

_cached_states = {}


def _make_api(attr, state_constructor=None, state_class=None):
    state_constructor = state_constructor or RandomState
    state_class = state_class or RandomState

    def wrapper(*args, **kwargs):
        backend = array_creation_dispatch.backend
        key = (backend, state_constructor.__name__)
        if key not in _cached_states:
            # Cache the default RandomState object for this backend
            _cached_states[key] = state_constructor()
        return getattr(
            _cached_states[key],
            attr,
        )(*args, **kwargs)

    wrapper.__name__ = getattr(state_class, attr).__name__
    wrapper.__doc__ = getattr(state_class, attr).__doc__
    return wrapper


"""
Generator only
"""

integers = _make_api("integers", default_rng, Generator)
multivariate_hypergeometric = _make_api(
    "multivariate_hypergeometric", default_rng, Generator
)
random = _make_api("random", default_rng, Generator)


"""
RandomState only
"""

seed = _make_api("seed")

random_sample = _make_api("random_sample")
random = _make_api("random_sample")
randint = _make_api("randint")
random_integers = _make_api("random_integers")


"""
Common distributions
"""

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
rayleigh = _make_api("rayleigh")
triangular = _make_api("triangular")
uniform = _make_api("uniform")
vonmises = _make_api("vonmises")
wald = _make_api("wald")
weibull = _make_api("weibull")
zipf = _make_api("zipf")


"""
Standard distributions
"""

standard_cauchy = _make_api("standard_cauchy")
standard_exponential = _make_api("standard_exponential")
standard_gamma = _make_api("standard_gamma")
standard_normal = _make_api("standard_normal")
standard_t = _make_api("standard_t")
