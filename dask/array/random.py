import contextlib
import numbers
from itertools import chain, product
from numbers import Integral
from operator import getitem

import numpy as np

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
from dask.utils import derived_from, random_state_data


class Generator:
    """
    The Generator provides access to a wide range of distributions, and serves
    as a replacement for RandomState. The default BitGenerator used by
    Generator is PCG64.

    Generator exposes a number of methods for generating random numbers drawn
    from a variety of probability distributions. In addition to the
    distribution-specific arguments, each method takes a keyword argument size
    that defaults to None. If size is None, then a single value is generated
    and returned. If size is an integer, then a 1-D array filled with generated
    values is returned. If size is a tuple, then an array with that shape is
    filled and returned.

    This object is identical to ``np.random.Generator`` except that all
    functions also take a ``chunks=`` keyword argument.

    Parameters
    ----------
    seed: Number
        Object to pass to Generator to serve as deterministic seed
    Generator: Callable[seed] -> Generator
        A callable that, when provided with a ``seed`` keyword provides an
        object that operates identically to ``np.random.Generator`` (the
        default).  This might also be a function that returns a
        ``randomgen.RandomState``, ``mkl_random``, or
        ``cupy.random.RandomState`` object.

    Examples
    --------
    >>> import dask.array as da
    >>> state = da.random.Generator(1234)  # a seed
    >>> x = state.normal(10, 0.1, size=3, chunks=(2,))
    >>> x.compute()
    array([10.00539854,  9.99862834,  9.81492093])

    See Also
    --------
    np.random.Generator
    """

    def __init__(self, seed=None, Generator=None):
        self._numpy_state = np.random.default_rng(seed)
        self._Generator = Generator

    def seed(self, seed=None):
        self._numpy_state = np.random.default_rng(seed)

    def _wrap(
        self, funcname, *args, size=None, chunks="auto", extra_chunks=(), **kwargs
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
        seeds = random_state_data(len(sizes), self._numpy_state)
        token = tokenize(seeds, size, chunks, args, kwargs)
        name = f"{funcname}-{token}"

        keys = product(
            [name], *([range(len(bd)) for bd in chunks] + [[0]] * len(extra_chunks))
        )
        blocks = product(*[range(len(bd)) for bd in chunks])

        vals = []
        for seed, size, slc, block in zip(seeds, sizes, slices, blocks):
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
            vals.append(
                (_apply_random, self._Generator, funcname, seed, size, arg, kwrg)
            )

        meta = _apply_random(
            self._Generator,
            funcname,
            seed,
            (0,) * len(size),
            small_args,
            small_kwargs,
        )

        dsk.update(dict(zip(keys, vals)))

        graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
        return Array(graph, name, chunks + extra_chunks, meta=meta)

    @derived_from(np.random.Generator, skipblocks=1)
    def beta(self, a, b, size=None, chunks="auto", **kwargs):
        return self._wrap("beta", a, b, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def binomial(self, n, p, size=None, chunks="auto", **kwargs):
        return self._wrap("binomial", n, p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def chisquare(self, df, size=None, chunks="auto", **kwargs):
        return self._wrap("chisquare", df, size=size, chunks=chunks, **kwargs)

    with contextlib.suppress(AttributeError):

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
            dependencies = []
            # Normalize and validate `a`
            if isinstance(a, Integral):
                # On windows the output dtype differs if p is provided or
                # absent, see https://github.com/numpy/numpy/issues/9867
                dummy_p = np.array([1]) if p is not None else p
                dtype = np.random.choice(1, size=(), p=dummy_p).dtype
                len_a = a
                if a < 0:
                    raise ValueError("a must be greater than 0")
            else:
                a = asarray(a)
                a = a.rechunk(a.shape)
                dtype = a.dtype
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

            chunks = normalize_chunks(chunks, size, dtype=np.float64)
            if not replace and len(chunks[0]) > 1:
                err_msg = (
                    "replace=False is not currently supported for "
                    "dask.array.choice with multi-chunk output "
                    "arrays"
                )
                raise NotImplementedError(err_msg)
            sizes = list(product(*chunks))
            state_data = random_state_data(len(sizes), self._numpy_state)

            name = "da.random.choice-%s" % tokenize(
                state_data, size, chunks, a, replace, p
            )
            keys = product([name], *(range(len(bd)) for bd in chunks))
            dsk = {
                k: (_choice, state, a, size, replace, p, axis, shuffle)
                for k, state, size in zip(keys, state_data, sizes)
            }

            graph = HighLevelGraph.from_collections(
                name, dsk, dependencies=dependencies
            )
            return Array(graph, name, chunks, dtype=dtype)

    # @derived_from(np.random.Generator, skipblocks=1)
    # def dirichlet(self, alpha, size=None, chunks="auto"):

    @derived_from(np.random.Generator, skipblocks=1)
    def exponential(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("exponential", scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def f(self, dfnum, dfden, size=None, chunks="auto", **kwargs):
        return self._wrap("f", dfnum, dfden, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def gamma(self, shape, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("gamma", shape, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def geometric(self, p, size=None, chunks="auto", **kwargs):
        return self._wrap("geometric", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def gumbel(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("gumbel", loc, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def hypergeometric(self, ngood, nbad, nsample, size=None, chunks="auto", **kwargs):
        return self._wrap(
            "hypergeometric", ngood, nbad, nsample, size=size, chunks=chunks, **kwargs
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
        return self._wrap(
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
        return self._wrap("laplace", loc, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def logistic(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("logistic", loc, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def lognormal(self, mean=0.0, sigma=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("lognormal", mean, sigma, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def logseries(self, p, size=None, chunks="auto", **kwargs):
        return self._wrap("logseries", p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def multinomial(self, n, pvals, size=None, chunks="auto", **kwargs):
        return self._wrap(
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
        return self._wrap(
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
        return self._wrap("negative_binomial", n, p, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def noncentral_chisquare(self, df, nonc, size=None, chunks="auto", **kwargs):
        return self._wrap(
            "noncentral_chisquare", df, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def noncentral_f(self, dfnum, dfden, nonc, size=None, chunks="auto", **kwargs):
        return self._wrap(
            "noncentral_f", dfnum, dfden, nonc, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def normal(self, loc=0.0, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("normal", loc, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def pareto(self, a, size=None, chunks="auto", **kwargs):
        return self._wrap("pareto", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def permutation(self, x):
        from dask.array.slicing import shuffle_slice

        if isinstance(x, numbers.Number):
            x = arange(x, chunks="auto")

        index = np.arange(len(x))
        self._numpy_state.shuffle(index)
        return shuffle_slice(x, index)

    @derived_from(np.random.Generator, skipblocks=1)
    def poisson(self, lam=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("poisson", lam, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def power(self, a, size=None, chunks="auto", **kwargs):
        return self._wrap("power", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def random(self, size=None, dtype=np.float64, out=None, chunks="auto", **kwargs):
        return self._wrap(
            "random", size=size, dtype=dtype, out=out, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def rayleigh(self, scale=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("rayleigh", scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_cauchy(self, size=None, chunks="auto", **kwargs):
        return self._wrap("standard_cauchy", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_exponential(self, size=None, chunks="auto", **kwargs):
        return self._wrap("standard_exponential", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_gamma(self, shape, size=None, chunks="auto", **kwargs):
        return self._wrap("standard_gamma", shape, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_normal(self, size=None, chunks="auto", **kwargs):
        return self._wrap("standard_normal", size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def standard_t(self, df, size=None, chunks="auto", **kwargs):
        return self._wrap("standard_t", df, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def triangular(self, left, mode, right, size=None, chunks="auto", **kwargs):
        return self._wrap(
            "triangular", left, mode, right, size=size, chunks=chunks, **kwargs
        )

    @derived_from(np.random.Generator, skipblocks=1)
    def uniform(self, low=0.0, high=1.0, size=None, chunks="auto", **kwargs):
        return self._wrap("uniform", low, high, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def vonmises(self, mu, kappa, size=None, chunks="auto", **kwargs):
        return self._wrap("vonmises", mu, kappa, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def wald(self, mean, scale, size=None, chunks="auto", **kwargs):
        return self._wrap("wald", mean, scale, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def weibull(self, a, size=None, chunks="auto", **kwargs):
        return self._wrap("weibull", a, size=size, chunks=chunks, **kwargs)

    @derived_from(np.random.Generator, skipblocks=1)
    def zipf(self, a, size=None, chunks="auto", **kwargs):
        return self._wrap("zipf", a, size=size, chunks=chunks, **kwargs)


class RandomState(Generator):
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
        super().__init__(seed=seed, Generator=RandomState)

    def randint(self, *args, **kwargs):
        return super().integers(*args, endpoint=False, **kwargs)

    def random_integers(self, *args, **kwargs):
        return super().integers(*args, endpoint=False, **kwargs)

    def random_sample(self, *args, **kwargs):
        return super().random(*args, **kwargs)

    def tomaxint(self, size=None, chunks="auto", **kwargs):
        return super().integers(0, np.iinfo(np.int_).max, endpoint=False)


def _choice(state_data, a, size, replace, p, axis, shuffle):
    state = np.random.default_rng(state_data)
    return state.choice(a, size=size, replace=replace, p=p, axis=axis, shuffle=shuffle)


def _apply_random(Generator, funcname, state_data, size, args, kwargs):
    """Apply Generator method with seed"""
    if Generator is None:
        Generator = np.random.Generator
    state = np.random.default_rng(state_data)
    func = getattr(state, funcname)
    return func(*args, size=size, **kwargs)


# TODO: change to Generator() after deprecation period
_state = RandomState()


seed = _state.seed


beta = _state.beta
binomial = _state.binomial
chisquare = _state.chisquare
if hasattr(_state, "choice"):
    choice = _state.choice
exponential = _state.exponential
f = _state.f
gamma = _state.gamma
geometric = _state.geometric
gumbel = _state.gumbel
hypergeometric = _state.hypergeometric
integers = _state.integers
laplace = _state.laplace
logistic = _state.logistic
lognormal = _state.lognormal
logseries = _state.logseries
multinomial = _state.multinomial
multivariate_hypergeometric = _state.multivariate_hypergeometric
negative_binomial = _state.negative_binomial
noncentral_chisquare = _state.noncentral_chisquare
noncentral_f = _state.noncentral_f
normal = _state.normal
pareto = _state.pareto
permutation = _state.permutation
poisson = _state.poisson
power = _state.power
random = _state.random
rayleigh = _state.rayleigh
triangular = _state.triangular
uniform = _state.uniform
vonmises = _state.vonmises
wald = _state.wald
weibull = _state.weibull
zipf = _state.zipf

"""
Standard distributions
"""

standard_cauchy = _state.standard_cauchy
standard_exponential = _state.standard_exponential
standard_gamma = _state.standard_gamma
standard_normal = _state.standard_normal
standard_t = _state.standard_t


"""
Deprecated. Only for RandomState
"""
if hasattr(_state, "random_sample"):
    random_sample = _state.random_sample
if hasattr(_state, "randint"):
    randint = _state.randint
if hasattr(_state, "random_integers"):
    random_integers = _state.random_integers
if hasattr(_state, "tomaxint"):
    tomaxint = _state.tomaxint
