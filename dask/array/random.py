import numbers
import warnings
from itertools import product
from numbers import Integral
from operator import getitem

import numpy as np

from .core import (
    normalize_chunks,
    Array,
    slices_from_chunks,
    asarray,
    broadcast_shapes,
    broadcast_to,
)
from .creation import arange
from ..base import tokenize
from ..highlevelgraph import HighLevelGraph
from ..utils import ignoring, random_state_data, derived_from, skip_doctest


def doc_wraps(func):
    """ Copy docstring from one function to another """
    warnings.warn(
        "dask.array.random.doc_wraps is deprecated and will be removed in a future version",
        FutureWarning,
    )

    def _(func2):
        if func.__doc__ is not None:
            func2.__doc__ = skip_doctest(func.__doc__)
        return func2

    return _


class RandomState(object):
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
        self._RandomState = RandomState

    def seed(self, seed=None):
        self._numpy_state.seed(seed)

    def _wrap(self, funcname, *args, **kwargs):
        """ Wrap numpy random function to produce dask.array random function

        extra_chunks should be a chunks tuple to append to the end of chunks
        """
        size = kwargs.pop("size", None)
        chunks = kwargs.pop("chunks", "auto")
        extra_chunks = kwargs.pop("extra_chunks", ())

        if size is not None and not isinstance(size, (tuple, list)):
            size = (size,)

        args_shapes = {ar.shape for ar in args if isinstance(ar, (Array, np.ndarray))}
        args_shapes.union(
            {ar.shape for ar in kwargs.values() if isinstance(ar, (Array, np.ndarray))}
        )

        shapes = list(args_shapes)
        if size is not None:
            shapes += [size]
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
        dsks = []
        lookup = {}
        small_args = []
        dependencies = []
        for i, ar in enumerate(args):
            if isinstance(ar, (np.ndarray, Array)):
                res = _broadcast_any(ar, size, chunks)
                if isinstance(res, Array):
                    dependencies.append(res)
                    dsks.append(res.dask)
                    lookup[i] = res.name
                elif isinstance(res, np.ndarray):
                    name = "array-{}".format(tokenize(res))
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
                    dsks.append(res.dask)
                    lookup[key] = res.name
                elif isinstance(res, np.ndarray):
                    name = "array-{}".format(tokenize(res))
                    lookup[key] = name
                    dsk[name] = res
                small_kwargs[key] = ar[tuple(0 for _ in ar.shape)]
            else:
                small_kwargs[key] = ar

        sizes = list(product(*chunks))
        seeds = random_state_data(len(sizes), self._numpy_state)
        token = tokenize(seeds, size, chunks, args, kwargs)
        name = "{0}-{1}".format(funcname, token)

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
                        dependencies.append(ar)
                        arg.append((lookup[i],) + block)
                    else:  # np.ndarray
                        arg.append((getitem, lookup[i], slc))
            kwrg = {}
            for k, ar in kwargs.items():
                if k not in lookup:
                    kwrg[k] = ar
                else:
                    if isinstance(ar, Array):
                        dependencies.append(ar)
                        kwrg[k] = (lookup[k],) + block
                    else:  # np.ndarray
                        kwrg[k] = (getitem, lookup[k], slc)
            vals.append(
                (_apply_random, self._RandomState, funcname, seed, size, arg, kwrg)
            )

        meta = _apply_random(
            self._RandomState,
            funcname,
            seed,
            (0,) * len(size),
            small_args,
            small_kwargs,
        )

        dsk.update(dict(zip(keys, vals)))

        graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
        return Array(graph, name, chunks + extra_chunks, meta=meta)

    @derived_from(np.random.RandomState)
    def beta(self, a, b, size=None, chunks="auto"):
        return self._wrap("beta", a, b, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def binomial(self, n, p, size=None, chunks="auto"):
        return self._wrap("binomial", n, p, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def chisquare(self, df, size=None, chunks="auto"):
        return self._wrap("chisquare", df, size=size, chunks=chunks)

    with ignoring(AttributeError):

        @derived_from(np.random.RandomState)
        def choice(self, a, size=None, replace=True, p=None, chunks="auto"):
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
                k: (_choice, state, a, size, replace, p)
                for k, state, size in zip(keys, state_data, sizes)
            }

            graph = HighLevelGraph.from_collections(
                name, dsk, dependencies=dependencies
            )
            return Array(graph, name, chunks, dtype=dtype)

    # @derived_from(np.random.RandomState)
    # def dirichlet(self, alpha, size=None, chunks="auto"):

    @derived_from(np.random.RandomState)
    def exponential(self, scale=1.0, size=None, chunks="auto"):
        return self._wrap("exponential", scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def f(self, dfnum, dfden, size=None, chunks="auto"):
        return self._wrap("f", dfnum, dfden, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def gamma(self, shape, scale=1.0, size=None, chunks="auto"):
        return self._wrap("gamma", shape, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def geometric(self, p, size=None, chunks="auto"):
        return self._wrap("geometric", p, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def gumbel(self, loc=0.0, scale=1.0, size=None, chunks="auto"):
        return self._wrap("gumbel", loc, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def hypergeometric(self, ngood, nbad, nsample, size=None, chunks="auto"):
        return self._wrap(
            "hypergeometric", ngood, nbad, nsample, size=size, chunks=chunks
        )

    @derived_from(np.random.RandomState)
    def laplace(self, loc=0.0, scale=1.0, size=None, chunks="auto"):
        return self._wrap("laplace", loc, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def logistic(self, loc=0.0, scale=1.0, size=None, chunks="auto"):
        return self._wrap("logistic", loc, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def lognormal(self, mean=0.0, sigma=1.0, size=None, chunks="auto"):
        return self._wrap("lognormal", mean, sigma, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def logseries(self, p, size=None, chunks="auto"):
        return self._wrap("logseries", p, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def multinomial(self, n, pvals, size=None, chunks="auto"):
        return self._wrap(
            "multinomial",
            n,
            pvals,
            size=size,
            chunks=chunks,
            extra_chunks=((len(pvals),),),
        )

    @derived_from(np.random.RandomState)
    def negative_binomial(self, n, p, size=None, chunks="auto"):
        return self._wrap("negative_binomial", n, p, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def noncentral_chisquare(self, df, nonc, size=None, chunks="auto"):
        return self._wrap("noncentral_chisquare", df, nonc, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def noncentral_f(self, dfnum, dfden, nonc, size=None, chunks="auto"):
        return self._wrap("noncentral_f", dfnum, dfden, nonc, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def normal(self, loc=0.0, scale=1.0, size=None, chunks="auto"):
        return self._wrap("normal", loc, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def pareto(self, a, size=None, chunks="auto"):
        return self._wrap("pareto", a, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def permutation(self, x):
        from .slicing import shuffle_slice

        if isinstance(x, numbers.Number):
            x = arange(x, chunks="auto")

        index = np.arange(len(x))
        self._numpy_state.shuffle(index)
        return shuffle_slice(x, index)

    @derived_from(np.random.RandomState)
    def poisson(self, lam=1.0, size=None, chunks="auto"):
        return self._wrap("poisson", lam, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def power(self, a, size=None, chunks="auto"):
        return self._wrap("power", a, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def randint(self, low, high=None, size=None, chunks="auto", dtype="l"):
        return self._wrap("randint", low, high, size=size, chunks=chunks, dtype=dtype)

    @derived_from(np.random.RandomState)
    def random_integers(self, low, high=None, size=None, chunks="auto"):
        return self._wrap("random_integers", low, high, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def random_sample(self, size=None, chunks="auto"):
        return self._wrap("random_sample", size=size, chunks=chunks)

    random = random_sample

    @derived_from(np.random.RandomState)
    def rayleigh(self, scale=1.0, size=None, chunks="auto"):
        return self._wrap("rayleigh", scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def standard_cauchy(self, size=None, chunks="auto"):
        return self._wrap("standard_cauchy", size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def standard_exponential(self, size=None, chunks="auto"):
        return self._wrap("standard_exponential", size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def standard_gamma(self, shape, size=None, chunks="auto"):
        return self._wrap("standard_gamma", shape, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def standard_normal(self, size=None, chunks="auto"):
        return self._wrap("standard_normal", size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def standard_t(self, df, size=None, chunks="auto"):
        return self._wrap("standard_t", df, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def tomaxint(self, size=None, chunks="auto"):
        return self._wrap("tomaxint", size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def triangular(self, left, mode, right, size=None, chunks="auto"):
        return self._wrap("triangular", left, mode, right, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def uniform(self, low=0.0, high=1.0, size=None, chunks="auto"):
        return self._wrap("uniform", low, high, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def vonmises(self, mu, kappa, size=None, chunks="auto"):
        return self._wrap("vonmises", mu, kappa, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def wald(self, mean, scale, size=None, chunks="auto"):
        return self._wrap("wald", mean, scale, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def weibull(self, a, size=None, chunks="auto"):
        return self._wrap("weibull", a, size=size, chunks=chunks)

    @derived_from(np.random.RandomState)
    def zipf(self, a, size=None, chunks="auto"):
        return self._wrap("zipf", a, size=size, chunks=chunks)


def _choice(state_data, a, size, replace, p):
    state = np.random.RandomState(state_data)
    return state.choice(a, size=size, replace=replace, p=p)


def _apply_random(RandomState, funcname, state_data, size, args, kwargs):
    """Apply RandomState method with seed"""
    if RandomState is None:
        RandomState = np.random.RandomState
    state = RandomState(state_data)
    func = getattr(state, funcname)
    return func(*args, size=size, **kwargs)


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
laplace = _state.laplace
logistic = _state.logistic
lognormal = _state.lognormal
logseries = _state.logseries
multinomial = _state.multinomial
negative_binomial = _state.negative_binomial
noncentral_chisquare = _state.noncentral_chisquare
noncentral_f = _state.noncentral_f
normal = _state.normal
pareto = _state.pareto
permutation = _state.permutation
poisson = _state.poisson
power = _state.power
rayleigh = _state.rayleigh
random_sample = _state.random_sample
random = random_sample
randint = _state.randint
random_integers = _state.random_integers
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
