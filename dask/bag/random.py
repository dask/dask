import heapq
import math
import random as rnd
from functools import partial

from .core import Bag


def sample(population, k):
    """Chooses k unique random elements from a bag.

    Returns a new bag containing elements from the population while
    leaving the original population unchanged.

    Parameters
    ----------
    population: Bag
        Elements to sample.
    k: integer, optional
        Number of elements to sample.

    Examples
    --------
    >>> import dask.bag as db # doctest: +SKIP
    ... from dask.bag import random
    ...
    ... b = db.from_sequence(range(5), npartitions=2)
    ... list(random.sample(b, 3).compute())
    [1, 3, 5]
    """
    return _sample(population=population, k=k, replace=False)


def choices(population, k=1):
    """
    Return a k sized list of elements chosen with replacement.

    Parameters
    ----------
    population: Bag
        Elements to sample.
    k: integer, optional
        Number of elements to sample.

    Examples
    --------
    >>> import dask.bag as db # doctest: +SKIP
    ... from dask.bag import random
    ...
    ... b = db.from_sequence(range(5), npartitions=2)
    ... list(random.choices(b, 3).compute())
    [1, 1, 5]
    """
    return _sample(population=population, k=k, replace=True)


def _sample(population, k, replace=False):
    return population.reduction(
        partial(_sample_map_partitions, k=k, replace=replace),
        partial(_sample_reduce, k=k, replace=replace),
        out_type=Bag,
    )


def _sample_map_partitions(population, k, replace):
    """
    Map function used on the sample and choices functions.
    Parameters
    ----------
    population : list
        List of elements to sample.
    k : int, optional
        Number of elements to sample. Default is 1.

    Returns
    -------
    sample: list
        List of sampled elements from the partition.
    lx: int
        Number of elements on the partition.
    k: int
        Number of elements to sample.
    """
    population = list(population)
    lx = len(population)
    real_k = k if k <= lx else lx
    sample_func = rnd.choices if replace else rnd.sample
    # because otherwise it raises IndexError:
    sampled = [] if real_k == 0 else sample_func(population=population, k=real_k)
    return sampled, lx


def _sample_reduce(reduce_iter, k, replace):
    """
    Reduce function used on the sample and choice functions.

    Parameters
    ----------
    reduce_iter : iterable
        Each element is a tuple coming generated by the _sample_map_partitions function.

    Returns a sequence of uniformly distributed samples;
    """
    ns_ks = []
    s = []
    n = 0
    # unfolding reduce outputs
    for i in reduce_iter:
        (s_i, n_i) = i
        s.extend(s_i)
        n += n_i
        k_i = len(s_i)
        ns_ks.append((n_i, k_i))

    if k < 0 or (k > n and not replace):
        raise ValueError("Sample larger than population or is negative")

    # creating the probability array
    p = []
    for n_i, k_i in ns_ks:
        if k_i > 0:
            p_i = n_i / (k_i * n)
            p += [p_i] * k_i

    sample_func = rnd.choices if replace else _weighted_sampling_without_replacement
    return sample_func(population=s, weights=p, k=k)


def _weighted_sampling_without_replacement(population, weights, k):
    """
    Source:
        Weighted random sampling with a reservoir, Pavlos S. Efraimidis, Paul G. Spirakis
    """
    elt = [(math.log(rnd.random()) / weights[i], i) for i in range(len(weights))]
    return [population[x[1]] for x in heapq.nlargest(k, elt)]
