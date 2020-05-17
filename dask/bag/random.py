import random
from functools import partial


def choices(population, k=1):
    """
    Return a k sized list of population elements chosen with replacement.

    If the relative weights or cumulative weights are not specified,
    the selections are made with equal probability.

    Parameters
    ----------
    population : Bag
        Elements to sample.
    k : integer, optional
        Number of elements to sample.
    Examples
    --------
    >>> import dask.bag as db # doctest: +SKIP
    >>> from dask.bag import random # doctest: +SKIP
    >>>
    >>> b = db.from_sequence(range(5), npartitions=2) # doctest: +SKIP
    >>> list(random.choices(b, 3).compute()) # doctest: +SKIP
    [1, 3, 5]
    """
    return population.reduction(
        partial(_sample_map_partitions, k=k), partial(_sample_reduce, k=k)
    )


def _sample_map_partitions(population, k):
    """
    Map function used on the sample and choices functions.
    Parameters
    ----------
    population : list
        List of elements to sample.
    k : int, optional
        Number of elements to sample. Default is a single value is returned.

    Returns
    -------
    sample : list
        List of sampled elements from the partition.
    lx : int
        Number of elements on the partition.
    k : int
        Number of elements to sample.
    replace: boolean
        Whether the sample is with or without replacement.
    """
    lx = len(population)
    real_k = k if k <= lx else lx
    # because otherwise it raises IndexError:
    sampled = [] if real_k == 0 else random.choices(population, k=real_k)
    return sampled, lx


def _sample_reduce(reduce_iter, k):
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

    if k < 0:
        raise ValueError("Sample is negative")

    # creating the probability array
    p = []
    for n_i, k_i in ns_ks:
        if k_i > 0:
            p_i = n_i / (k_i * n)
            p += [p_i] * k_i

    return random.choices(population=s, weights=p, k=k)
