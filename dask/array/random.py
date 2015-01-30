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
    Random dask array uniform in [0, 1)

    >>> random(size=(1000, 1000), blockshape=(500, 500)).dask  # doctest: +SKIP
    {('rand_1', 0, 0): (np.random.random, 500, 500),
     ('rand_1', 0, 1): (np.random.random, 500, 500),
     ('rand_1', 1, 0): (np.random.random, 500, 500),
     ('rand_1', 1, 1): (np.random.random, 500, 500)}

    Handle uneven sizes

    >>> random(size=(1000, 1000), blockshape=(600, 600)).dask  # doctest: +SKIP
    {('rand_1', 0, 0): (np.random.random, 600, 600),
     ('rand_1', 0, 1): (np.random.random, 600, 400),
     ('rand_1', 1, 0): (np.random.random, 400, 600),
     ('rand_1', 1, 1): (np.random.random, 400, 400)}
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

random = wraps(np.random.random)(curry(generic, np.random.random))
beta = wraps(np.random.beta)(curry(generic, np.random.beta))
binomial = wraps(np.random.binomial)(curry(generic, np.random.binomial))
chisquare = wraps(np.random.chisquare)(curry(generic, np.random.chisquare))
dirichlet = wraps(np.random.dirichlet)(curry(generic, np.random.dirichlet))
exponential = wraps(np.random.exponential)(curry(generic, np.random.exponential))
f = wraps(np.random.f)(curry(generic, np.random.f))
gamma = wraps(np.random.gamma)(curry(generic, np.random.gamma))
geometric = wraps(np.random.geometric)(curry(generic, np.random.geometric))
gumbel = wraps(np.random.gumbel)(curry(generic, np.random.gumbel))
hypergeometric = wraps(np.random.hypergeometric)(curry(generic, np.random.hypergeometric))
