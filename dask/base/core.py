from collections import defaultdict
from operator import getitem, add
from datetime import datetime
from time import time
from ..core import istask, ishashable


class Base(object):
    """ DaskBase - A storage of data and computation

    Example
    -------

    DaskBase stores data like a dictionary

    >>> import dask
    >>> db = dask.Base()
    >>> db['x'] = 10
    >>> db['x']
    10

    DaskBase also stores computation on that data

    >>> db['y'] = (add, 'x', 5)

    Accessing these keys results in computations.  Results may be cached for
    reuse.

    >>> db['y']
    15

    Design
    ------

    A Base maintains the following state

    dsk: dict
        A dask to define all computation
    cache: dict-like
        Stores both ground data and cached intermediate values
    data: set
        The keys in the cache that can not be removed for correctness.
    compute_time: dict:: {key: float}
        dict mapping the time it took to compute each key
    access_times: dict:: {key: [datetimes]}
        The times at which a key was accessed
    """

    def __init__(self, cache=None):
        self.dsk = dict()
        if cache is None:
            cache = dict()
        self.cache = cache
        self.data = set()
        self.compute_time = dict()
        self.access_times = defaultdict(list)

    def __setitem__(self, key, value):
        if key in self.dsk:
            raise KeyError("Can not overwrite data")
        if istask(value):
            self.dsk[key] = value
        else:
            self.cache[key] = value
            self.dsk[key] = (getitem, self.cache, key)
            self.data.add(key)

    def __getitem__(self, key):
        if isinstance(key, list):
            return (self[item] for item in key)
        if not ishashable(key):
            return key
        if key not in self.dsk:
            return key

        self.access_times[key].append(datetime.now())

        if key in self.cache:
            return self.cache[key]

        task = self.dsk[key]
        func, args = task[0], task[1:]

        if func == getitem and args[0] is self.cache:
            return self.cache[args[1]]

        args = [self[arg] for arg in args]

        start = time()
        result = func(*args)
        end = time()

        self.cache[key] = result
        self.compute_time[key] = end - start

        return result
