import itertools
from toolz import merge, concat, frequencies, merge_with, take, curry
from ..threaded import get # TODO: get better get

names = ('bag-%d' % i for i in itertools.count(1))


class Item(object):
    def __init__(self, dsk, key):
        self.dask = dsk
        self.key = key

    def compute(self):
        return get(self.dask, self.key)

    __int__ = __float__ = __complex__ = __bool__ = compute


class Bag(object):
    """ Unordered collection with repeats

    Computed in paritions with dask

    >>> dsk = {('x', 0): (range, 5),
    ...        ('x', 1): (range, 5),
    ...        ('x', 2): (range, 5)}
    >>> b = Bag(dsk, 'x', npartitions=3)

    >>> sorted(b.map(lambda x: x * 10))
    [0, 0, 0, 10, 10, 10, 20, 20, 20, 30, 30, 30, 40, 40, 40]

    >>> int(b.fold(lambda x, y: x + y))
    30
    """
    def __init__(self, dsk, name, npartitions):
        self.dask = dsk
        self.name = name
        self.npartitions = npartitions

    def map(self, func):
        name = next(names)
        dsk = dict(((name, i), (map, func, (self.name, i)))
                        for i in range(self.npartitions))
        return Bag(merge(self.dask, dsk), name, self.npartitions)

    def filter(self, predicate):
        name = next(names)
        dsk = dict(((name, i), (filter, predicate, (self.name, i)))
                        for i in range(self.npartitions))
        return Bag(merge(self.dask, dsk), name, self.npartitions)

    def fold(self, binop, combine=None, initial=None):
        a = next(names)
        b = next(names)
        if initial:
            dsk = dict(((a, i), (reduce, binop, (self.name, i), initial))
                            for i in range(self.npartitions))
        else:
            dsk = dict(((a, i), (reduce, binop, (self.name, i)))
                            for i in range(self.npartitions))
        dsk2 = {b: (reduce, combine or binop, list(dsk.keys()))}
        return Item(merge(self.dask, dsk, dsk2), b)

    def frequencies(self):
        a = next(names)
        b = next(names)
        dsk = dict(((a, i), (frequencies, (self.name, i)))
                        for i in range(self.npartitions))
        dsk2 = {(b, 0): (dict.items,
                            (merge_with, sum, list(sorted(dsk.keys()))))}
        return Bag(merge(self.dask, dsk, dsk2), b, 1)


    def topk(self, k):
        a = next(names)
        b = next(names)
        rsorted = curry(sorted, reverse=True)
        dsk = dict(((a, i), (list, (take, k, (rsorted, (self.name, i)))))
                        for i in range(self.npartitions))
        dsk2 = {(b, 0): (list, (take, k, (rsorted, (concat, dsk.keys()))))}
        return Bag(merge(self.dask, dsk, dsk2), b, 1)

    def keys(self):
        return [(self.name, i) for i in range(self.npartitions)]

    def __iter__(self):
        return concat(get(self.dask, self.keys()))
