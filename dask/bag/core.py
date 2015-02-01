from __future__ import absolute_import, division, print_function

import itertools
import math
from collections import Iterable, Iterator
from toolz import (merge, concat, frequencies, merge_with, take, curry, reduce,
        join, reduceby, compose, second)
try:
    from cytoolz import (curry, frequencies, merge_with, join, reduceby,
            compose, second)
except ImportError:
    pass
from ..multiprocessing import get

names = ('bag-%d' % i for i in itertools.count(1))


class Item(object):
    def __init__(self, dsk, key, get=get):
        self.dask = dsk
        self.key = key
        self.get = get

    def compute(self):
        return self.get(self.dask, self.key)

    __int__ = __float__ = __complex__ = __bool__ = compute


class Bag(object):
    """ Unordered collection with repeats

    Computed in paritions with dask

    >>> dsk = {('x', 0): (range, 5),
    ...        ('x', 1): (range, 5),
    ...        ('x', 2): (range, 5)}
    >>> b = Bag(dsk, 'x', npartitions=3)

    >>> sorted(b.map(lambda x: x * 10))  # doctest: +SKIP
    [0, 0, 0, 10, 10, 10, 20, 20, 20, 30, 30, 30, 40, 40, 40]

    >>> int(b.fold(lambda x, y: x + y))  # doctest: +SKIP
    30
    """
    def __init__(self, dsk, name, npartitions, get=get):
        self.dask = dsk
        self.name = name
        self.npartitions = npartitions
        self.get = get

    def map(self, func):
        name = next(names)
        dsk = dict(((name, i), (list, (map, func, (self.name, i))))
                        for i in range(self.npartitions))
        return Bag(merge(self.dask, dsk), name, self.npartitions)

    def filter(self, predicate):
        name = next(names)
        dsk = dict(((name, i), (list, (filter, predicate, (self.name, i))))
                        for i in range(self.npartitions))
        return Bag(merge(self.dask, dsk), name, self.npartitions)

    def map_partitions(self, func):
        name = next(names)
        dsk = dict(((name, i), (func, (self.name, i)))
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
        dsk2 = {(b, 0): (dictitems,
                            (merge_with, sum, list(sorted(dsk.keys()))))}
        return Bag(merge(self.dask, dsk, dsk2), b, 1)


    def topk(self, k):
        a = next(names)
        b = next(names)
        rsorted = curry(sorted, reverse=True)
        dsk = dict(((a, i), (list, (take, k, (rsorted, (self.name, i)))))
                        for i in range(self.npartitions))
        dsk2 = {(b, 0): (list, (take, k, (rsorted, (concat, list(dsk.keys())))))}
        return Bag(merge(self.dask, dsk, dsk2), b, 1)


    def _reduction(self, perpartition, aggregate):
        a = next(names)
        b = next(names)
        dsk = dict(((a, i), (perpartition, (self.name, i)))
                        for i in range(self.npartitions))
        dsk2 = {b: (aggregate, list(dsk.keys()))}
        return Item(merge(self.dask, dsk, dsk2), b)

    def sum(self):
        return self._reduction(sum, sum)

    def max(self):
        return self._reduction(max, max)

    def min(self):
        return self._reduction(min, min)

    def any(self):
        return self._reduction(any, any)

    def all(self):
        return self._reduction(all, all)

    def count(self):
        return self._reduction(len, sum)

    def mean(self):
        def chunk(x):
            return sum(x), len(x)
        def agg(x):
            totals, counts = list(zip(*x))
            return 1.0 * sum(totals) / sum(counts)
        return self._reduction(chunk, agg)

    def var(self, ddof=0):
        def chunk(seq):
            return sum([x**2 for x in seq]), sum(seq), len(seq)
        def agg(x):
            squares, totals, counts = list(zip(*x))
            x2, x, n = float(sum(squares)), float(sum(totals)), sum(counts)
            result = (x2 / n) - (x / n)**2
            return result * n / (n - ddof)
        return self._reduction(chunk, agg)

    def std(self, ddof=0):
        return math.sqrt(self.var(ddof=ddof))

    def join(self, other, on_self, on_other=None):
        assert isinstance(other, Iterable)
        assert not isinstance(other, Bag)
        if on_other is None:
            on_other = on_self
        name = next(names)
        dsk = dict(((name, i), (list, (join, on_other, other,
                                             on_self, (self.name, i))))
                        for i in range(self.npartitions))
        return Bag(merge(self.dask, dsk), name, self.npartitions)

    def foldby(self, key, binop, initial=None, combine=None,
               combine_initial=None):
        a = next(names)
        b = next(names)
        if combine is None:
            combine = binop
        if initial and not combine_initial and not combine:
            combine_initial = initial
        if combine_initial is None:
            raise NotImplementedError("You must supply an initial value")
        if initial:
            dsk = dict(((a, i),
                        (reduceby, key, binop, (self.name, i), initial))
                        for i in range(self.npartitions))
        else:
            dsk = dict(((a, i),
                        (reduceby, key, binop, (self.name, i)))
                        for i in range(self.npartitions))
        combine2 = lambda acc, x: combine(acc, x[1])
        dsk2 = {(b, 0): (dictitems,
                          (reduceby,
                            0, combine2,
                            (concat, (map, dictitems, list(dsk.keys()))),
                            combine_initial))}
        return Bag(merge(self.dask, dsk, dsk2), b, 1)


    def keys(self):
        return [(self.name, i) for i in range(self.npartitions)]

    def __iter__(self):
        results = self.get(self.dask, self.keys())
        if isinstance(results[0], Iterable):
            results = concat(results)
        if not isinstance(results, Iterator):
            results = iter(results)
        return results


def dictitems(d):
    """ A pickleable version of dict.items """
    return list(d.items())


from glob import glob

def loadtext(globstring):
    """ Loads a collection of files into a Bag.  Takes a globstring

    >>> loadtext('all-my-files-*.log')  # doctest: +SKIP
    <Bag>
    """
    filenames = sorted(glob(globstring))
    d = {('load', i): (list, (open, fn)) for i, fn in enumerate(filenames)}
    return Bag(d, 'load', len(d))
