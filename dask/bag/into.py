from __future__ import absolute_import, division, print_function

from into import discover, convert, append, into, chunks, TextFile
from collections import Iterator
from .core import Bag


@convert.register(Iterator, Bag)
def bag_to_iterator(x, **kwargs):
    return iter(x)


@convert.register(Bag, chunks(TextFile))
def bag_to_iterator(x, **kwargs):
    d = dict((('load', i), (list, (open, fn.path))) for i, fn in enumerate(x))
    # d = dict((('load', i), (list, (into, Iterator, tf))) for i, tf in enumerate(x))
    return Bag(d, 'load', len(d))
