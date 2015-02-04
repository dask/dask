from __future__ import absolute_import, division, print_function

from into import discover, convert, append, into, chunks, TextFile
from into.utils import keywords
from collections import Iterator
from .core import Bag


@convert.register(Iterator, Bag)
def bag_to_iterator(x, **kwargs):
    return iter(x)


@convert.register(Bag, chunks(TextFile))
def bag_to_iterator(x, **kwargs):
    return Bag.from_filenames([tf.path for tf in x])


@convert.register(Bag, list)
def bag_to_iterator(x, **kwargs):
    keys = keywords(Bag.from_sequence)
    kwargs2 = dict((k, v) for k, v in kwargs.items() if k in keys)
    return Bag.from_sequence(x, **kwargs2)
