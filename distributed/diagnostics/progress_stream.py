from __future__ import print_function, division, absolute_import

import logging

from toolz import valmap, merge
from tornado import gen

from .progress import AllProgress

from ..core import connect, write, coerce_to_address
from ..scheduler import Scheduler
from ..worker import dumps_function


logger = logging.getLogger(__name__)


def counts(scheduler, allprogress):
    return merge({'all': valmap(len, allprogress.all),
                  'nbytes': allprogress.nbytes},
                 {state: valmap(len, allprogress.state[state])
                     for state in ['memory', 'erred', 'released']})


@gen.coroutine
def progress_stream(address, interval):
    """ Open a TCP connection to scheduler, receive progress messages

    The messages coming back are dicts containing counts of key groups::

        {'inc': {'all': 5, 'memory': 2, 'erred': 0, 'released': 1},
         'dec': {'all': 1, 'memory': 0, 'erred': 0, 'released': 0}}

    Parameters
    ----------
    address: address of scheduler
    interval: time between batches, in seconds

    Examples
    --------
    >>> stream = yield eventstream('127.0.0.1:8786', 0.100)  # doctest: +SKIP
    >>> print(yield read(stream))  # doctest: +SKIP
    """
    ip, port = coerce_to_address(address, out=tuple)
    stream = yield connect(ip, port)
    yield write(stream, {'op': 'feed',
                         'setup': dumps_function(AllProgress),
                         'function': dumps_function(counts),
                         'interval': interval,
                         'teardown': dumps_function(Scheduler.remove_plugin)})
    raise gen.Return(stream)


def nbytes_bar(nbytes):
    """ Convert nbytes message into rectangle placements

    >>> nbytes_bar({'inc': 1000, 'dec': 3000}) # doctest: +NORMALIZE_WHITESPACE
    {'names': ['dec', 'inc'],
     'left': [0, 0.75],
     'center': [0.375, 0.875],
     'right': [0.75, 1.0]}
    """
    total = sum(nbytes.values())
    names = sorted(nbytes)

    d = {'name': [],
         'text': [],
         'left': [],
         'right': [],
         'center': [],
         'color': [],
         'percent': [],
         'MB': []}
    right = 0
    for name in names:
        left = right
        right = nbytes[name] / total + left
        center = (right + left) / 2
        d['MB'].append(nbytes[name] / 1000000)
        d['percent'].append(round(nbytes[name] / total * 100, 2))
        d['left'].append(left)
        d['right'].append(right)
        d['center'].append(center)
        d['color'].append(task_stream_palette[incrementing_index(name)])
        d['name'].append(name)
        if right - left > 0.1:
            d['text'].append(name)
        else:
            d['text'].append('')

    return d


def progress_quads(msg):
    """

    Consumes messages like the following::

          {'all': {'inc': 5, 'dec': 1},
           'memory': {'inc': 2, 'dec': 0},
           'erred': {'inc': 0, 'dec': 1},
           'released': {'inc': 1, 'dec': 0}}

    """
    names = sorted(msg['all'], key=msg['all'].get, reverse=True)
    d = {k: [v.get(name, 0) for name in names] for k, v in msg.items()}
    d['name'] = names
    d['top'] = [i + 0.7 for i in range(len(names))]
    d['center'] = [i + 0.5 for i in range(len(names))]
    d['bottom'] = [i + 0.3 for i in range(len(names))]
    d['released_right'] = [r / a for r, a in zip(d['released'], d['all'])]
    d['memory_right'] = [(r + im) / a for r, im, a in
            zip(d['released'], d['memory'], d['all'])]
    d['fraction'] = ['%d / %d' % (im + r, a)
                   for im, r, a in zip(d['memory'], d['released'], d['all'])]
    d['erred_left'] = [1 - e / a for e, a in zip(d['erred'], d['all'])]
    return d


from toolz import memoize
from bokeh.palettes import Spectral11, Spectral9, viridis
import random
task_stream_palette = list(viridis(25))
random.shuffle(task_stream_palette)

import itertools
counter = itertools.count()
@memoize
def incrementing_index(o):
    return next(counter)
