from __future__ import print_function, division, absolute_import

import itertools
import logging
import random

from bokeh.palettes import viridis
from toolz import valmap, merge, memoize
from tornado import gen

from .progress import AllProgress

from ..core import connect, write, coerce_to_address
from ..scheduler import Scheduler
from ..utils import key_split
from ..worker import dumps_function


logger = logging.getLogger(__name__)

task_stream_palette = list(viridis(25))
random.shuffle(task_stream_palette)

def counts(scheduler, allprogress):
    return merge({'all': valmap(len, allprogress.all),
                  'nbytes': allprogress.nbytes},
                 {state: valmap(len, allprogress.state[state])
                     for state in ['memory', 'erred', 'released']})


counter = itertools.count()
@memoize
def incrementing_index(o):
    return next(counter)


def color_of(o, palette=task_stream_palette):
    return palette[incrementing_index(o) % len(palette)]


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

    if not total:
        return d

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
        d['color'].append(color_of(name))
        d['name'].append(name)
        if right - left > 0.1:
            d['text'].append(name)
        else:
            d['text'].append('')

    return d


def progress_quads(msg, nrows=8, ncols=3):
    """

    >>> msg = {'all': {'inc': 5, 'dec': 1, 'add': 4},
    ...        'memory': {'inc': 2, 'dec': 0, 'add': 1},
    ...        'erred': {'inc': 0, 'dec': 1, 'add': 0},
    ...        'released': {'inc': 1, 'dec': 0, 'add': 1}}

    >>> progress_quads(msg, nrows=2)  # doctest: +SKIP
    {'name': ['inc', 'add', 'dec'],
     'left': [0, 0, 1],
     'right': [0.9, 0.9, 1.9],
     'top': [0, -1, 0],
     'bottom': [-.8, -1.8, -.8],
     'released': [1, 1, 0],
     'memory': [2, 1, 0],
     'erred': [0, 0, 1],
     'done': ['3 / 5', '2 / 4', '1 / 1'],
     'released-loc': [.2/.9, .25 / 0.9, 1],
     'memory-loc': [3 / 5 / .9, .5 / 0.9, 1],
     'erred-loc': [3 / 5 / .9, .5 / 0.9, 1.9]}
    """
    width = 0.9
    names = sorted(msg['all'], key=msg['all'].get, reverse=True)
    names = names[:nrows * ncols]
    n = len(names)
    d = {k: [v.get(name, 0) for name in names] for k, v in msg.items()}

    d['name'] = names
    d['show-name'] = [name if len(name) <= 15 else name[:12] + '...'
                      for name in names]
    d['left'] = [i // nrows for i in range(n)]
    d['right'] = [i // nrows + width for i in range(n)]
    d['top'] = [-(i % nrows) for i in range(n)]
    d['bottom'] = [-(i % nrows) - 0.8 for i in range(n)]
    d['color'] = [color_of(name) for name in names]

    d['released-loc'] = []
    d['memory-loc'] = []
    d['erred-loc'] = []
    d['done'] = []
    for r, m, e, a, l in zip(d['released'], d['memory'],
                             d['erred'], d['all'], d['left']):
        rl = width * r / a + l
        ml = width * (r + m) / a + l
        el = width * (r + m + e) / a + l
        done = '%d / %d' % (r + m + e, a)
        d['released-loc'].append(rl)
        d['memory-loc'].append(ml)
        d['erred-loc'].append(el)
        d['done'].append(done)

    return d

def task_stream_append(lists, msg, workers, palette=task_stream_palette):
    start, stop = msg['compute_start'], msg['compute_stop']
    lists['start'].append((start + stop) / 2 * 1000)
    lists['duration'].append(1000 * (stop - start))
    key = msg['key']
    name = key_split(key)
    if msg['status'] == 'OK':
        color = color_of(name, palette=palette)
    else:
        color = 'black'
    lists['key'].append(key)
    lists['name'].append(name)
    lists['color'].append(color)
    lists['alpha'].append(1)
    lists['worker'].append(msg['worker'])

    worker_thread = '%s-%d' % (msg['worker'], msg['thread'])
    lists['worker_thread'].append(worker_thread)
    if worker_thread not in workers:
        workers[worker_thread] = len(workers)
    lists['y'].append(workers[worker_thread])

    count = 1

    if (msg.get('transfer_start') is not None and
        msg['transfer_stop'] > (start - 1)):
        start, stop = msg['transfer_start'], msg['transfer_stop']
        lists['start'].append((start + stop) / 2 * 1000)
        lists['duration'].append(1000 * (stop - start))

        lists['key'].append(key)
        lists['name'].append('transfer-to-' + name)
        lists['worker'].append(msg['worker'])
        lists['color'].append('#FF0020')
        lists['alpha'].append('0.4')
        lists['worker_thread'].append(worker_thread)
        lists['y'].append(workers[worker_thread])
        count += 1

    if msg.get('disk_load_start') is not None:
        start, stop = msg['disk_load_start'], msg['disk_load_stop']
        lists['start'].append((start + stop) / 2 * 1000)
        lists['duration'].append(1000 * (stop - start))

        lists['key'].append(key)
        lists['name'].append('disk-load-' + name)
        lists['worker'].append(msg['worker'])
        lists['color'].append('#FF2000')
        lists['alpha'].append('0.4')
        lists['worker_thread'].append(worker_thread)
        lists['y'].append(workers[worker_thread])
        count += 1

    return count
