from __future__ import print_function, division, absolute_import

import logging

from toolz import valmap
from tornado import gen

from .progress import AllProgress

from ..core import connect, write, coerce_to_address
from ..scheduler import Scheduler
from ..worker import dumps_function


logger = logging.getLogger(__name__)


def counts(scheduler, allprogress):
    return {'all': valmap(len, allprogress.all),
            'in_memory': valmap(len, allprogress.in_memory),
            'released': valmap(len, allprogress.released),
            'erred': valmap(len, allprogress.erred)}


@gen.coroutine
def progress_stream(address, interval):
    """ Open a TCP connection to scheduler, receive progress messages

    The messages coming back are dicts containing counts of key groups::

        {'inc': {'all': 5, 'in_memory': 2, 'erred': 0, 'released': 1},
         'dec': {'all': 1, 'in_memory': 0, 'erred': 0, 'released': 0}}

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


def progress_quads(msg):
    """

    Consumes messages like the following::

          {'all': {'inc': 5, 'dec': 1},
           'in_memory': {'inc': 2, 'dec': 0},
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
    d['in_memory_right'] = [(r + im) / a for r, im, a in
            zip(d['released'], d['in_memory'], d['all'])]
    d['fraction'] = ['%d / %d' % (im + r, a)
                   for im, r, a in zip(d['in_memory'], d['released'], d['all'])]
    d['erred_left'] = [1 - e / a for e, a in zip(d['erred'], d['all'])]
    return d
