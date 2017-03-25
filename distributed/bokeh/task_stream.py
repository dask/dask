from __future__ import print_function, division, absolute_import

import logging
import random

from bokeh.palettes import viridis
from toolz import memoize
import itertools

from ..diagnostics.plugin import SchedulerPlugin
from ..utils import key_split


logger = logging.getLogger(__name__)


class TaskStreamPlugin(SchedulerPlugin):
    def __init__(self, scheduler):
        self.buffer = []
        scheduler.add_plugin(self)
        self.index = 0
        self.maxlen = 100000

    def transition(self, key, start, finish, *args, **kwargs):
        if start == 'processing':
            kwargs['key'] = key
            if finish == 'memory' or finish == 'erred':
                self.buffer.append(kwargs)
                self.index += 1
                if len(self.buffer) > self.maxlen:
                    self.buffer = self.buffer[len(self.buffer):]

    def rectangles(self, istart, istop=None, workers=None):
        L_start = []
        L_duration = []
        L_key = []
        L_name = []
        L_color = []
        L_alpha = []
        L_worker = []
        L_worker_thread = []
        L_y = []

        diff = self.index - len(self.buffer)
        for msg in self.buffer[istart - diff: istop - diff if istop else istop]:
            key = msg['key']
            name = key_split(key)
            startstops = msg.get('startstops', [])
            try:
                worker_thread = '%s-%d' % (msg['worker'], msg['thread'])
            except Exception:
                continue
                logger.warning("Message contained bad information: %s", msg,
                               exc_info=True)
                worker_thread = ''

            if worker_thread not in workers:
                workers[worker_thread] = len(workers) / 2

            for action, start, stop in startstops:
                color = colors[action]
                if type(color) is not str:
                    color = color(msg)

                L_start.append((start + stop) / 2 * 1000)
                L_duration.append(1000 * (stop - start))
                L_key.append(key)
                L_name.append(prefix[action] + name)
                L_color.append(color)
                L_alpha.append(alphas[action])
                L_worker.append(msg['worker'])
                L_worker_thread.append(worker_thread)
                L_y.append(workers[worker_thread])

        return {'start': L_start,
                 'duration': L_duration,
                 'key': L_key,
                 'name': L_name,
                 'color': L_color,
                 'alpha': L_alpha,
                 'worker': L_worker,
                 'worker_thread': L_worker_thread,
                 'y': L_y}


def color_of_message(msg):
    if msg['status'] == 'OK':
        split = key_split(msg['key'])
        return color_of(split)
    else:
        return 'black'


colors = {'transfer': 'red',
          'disk-write': 'orange',
          'disk-read': 'orange',
          'deserialize': 'gray',
          'compute': color_of_message}


alphas = {'transfer': 0.4,
          'compute': 1,
          'deserialize': 0.4,
          'disk-write': 0.4,
          'disk-read': 0.4}


prefix = {'transfer': 'transfer-',
          'disk-write': 'disk-write-',
          'disk-read': 'disk-read-',
          'deserialize': 'deserialize-',
          'compute': ''}


counter = itertools.count()


@memoize
def incrementing_index(o):
    return next(counter)


task_stream_palette = list(viridis(25))
random.shuffle(task_stream_palette)


def color_of(o, palette=task_stream_palette):
    return palette[incrementing_index(o) % len(palette)]
