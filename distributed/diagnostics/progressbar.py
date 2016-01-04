from ..executor import default_executor
from tornado import gen
from tornado.ioloop import IOLoop
from ..core import connect, read, write
from ..utils import sync, ignoring
from timeit import default_timer
from .progress import format_time, Progress
import sys


class TextProgressBar(object):
    def __init__(self, keys, scheduler=None, minimum=0, interval=0.1, width=40,
            loop=None, complete=True, start=True):
        if scheduler is None:
            scheduler = default_executor().scheduler
            self.scheduler = scheduler.ip, scheduler.port
        else:
            if isinstance(scheduler, str):
                scheduler = scheduler.split(':')
            self.scheduler = scheduler

        self.keys = [k.key if hasattr(k, 'key') else k for k in keys]
        self.minimum = minimum
        self.interval = interval
        self.width = width
        self.complete = complete
        self._start_time = default_timer()
        self.loop = loop or IOLoop()

        if start:
            sync(self.loop, self.listen)

    @property
    def elapsed(self):
        return default_timer() - self._start_time

    @gen.coroutine
    def listen(self):
        complete = self.complete
        keys = self.keys

        def setup(scheduler):
            p = Progress(keys, scheduler, complete=complete)
            scheduler.add_plugin(p)
            return p

        def func(scheduler, p):
            return {'all': len(p.all_keys),
                    'left': len(p.keys),
                    'status': p.status}

        self.stream = yield connect(*self.scheduler)

        yield write(self.stream, {'op': 'feed',
                                  'setup': setup,
                                  'function': func,
                                  'interval': self.interval})

        while True:
            self._last_response = response = yield read(self.stream)
            self.status = response['status']
            self._draw_bar((1 - response['left'] / response['all'])
                           if response['all'] else 1.0, self.elapsed)
            if response['status'] in ('error', 'finished'):
                self.stream.close()
                break

    def _draw_bar(self, frac, elapsed):
        bar = '#' * int(self.width * frac)
        percent = int(100 * frac)
        elapsed = format_time(elapsed)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self.width,
                                                           percent, elapsed)
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()
