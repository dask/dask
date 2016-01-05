from ..executor import default_executor
from tornado import gen
from tornado.ioloop import IOLoop
from ..core import connect, read, write
from ..utils import sync, ignoring
from timeit import default_timer
from .progress import format_time, Progress
import sys


def get_scheduler(scheduler):
    if scheduler is None:
        scheduler = default_executor().scheduler
        return scheduler.ip, scheduler.port
    if isinstance(scheduler, str):
        scheduler = scheduler.split(':')
    if isinstance(scheduler, tuple):
        return scheduler
    raise TypeError("Expected None, string, or tuple")


class ProgressBar(object):
    def __init__(self, keys, scheduler=None, interval=0.1, complete=True):
        self.scheduler = get_scheduler(scheduler)

        self.keys = {k.key if hasattr(k, 'key') else k for k in keys}
        self.interval = interval
        self.complete = complete
        self._start_time = default_timer()

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
                    'remaining': len(p.keys),
                    'status': p.status}

        self.stream = yield connect(*self.scheduler)

        yield write(self.stream, {'op': 'feed',
                                  'setup': setup,
                                  'function': func,
                                  'interval': self.interval})

        while True:
            self._last_response = response = yield read(self.stream)
            self.status = response['status']
            self._draw_bar(**response)
            if response['status'] in ('error', 'finished'):
                self.stream.close()
                self._draw_stop(**response)
                break

    def _draw_stop(self, **kwargs):
        pass


class TextProgressBar(ProgressBar):
    def __init__(self, keys, scheduler=None, interval=0.1, width=40,
                       loop=None, complete=True, start=True):
        super(TextProgressBar, self).__init__(keys, scheduler, interval,
                                              complete)
        self.width = width
        self.loop = loop or IOLoop()

        if start:
            sync(self.loop, self.listen)

    def _draw_bar(self, remaining, all, **kwargs):
        frac = (1 - remaining / all) if all else 1.0
        bar = '#' * int(self.width * frac)
        percent = int(100 * frac)
        elapsed = format_time(self.elapsed)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self.width,
                                                           percent, elapsed)
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()


class ProgressWidget(ProgressBar):
    """ ProgressBar that uses an IPython ProgressBar widget for the notebook

    See Also
    --------
    progress: User function
    Progress: Super class with most of the logic
    TextProgressBar: Text version suitable for the console
    """
    def __init__(self, keys, scheduler=None, interval=0.1,
                complete=False, loop=None, start=True):
        super(ProgressWidget, self).__init__(keys, scheduler, interval,
                                             complete)

        from ipywidgets import FloatProgress, HBox, VBox, HTML
        self.elapsed_time = HTML('')
        self.bar = FloatProgress(min=0, max=1, description='', height = '10px')
        self.bar_text = HTML('', width = "140px")

        self.bar_widget = HBox([ self.bar_text, self.bar ])
        self.widget = VBox([self.elapsed_time, self.bar_widget])

    def _ipython_display_(self, **kwargs):
        IOLoop.current().add_callback(self.listen)
        return self.widget._ipython_display_(**kwargs)

    def _draw_stop(self, remaining, status, **kwargs):
        if status == 'error':
            self.bar.bar_style = 'danger'
            self.elapsed_time.value = '<div style="padding: 0px 10px 5px 10px"><b>Warning:</b> the computation terminated due to an error after ' + format_time(self.elapsed) + '</div>'
        elif not remaining:
            self.bar.bar_style = 'success'

    def _draw_bar(self, remaining, all, **kwargs):
        ndone = all - remaining
        self.elapsed_time.value = '<div style=\"padding: 0px 10px 5px 10px\"><b>Elapsed time:</b> ' + format_time(self.elapsed) + '</div>'
        self.bar.value = ndone / all if all else 1.0
        self.bar_text.value = '<div style="padding: 0px 10px 0px 10px; text-align:right;">%d / %d</div>' % (ndone, all)
