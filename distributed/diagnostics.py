import logging
import sys
import threading
import time
from timeit import default_timer

from tornado.ioloop import PeriodicCallback, IOLoop

from .utils import ignoring, sync
from .executor import default_executor


logger = logging.getLogger(__name__)


class Diagnostic(object):
    def start(self, scheduler):
        pass

    def task_finished(self, scheduler, key, worker, nbytes):
        pass

    def update_graph(self, scheduler, dsk, keys, restrictions):
        pass

    def task_erred(self, scheduler, key, worker, exception):
        pass


def incomplete_keys(keys, scheduler):
    """ All keys that need to compute for these keys to finish """
    out = set()
    stack = list(keys)
    while stack:
        key = stack.pop()
        if (key in out or
            scheduler.who_has.get(key) or
            key in scheduler.processing or
            key in scheduler.stacks):
            continue

        out.add(key)
        stack.extend(scheduler.waiting.get(key, []))
    return out


class ProgressBar(Diagnostic):
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1):
        keys = {k.key if hasattr(k, 'key') else k for k in keys}

        if scheduler is None:
            executor = default_executor()
            if executor is None:
                raise ValueError("Can not find scheduler.\n"
                 "Either create an executor or supply scheduler as an argument")
            else:
                scheduler = default_executor().scheduler

        start = time.time()
        while not keys.issubset(scheduler.dask):
            time.sleep(0.01)
            if time.time() > start + 1:
                raise ValueError("Keys not found: %s" % str(keys -
                    scheduler.in_play))

        self._start_time = default_timer()

        self._minimum = minimum
        self._dt = dt
        self.last_duration = 0
        self.keys = None

        self._running = False

        self.scheduler = scheduler

        def f():
            scheduler.add_diagnostic(self)  # subtle race condition here
            self.keys = incomplete_keys(keys, scheduler)
            self.all_keys = self.keys.copy()
            self.all_keys.update(keys)

        if scheduler.loop._thread_ident == threading.current_thread().ident:
            f()
        else:
            sync(scheduler.loop, f)

        self.status = None

    def task_finished(self, scheduler, key, worker, nbytes):
        if key in self.keys:
            self.keys.remove(key)

        if not self.keys:
            self.stop()

    def task_erred(self, scheduler, key, worker, exception):
        logger.info("Progressbar sees task erred")
        if key in self.all_keys:
            self.stop(exception=exception)

    def stop(self, exception=None):
        if self in self.scheduler.diagnostics:
            self.scheduler.diagnostics.remove(self)

        if exception:
            self.status = 'error'
        else:
            self.status = 'finished'

    @property
    def elapsed(self):
        return default_timer() - self._start_time


class TextProgressBar(ProgressBar):
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1, width=40):
        ProgressBar.__init__(self, keys, scheduler, minimum, dt)
        self._width = width

    def start(self):
        if not self._running:
            # Start background thread
            self._running = True
            self._timer = threading.Thread(target=self._timer_func)
            self._timer.daemon = True
            self._timer.start()

        self._update_bar()
        if not self.keys:
            self.stop()
        if all(k in self.scheduler.exceptions_blame for k in self.keys):
            self.stop(True)

    def stop(self, exception=None):
        ProgressBar.stop(self, exception)
        if self._running:
            self._running = False
            self._timer.join()
            self.last_duration = self.elapsed
            if self.last_duration < self._minimum:
                return
            else:
                self._update_bar()
            sys.stdout.write('\n')
            sys.stdout.flush()

    def _timer_func(self):
        """Background thread for updating the progress bar"""
        while self._running:
            if self.elapsed > self._minimum:
                self._update_bar()
            time.sleep(self._dt)

    def _update_bar(self):
        ntasks = len(self.all_keys)
        ndone = ntasks - len(self.keys)
        self._draw_bar(ndone / ntasks if ntasks else 1.0, self.elapsed)

    def _draw_bar(self, frac, elapsed):
        bar = '#' * int(self._width * frac)
        percent = int(100 * frac)
        elapsed = format_time(elapsed)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self._width,
                                                           percent, elapsed)
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()


class ProgressWidget(ProgressBar):
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1):
        ProgressBar.__init__(self, keys, scheduler, minimum, dt)
        from ipywidgets import FloatProgress
        self.bar = FloatProgress(min=0, max=1, description='Hello')
        from zmq.eventloop.ioloop import IOLoop
        loop = IOLoop.instance()
        self.pc = PeriodicCallback(self._update_bar, self._dt, io_loop=loop)

    def start(self):
        from IPython.display import display
        display(self.bar)
        self.pc.start()
        if not self.keys:
            self._update_bar()
            self.stop()

    def stop(self, exception=None):
        self.pc.stop()
        ProgressBar.stop(self, exception)
        if exception:
            self.bar.bar_style = 'danger'
        elif not self.keys:
            self.bar.bar_style = 'success'

    def _update_bar(self):
        ntasks = len(self.all_keys)
        ndone = ntasks - len(self.keys)
        self.bar.value = ndone / ntasks if ntasks else 1.0
        self.bar.description = format_time(self.elapsed)


def progress_bar(*args, **kwargs):
    notebook = kwargs.pop('notebook', None)
    if notebook is None:
        notebook = is_kernel()  # often but not always correct assumption
    if notebook:
        return WidgetProgressBar(*args, **kwargs)
    else:
        return TextProgressBar(*args, **kwargs)


def is_kernel():
    # http://stackoverflow.com/questions/34091701/determine-if-were-in-an-ipython-notebook-session
    if 'IPython' not in sys.modules:
        # IPython hasn't been imported, definitely not
        return False
    from IPython import get_ipython
    # check for `kernel` attribute on the IPython instance
    return getattr(get_ipython(), 'kernel', None) is not None



def format_time(t):
    """Format seconds into a human readable form.

    >>> format_time(10.4)
    '10.4s'
    >>> format_time(1000.4)
    '16min 40.4s'
    """
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    if h:
        return '{0:2.0f}hr {1:2.0f}min {2:4.1f}s'.format(h, m, s)
    elif m:
        return '{0:2.0f}min {1:4.1f}s'.format(m, s)
    else:
        return '{0:4.1f}s'.format(s)
