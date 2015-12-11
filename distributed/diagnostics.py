import logging
import sys
import threading
import time
from timeit import default_timer

import dask
from toolz import valmap, groupby, concat
from tornado.ioloop import PeriodicCallback, IOLoop

from .utils import ignoring, sync
from .executor import default_executor


logger = logging.getLogger(__name__)


class SchedulerPlugin(object):
    """ Interface to extend the Scheduler

    The scheduler operates by triggering and responding to events like
    ``task_finished``, ``update_graph``, ``task_erred``, etc..

    A plugin enables custom code to run at each of those same events.  The
    scheduler will run the analagous methods on this class when each event is
    triggered.  This runs user code within the scheduler thread that can
    perform arbitrary operations in synchrony with the scheduler itself.

    Plugins are often used for diagnostics and measurement, but have full
    access to the scheduler and could in principle affect core scheduling.

    To implement a plugin implement some of the methods of this class and add
    the plugin to the scheduler with ``Scheduler.add_plugin(myplugin)``.

    Examples
    --------
    >>> class Counter(SchedulerPlugin):
    ...     def __init__(self):
    ...         self.counter = 0
    ...
    ...     def task_finished(self, scheduler, key, worker, nbytes):
    ...         self.counter += 1
    ...
    ...     def restart(self, scheduler):
    ...         self.counter = 0

    >>> c = Counter()
    >>> scheduler.add_plugin(c)  # doctest: +SKIP
    """
    def task_finished(self, scheduler, key, worker, nbytes):
        """ Run when a task is reported complete """
        pass

    def update_graph(self, scheduler, dsk, keys, restrictions):
        """ Run when a new graph / tasks enter the scheduler """
        pass

    def task_erred(self, scheduler, key, worker, exception):
        """ Run when a task is reported failed """
        pass

    def restart(self, scheduler):
        """ Run when the scheduler restarts itself """
        pass


def dependent_keys(keys, who_has, processing, stacks, dependencies, exceptions, complete=False):
    """ All keys that need to compute for these keys to finish """
    out = set()
    errors = set()
    stack = list(keys)
    while stack:
        key = stack.pop()
        if key in out:
            continue
        if not complete and (who_has.get(key) or
                             key in processing or
                             key in stacks):
            continue
        if key in exceptions:
            errors.add(key)
            if not complete:
                continue

        out.add(key)
        stack.extend(dependencies.get(key, []))
    return out, errors


class Progress(SchedulerPlugin):
    """ Tracks progress of a set of keys or futures

    On creation we provide a set of keys or futures that interest us as well as
    a scheduler.  We traverse through the scheduler's depenedencies to find all
    relevent keys on which our keys depend.  We then plug into the scheduler to
    learn when our keys become available in memory at which point we record
    their completion.

    State
    -----
    keys: set
        Set of keys that are not yet computed
    all_keys: set
        Set of all keys that we track

    This class performs no visualization.  However it is used by other classes,
    notably TextProgressBar and ProgressWidget, which do perform visualization.
    """
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1, complete=False):
        keys = {k.key if hasattr(k, 'key') else k for k in keys}
        self.setup_pre(keys, scheduler, minimum, dt, complete)

        def clear_errors(errors):
            for k in errors:
                self.task_erred(None, k, None, True)

        if self.scheduler.loop._thread_ident == threading.current_thread().ident:
            errors = self.setup(keys, complete)
        else:
            errors = sync(self.scheduler.loop, self.setup, keys, complete)
        clear_errors(errors)

    def setup(self, keys, complete):
        self.scheduler.add_plugin(self)  # subtle race condition here
        self.all_keys, errors = dependent_keys(keys, self.scheduler.who_has,
                self.scheduler.processing, self.scheduler.stacks,
                self.scheduler.dependencies, self.scheduler.exceptions,
                complete=complete)
        if not complete:
            self.keys = self.all_keys.copy()
        else:
            self.keys, _ = dependent_keys(keys, self.scheduler.who_has,
                    self.scheduler.processing, self.scheduler.stacks,
                    self.scheduler.dependencies, self.scheduler.exceptions,
                    complete=False)
        self.all_keys.update(keys)
        self.keys |= errors & self.all_keys

        logger.debug("Set up Progress keys")
        return errors

    def setup_pre(self, keys, scheduler, minimum, dt, complete):
        if scheduler is None:
            executor = default_executor()
            if executor is None:
                raise ValueError("Can not find scheduler.\n"
                 "Either create an executor or supply scheduler as an argument")
            else:
                scheduler = default_executor().scheduler

        while not keys.issubset(scheduler.dask):
            time.sleep(0.05)

        self.scheduler = scheduler

        self._start_time = default_timer()

        self._minimum = minimum
        self._dt = dt
        self.last_duration = 0
        self.keys = None

        self._running = False
        self.status = None

    def start(self):
        self.status = 'running'
        logger.debug("Start Progress Plugin")
        self._start()
        if any(k in self.scheduler.exceptions_blame for k in self.all_keys):
            self.stop(True)
        elif not self.keys:
            self.stop()

    def _start(self):
        pass

    def task_finished(self, scheduler, key, worker, nbytes):
        logger.debug("Progress sees key %s", key)
        if key in self.keys:
            self.keys.remove(key)

        if not self.keys:
            self.stop()

    def task_erred(self, scheduler, key, worker, exception):
        logger.debug("Progress sees task erred")
        if key in self.all_keys:
            self.stop(exception=exception, key=key)

    def restart(self, scheduler):
        self.stop()

    def stop(self, exception=None, key=None):
        if self in self.scheduler.plugins:
            self.scheduler.plugins.remove(self)
        if exception:
            self.status = 'error'
        else:
            self.status = 'finished'
        logger.debug("Remove Progress plugin")

    @property
    def elapsed(self):
        return default_timer() - self._start_time


def key_split(s):
    """
    >>> key_split('x-1')
    'x'
    >>> key_split('x-1-2-3')
    'x'
    >>> key_split(('x-2', 1))
    'x'
    >>> key_split(None)
    'Other'
    """
    if isinstance(s, tuple):
        return key_split(s[0])
    try:
        return s.split('-', 1)[0]
    except:
        return 'Other'


class MultiProgress(Progress):
    """ Progress variant that keeps track of different groups of keys

    See Progress for most details.  This only adds a function ``func=``
    that splits keys.  This defaults to ``key_split`` which aligns with naming
    conventions chosen in the dask project (tuples, hyphens, etc..)

    State
    -----
    keys: dict
        Maps group name to set of not-yet-complete keys for that group
    all_keys: dict
        Maps group name to set of all keys for that group

    Examples
    --------
    >>> split = lambda s: s.split('-')[0]
    >>> p = MultiProgress(['y-2'], func=split)  # doctest: +SKIP
    >>> p.keys   # doctest: +SKIP
    {'x': {'x-1', 'x-2', 'x-3'},
     'y': {'y-1', 'y-2'}}
    """
    def __init__(self, keys, scheduler=None, func=key_split, minimum=0, dt=0.1, complete=False):
        self.func = func
        Progress.__init__(self, keys, scheduler, minimum=minimum, dt=dt,
                            complete=complete)

    def setup(self, keys, complete):
        errors = Progress.setup(self, keys, complete)

        # Group keys by func name
        self.keys = valmap(set, groupby(self.func, self.keys))
        self.all_keys = valmap(set, groupby(self.func, self.all_keys))
        for k in self.all_keys:
            if k not in self.keys:
                self.keys[k] = set()

        logger.debug("Set up Progress keys")
        return errors

    def task_finished(self, scheduler, key, worker, nbytes):
        s = self.keys.get(self.func(key), None)
        if s and key in s:
            s.remove(key)

        if not self.keys or not any(self.keys.values()):
            self.stop()

    def task_erred(self, scheduler, key, worker, exception):
        logger.debug("Progress sees task erred")

        if (self.func(key) in self.all_keys and
            key in self.all_keys[self.func(key)]):
            self.stop(exception=exception, key=key)

    def start(self):
        self.status = 'running'
        logger.debug("Start Progress Plugin")
        self._start()
        if not self.keys or not any(v for v in self.keys.values()):
            self.stop()
        elif all(k in self.scheduler.exceptions_blame for k in
                concat(self.keys.values())):
            key = next(k for k in concat(self.keys.values()) if k in
                    self.scheduler.exceptions_blame)
            self.stop(exception=True, key=key)


class TextProgressBar(Progress):
    """ ProgressBar that emits result to stdout

    This is suitable for console use.  See ``Progress`` for more information

    See Also
    --------
    progress: User function
    Progress: Super class with most of the logic
    ProgressWidget: Widget version suitable for the notebook
    """
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1, width=40,
            complete=False):
        self._width = width
        self._timer = None
        Progress.__init__(self, keys, scheduler, minimum, dt, complete=complete)

    def _start(self):
        if not self._running:
            # Start background thread
            self._running = True
            self._timer = threading.Thread(target=self._timer_func)
            self._timer.daemon = True
            self._timer.start()

        self._update()

    def stop(self, exception=None, key=None, timeout=10):
        Progress.stop(self, exception, key=None)
        self._update()
        if self._running:
            self._running = False
            self._timer.join(timeout=timeout)
            self.last_duration = self.elapsed
            if self.last_duration < self._minimum:
                return
            else:
                self._update()
            sys.stdout.write('\n')
            sys.stdout.flush()

    def _timer_func(self):
        """Background thread for updating the progress bar"""
        while self._running:
            if self.elapsed > self._minimum:
                self._update()
            time.sleep(self._dt)

    def _update(self):
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


class ProgressWidget(Progress):
    """ ProgressBar that uses an IPython ProgressBar widget for the notebook

    See Also
    --------
    progress: User function
    Progress: Super class with most of the logic
    TextProgressBar: Text version suitable for the console
    """
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1, complete=False):
        keys = {k.key if hasattr(k, 'key') else k for k in keys}
        self.setup_pre(keys, scheduler, minimum, dt, complete)

        def clear_errors(errors):
            for k in errors:
                self.task_erred(None, k, None, True)

        if self.scheduler.loop._thread_ident == threading.current_thread().ident:
            errors = self.setup(keys, complete)
        else:
            errors = sync(self.scheduler.loop, self.setup, keys, complete)

        self.pc = PeriodicCallback(self._update, 1000 * self._dt)

        from ipywidgets import FloatProgress, HBox, VBox, HTML
        self.elapsed_time = HTML('')
        self.bar = FloatProgress(min=0, max=1, description='', height = '10px')
        self.bar_text = HTML('', width = "140px")

        self.bar_widget = HBox([ self.bar_text, self.bar ])
        self.widget = VBox([self.elapsed_time, self.bar_widget])

        clear_errors(errors)

        self.pc.start()

    def setup(self, keys, complete):
        errors = Progress.setup(self, keys, complete)
        return errors

    def _ipython_display_(self, **kwargs):
        return self.widget._ipython_display_(**kwargs)

    def _start(self):
        return self._update()

    def stop(self, exception=None, key=None):
        Progress.stop(self, exception, key=None)
        with ignoring(AttributeError):
            self.pc.stop()
        self._update()
        if exception:
            self.bar.bar_style = 'danger'
            self.elapsed_time.value = '<div style="padding: 0px 10px 5px 10px"><b>Warning:</b> the computation terminated due to an error after ' + format_time(self.elapsed) + '</div>'
        elif not self.keys:
            self.bar.bar_style = 'success'

    def _update(self):
        ntasks = len(self.all_keys)
        ndone = ntasks - len(self.keys)
        self.elapsed_time.value = '<div style=\"padding: 0px 10px 5px 10px\"><b>Elapsed time:</b> ' + format_time(self.elapsed) + '</div>'
        self.bar.value = ndone / ntasks if ntasks else 1.0
        self.bar_text.value = '<div style="padding: 0px 10px 0px 10px; text-align:right;">%d / %d</div>' % (ndone, ntasks)

class MultiProgressWidget(MultiProgress):
    """ Multiple progress bar Widget suitable for the notebook

    Displays multiple progress bars for a computation, split on computation
    type.

    See Also
    --------
    progress: User-level function <--- use this
    MultiProgress: Non-visualization component that contains most logic
    ProgressWidget: Single progress bar widget
    """
    def __init__(self, keys, scheduler=None, minimum=0, dt=0.1, func=key_split,
                 complete=False):
        self.func = func
        keys = {k.key if hasattr(k, 'key') else k for k in keys}
        self.setup_pre(keys, scheduler, minimum, dt, complete)

        def clear_errors(errors):
            for k in errors:
                self.task_erred(None, k, None, True)

        # Get keys and all-keys
        if self.scheduler.loop._thread_ident == threading.current_thread().ident:
            errors = self.setup(keys, complete)
        else:
            errors = sync(self.scheduler.loop, self.setup, keys, complete)

        # Set up widgets
        from ipywidgets import FloatProgress, HBox, VBox, HTML
        import cgi
        self.elapsed_time = HTML('')
        self.bars = {key: FloatProgress(min=0, max=1, description='', height = '10px')
                        for key in self.all_keys}
        self.bar_texts = {key: HTML('', width = "140px") for key in self.all_keys}
        self.bar_labels = {key: HTML('<div style=\"padding: 0px 10px 0px 10px; text-align:left; word-wrap: break-word;\">'
                                     + cgi.escape(key) + '</div>') for key in self.all_keys}

        # Check to see if 'finalize' is one of the keys. If it is, move it to
        # the end so that it is rendered last in the list (for aesthetics...)
        key_order = set(self.all_keys.keys())
        if 'finalize' in key_order:
            key_order.remove('finalize')
            key_order = sorted(list(key_order)) + ['finalize']
        else:
            key_order = list(key_order)

        self.bar_widgets = VBox([ HBox([ self.bar_texts[key], self.bars[key], self.bar_labels[key] ]) for key in key_order ])
        self.widget = VBox([self.elapsed_time, self.bar_widgets])

        from tornado.ioloop import IOLoop
        loop = IOLoop.instance()
        self.pc = PeriodicCallback(self._update, 1000 * self._dt, io_loop=loop)
        self.pc.start()

        # Clear out errors
        clear_errors(errors)

    def _start(self):
        return self._update()

    def _ipython_display_(self, **kwargs):
        return self.widget._ipython_display_(**kwargs)

    def stop(self, exception=None, key=None):
        with ignoring(AttributeError):
            self.pc.stop()
        Progress.stop(self, exception, key)
        self._update()
        for k, v in self.keys.items():
            if not v:
                self.bars[k].bar_style = 'success'
        if exception:
            self.bars[self.func(key)].bar_style = 'danger'
            self.elapsed_time.value = '<div style="padding: 0px 10px 5px 10px"><b>Warning:</b> the computation terminated due to an error after ' + format_time(self.elapsed) + '</div>'

    def _update(self):
        for k in self.all_keys:
            ntasks = len(self.all_keys[k])
            ndone = ntasks - len(self.keys[k])
            self.elapsed_time.value = '<div style="padding: 0px 10px 5px 10px"><b>Elapsed time:</b> ' + format_time(self.elapsed) + '</div>'
            self.bars[k].value = ndone / ntasks if ntasks else 1.0
            self.bar_texts[k].value = '<div style="padding: 0px 10px 0px 10px; text-align: right">%d / %d</div>' % (ndone, ntasks)


def progress(*futures, **kwargs):
    """ Track progress of futures

    This operates differently in the notebook and the console

    *  Notebook:  This returns immediately, leaving an IPython widget on screen
    *  Console:  This blocks until the computation completes

    Parameters
    ----------
    futures: Futures
        A list of futures or keys to track
    notebook: bool (optional)
        Running in the notebook or not (defaults to guess)
    multi: bool (optional)
        Track different functions independently (defaults to True)
    complete: bool (optional)
        Track all keys (True) or only keys that have not yet run (False)
        (defaults to True)

    Examples
    --------
    >>> progress(futures)  # doctest: +SKIP
    [########################################] | 100% Completed |  1.7s
    """
    notebook = kwargs.pop('notebook', None)
    multi = kwargs.pop('multi', True)
    complete = kwargs.pop('complete', True)
    assert not kwargs

    futures = list(dask.core.flatten(list(futures)))
    if not isinstance(futures, (set, list)):
        futures = [futures]
    if notebook is None:
        notebook = is_kernel()  # often but not always correct assumption
    if notebook:
        if multi:
            bar = MultiProgressWidget(futures, complete=complete)
        else:
            bar = ProgressWidget(futures, complete=complete)
        bar.start()
        return bar
    else:
        bar = TextProgressBar(futures, complete=complete)
        bar.start()
        bar._timer.join()


def is_kernel():
    """ Determine if we're running within an IPython kernel

    >>> is_kernel()
    False
    """
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
