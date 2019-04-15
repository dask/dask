from __future__ import print_function, division, absolute_import

import logging
from timeit import default_timer
import sys
import weakref

from toolz import valmap
from tornado import gen
from tornado.ioloop import IOLoop

from .progress import format_time, Progress, MultiProgress

from ..compatibility import html_escape
from ..core import connect, coerce_to_address, CommClosedError
from ..client import default_client, futures_of
from ..protocol.pickle import dumps
from ..utils import ignoring, key_split, is_kernel, LoopRunner, parse_timedelta


logger = logging.getLogger(__name__)


def get_scheduler(scheduler):
    if scheduler is None:
        return default_client().scheduler.address
    return coerce_to_address(scheduler)


class ProgressBar(object):
    def __init__(self, keys, scheduler=None, interval="100ms", complete=True):
        self.scheduler = get_scheduler(scheduler)

        self.client = None
        for key in keys:
            if hasattr(key, "client"):
                self.client = weakref.ref(key.client)
                break

        self.keys = {k.key if hasattr(k, "key") else k for k in keys}
        self.interval = parse_timedelta(interval, default="s")
        self.complete = complete
        self._start_time = default_timer()

    @property
    def elapsed(self):
        return default_timer() - self._start_time

    @gen.coroutine
    def listen(self):
        complete = self.complete
        keys = self.keys

        @gen.coroutine
        def setup(scheduler):
            p = Progress(keys, scheduler, complete=complete)
            yield p.setup()
            raise gen.Return(p)

        def function(scheduler, p):
            result = {
                "all": len(p.all_keys),
                "remaining": len(p.keys),
                "status": p.status,
            }
            if p.status == "error":
                result.update(p.extra)
            return result

        self.comm = yield connect(
            self.scheduler,
            connection_args=self.client().connection_args if self.client else None,
        )
        logger.debug("Progressbar Connected to scheduler")

        yield self.comm.write(
            {
                "op": "feed",
                "setup": dumps(setup),
                "function": dumps(function),
                "interval": self.interval,
            },
            serializers=self.client()._serializers if self.client else None,
        )

        while True:
            try:
                response = yield self.comm.read(
                    deserializers=self.client()._deserializers if self.client else None
                )
            except CommClosedError:
                break
            self._last_response = response
            self.status = response["status"]
            self._draw_bar(**response)
            if response["status"] in ("error", "finished"):
                yield self.comm.close()
                self._draw_stop(**response)
                break

        logger.debug("Progressbar disconnected from scheduler")

    def _draw_stop(self, **kwargs):
        pass

    def __del__(self):
        with ignoring(AttributeError):
            self.comm.abort()


class TextProgressBar(ProgressBar):
    def __init__(
        self,
        keys,
        scheduler=None,
        interval="100ms",
        width=40,
        loop=None,
        complete=True,
        start=True,
    ):
        super(TextProgressBar, self).__init__(keys, scheduler, interval, complete)
        self.width = width
        self.loop = loop or IOLoop()

        if start:
            loop_runner = LoopRunner(self.loop)
            loop_runner.run_sync(self.listen)

    def _draw_bar(self, remaining, all, **kwargs):
        frac = (1 - remaining / all) if all else 1.0
        bar = "#" * int(self.width * frac)
        percent = int(100 * frac)
        elapsed = format_time(self.elapsed)
        msg = "\r[{0:<{1}}] | {2}% Completed | {3}".format(
            bar, self.width, percent, elapsed
        )
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()

    def _draw_stop(self, **kwargs):
        sys.stdout.write("\r")
        sys.stdout.flush()


class ProgressWidget(ProgressBar):
    """ ProgressBar that uses an IPython ProgressBar widget for the notebook

    See Also
    --------
    progress: User function
    TextProgressBar: Text version suitable for the console
    """

    def __init__(
        self, keys, scheduler=None, interval="100ms", complete=False, loop=None
    ):
        super(ProgressWidget, self).__init__(keys, scheduler, interval, complete)

        from ipywidgets import FloatProgress, HBox, VBox, HTML

        self.elapsed_time = HTML("")
        self.bar = FloatProgress(min=0, max=1, description="")
        self.bar_text = HTML("")

        self.bar_widget = HBox([self.bar_text, self.bar])
        self.widget = VBox([self.elapsed_time, self.bar_widget])

    def _ipython_display_(self, **kwargs):
        IOLoop.current().add_callback(self.listen)
        return self.widget._ipython_display_(**kwargs)

    def _draw_stop(self, remaining, status, exception=None, **kwargs):
        if status == "error":
            self.bar.bar_style = "danger"
            self.elapsed_time.value = (
                '<div style="padding: 0px 10px 5px 10px"><b>Exception</b> '
                "<tt>"
                + repr(exception)
                + "</tt>:"
                + format_time(self.elapsed)
                + " "
                + "</div>"
            )
        elif not remaining:
            self.bar.bar_style = "success"
            self.elapsed_time.value = (
                '<div style="padding: 0px 10px 5px 10px"><b>Finished:</b> '
                + format_time(self.elapsed)
                + "</div>"
            )

    def _draw_bar(self, remaining, all, **kwargs):
        ndone = all - remaining
        self.elapsed_time.value = (
            '<div style="padding: 0px 10px 5px 10px"><b>Computing:</b> '
            + format_time(self.elapsed)
            + "</div>"
        )
        self.bar.value = ndone / all if all else 1.0
        self.bar_text.value = (
            '<div style="padding: 0px 10px 0px 10px; text-align:right;">%d / %d</div>'
            % (ndone, all)
        )


class MultiProgressBar(object):
    def __init__(
        self, keys, scheduler=None, func=key_split, interval="100ms", complete=False
    ):
        self.scheduler = get_scheduler(scheduler)

        self.client = None
        for key in keys:
            if hasattr(key, "client"):
                self.client = weakref.ref(key.client)
                break

        self.keys = {k.key if hasattr(k, "key") else k for k in keys}
        self.func = func
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
        func = self.func

        @gen.coroutine
        def setup(scheduler):
            p = MultiProgress(keys, scheduler, complete=complete, func=func)
            yield p.setup()
            raise gen.Return(p)

        def function(scheduler, p):
            result = {
                "all": valmap(len, p.all_keys),
                "remaining": valmap(len, p.keys),
                "status": p.status,
            }
            if p.status == "error":
                result.update(p.extra)
            return result

        self.comm = yield connect(
            self.scheduler,
            connection_args=self.client().connection_args if self.client else None,
        )
        logger.debug("Progressbar Connected to scheduler")

        yield self.comm.write(
            {
                "op": "feed",
                "setup": dumps(setup),
                "function": dumps(function),
                "interval": self.interval,
            }
        )

        while True:
            response = yield self.comm.read(
                deserializers=self.client()._deserializers if self.client else None
            )
            self._last_response = response
            self.status = response["status"]
            self._draw_bar(**response)
            if response["status"] in ("error", "finished"):
                yield self.comm.close()
                self._draw_stop(**response)
                break
        logger.debug("Progressbar disconnected from scheduler")

    def _draw_stop(self, **kwargs):
        pass

    def __del__(self):
        with ignoring(AttributeError):
            self.comm.abort()


class MultiProgressWidget(MultiProgressBar):
    """ Multiple progress bar Widget suitable for the notebook

    Displays multiple progress bars for a computation, split on computation
    type.

    See Also
    --------
    progress: User-level function <--- use this
    MultiProgress: Non-visualization component that contains most logic
    ProgressWidget: Single progress bar widget
    """

    def __init__(
        self,
        keys,
        scheduler=None,
        minimum=0,
        interval=0.1,
        func=key_split,
        complete=False,
    ):
        super(MultiProgressWidget, self).__init__(
            keys, scheduler, func, interval, complete
        )
        from ipywidgets import VBox

        self.widget = VBox([])

    def make_widget(self, all):
        from ipywidgets import FloatProgress, HBox, VBox, HTML

        self.elapsed_time = HTML("")
        self.bars = {key: FloatProgress(min=0, max=1, description="") for key in all}
        self.bar_texts = {key: HTML("") for key in all}
        self.bar_labels = {
            key: HTML(
                '<div style="padding: 0px 10px 0px 10px;'
                " text-align:left; word-wrap: "
                'break-word;">'
                + html_escape(key.decode() if isinstance(key, bytes) else key)
                + "</div>"
            )
            for key in all
        }

        def keyfunc(kv):
            """ Order keys by most numerous, then by string name """
            return kv[::-1]

        key_order = [k for k, v in sorted(all.items(), key=keyfunc, reverse=True)]

        self.bar_widgets = VBox(
            [
                HBox([self.bar_texts[key], self.bars[key], self.bar_labels[key]])
                for key in key_order
            ]
        )
        self.widget.children = (self.elapsed_time, self.bar_widgets)

    def _ipython_display_(self, **kwargs):
        IOLoop.current().add_callback(self.listen)
        return self.widget._ipython_display_(**kwargs)

    def _draw_stop(self, remaining, status, exception=None, key=None, **kwargs):
        for k, v in remaining.items():
            if not v:
                self.bars[k].bar_style = "success"
            else:
                self.bars[k].bar_style = "danger"

        if status == "error":
            # self.bars[self.func(key)].bar_style = 'danger'  # TODO
            self.elapsed_time.value = (
                '<div style="padding: 0px 10px 5px 10px"><b>Exception</b> '
                + "<tt>"
                + repr(exception)
                + "</tt>:"
                + format_time(self.elapsed)
                + " "
                + "</div>"
            )
        else:
            self.elapsed_time.value = (
                '<div style="padding: 0px 10px 5px 10px"><b>Finished:</b> '
                + format_time(self.elapsed)
                + "</div>"
            )

    def _draw_bar(self, remaining, all, status, **kwargs):
        if self.keys and not self.widget.children:
            self.make_widget(all)
        for k, ntasks in all.items():
            ndone = ntasks - remaining[k]
            self.elapsed_time.value = (
                '<div style="padding: 0px 10px 5px 10px"><b>Computing:</b> '
                + format_time(self.elapsed)
                + "</div>"
            )
            self.bars[k].value = ndone / ntasks if ntasks else 1.0
            self.bar_texts[k].value = (
                '<div style="padding: 0px 10px 0px 10px; text-align: right">%d / %d</div>'
                % (ndone, ntasks)
            )


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

    Notes
    -----
    In the notebook, the output of `progress` must be the last statement
    in the cell. Typically, this means calling `progress` at the end of a
    cell.

    Examples
    --------
    >>> progress(futures)  # doctest: +SKIP
    [########################################] | 100% Completed |  1.7s
    """
    notebook = kwargs.pop("notebook", None)
    multi = kwargs.pop("multi", True)
    complete = kwargs.pop("complete", True)
    assert not kwargs

    futures = futures_of(futures)
    if not isinstance(futures, (set, list)):
        futures = [futures]
    if notebook is None:
        notebook = is_kernel()  # often but not always correct assumption
    if notebook:
        if multi:
            bar = MultiProgressWidget(futures, complete=complete)
        else:
            bar = ProgressWidget(futures, complete=complete)
        return bar
    else:
        TextProgressBar(futures, complete=complete)
