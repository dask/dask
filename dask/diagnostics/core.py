from ..callbacks import Callback, add_callbacks
from ..context import _globals


class Diagnostic(object):
    """Base class for diagnostics using the callback mechanism."""

    @property
    def _callback(self):
        funcs = ['_start', '_pretask', '_posttask', '_finish']
        cbs = [getattr(self, f) if hasattr(self, f) else None for f in funcs]
        return Callback(*cbs)

    def __enter__(self):
        self._cm = add_callbacks(self._callback)
        self._cm.__enter__()
        return self

    def __exit__(self, *args):
        self._cm.__exit__(*args)

    def register(self):
        _globals['callbacks'].add(self._callback)

    def unregister(self):
        _globals['callbacks'].remove(self._callback)
