from ..async import callbacks
from ..context import set_options


class Callback(object):
    """Base class for diagnostics using the callback mechanism."""

    @property
    def callbacks(self):
        funcs = ['_start', '_pretask', '_posttask', '_finish']
        cbs = [getattr(self, f) if hasattr(self, f) else None for f in funcs]
        return callbacks(*cbs)

    def __enter__(self):
        self._set_options = set_options(callbacks=self.callbacks)
        self._set_options.__enter__()
        return self

    def __exit__(self, *args):
        self._set_options.__exit__(*args)
