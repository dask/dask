from ..callbacks import callbacks, add_callbacks


class Diagnostic(object):
    """Base class for diagnostics using the callback mechanism."""

    @property
    def callbacks(self):
        funcs = ['_start', '_pretask', '_posttask', '_finish']
        cbs = [getattr(self, f) if hasattr(self, f) else None for f in funcs]
        return callbacks(*cbs)

    def __enter__(self):
        self._cm = add_callbacks(self.callbacks)
        self._cm.__enter__()
        return self

    def __exit__(self, *args):
        self._cm.__exit__(*args)
