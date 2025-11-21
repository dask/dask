from __future__ import annotations

import os

try:
    import setproctitle as setproctitle_mod
except ImportError:
    setproctitle_mod = None


_enabled = False


def enable_proctitle_on_children():
    """
    Enable setting the process title on this process' children and
    grandchildren.
    """
    os.environ["DASK_PARENT"] = str(os.getpid())


def enable_proctitle_on_current():
    """
    Enable setting the process title on this process.
    """
    global _enabled
    _enabled = True


def setproctitle(title):
    """
    Change this process' title, as displayed in various utilities
    such as `ps`.
    """
    if setproctitle_mod is None:
        return
    enabled = _enabled
    if not enabled:
        try:
            enabled = int(os.environ.get("DASK_PARENT", "")) != os.getpid()
        except ValueError:  # pragma: no cover
            pass
    if enabled:
        setproctitle_mod.setproctitle(title)
