from __future__ import annotations

import gc
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore


@pytest.fixture
def check_fd_leaks():
    if sys.platform == "win32" or psutil is None:
        yield
    else:
        proc = psutil.Process()
        before = proc.num_fds()
        yield
        gc.collect()
        assert proc.num_fds() == before


@pytest.fixture
def is_locked():
    """Callable that returns True if the parameter zict mapping has its RLock engaged"""
    with ThreadPoolExecutor(1) as ex:

        def __is_locked(d):
            out = d.lock.acquire(blocking=False)
            if out:
                d.lock.release()
            return not out

        def _is_locked(d):
            return ex.submit(__is_locked, d).result()

        yield _is_locked


@pytest.fixture
def check_thread_leaks():
    active_threads_start = threading.enumerate()

    yield

    bad_threads = [
        thread for thread in threading.enumerate() if thread not in active_threads_start
    ]
    if bad_threads:
        raise RuntimeError(f"Leaked thread(s): {bad_threads}")
