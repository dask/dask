from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import importlib
import inspect
import json
import logging
import multiprocessing
import os
import pkgutil
import re
import socket
import sys
import tempfile
import threading
import traceback
import warnings
import weakref
import xml.etree.ElementTree
from asyncio import Event as LateLoopEvent
from asyncio import TimeoutError
from collections import deque
from collections.abc import (
    Awaitable,
    Callable,
    Collection,
    Container,
    Generator,
    Iterator,
    KeysView,
    ValuesView,
)
from concurrent.futures import (  # noqa: F401
    CancelledError,
    Executor,
    ThreadPoolExecutor,
)
from contextvars import ContextVar
from datetime import timedelta
from functools import wraps
from hashlib import md5
from importlib.util import cache_from_source
from pickle import PickleBuffer
from time import sleep
from types import ModuleType
from typing import TYPE_CHECKING
from typing import Any as AnyType
from typing import ClassVar, TypeVar, overload

import psutil
import tblib.pickling_support
from tornado import escape

from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory

try:
    import resource
except ImportError:
    resource = None  # type: ignore

import tlz as toolz
from tornado import gen
from tornado.ioloop import IOLoop

import dask
from dask.utils import _deprecated
from dask.utils import ensure_bytes as _ensure_bytes
from dask.utils import key_split
from dask.utils import parse_timedelta as _parse_timedelta
from dask.widgets import get_template

from distributed.compatibility import WINDOWS
from distributed.metrics import context_meter, monotonic, time

try:
    from dask.context import thread_state
except ImportError:
    thread_state = threading.local()

# For some reason this is required in python >= 3.9
if WINDOWS:
    import multiprocessing.popen_spawn_win32
else:
    import multiprocessing.popen_spawn_posix

logger = _logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    # TODO: import from typing (requires Python >=3.10)
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    T = TypeVar("T")

_forkserver_preload_set = False


def get_mp_context():
    """Create a multiprocessing context

    The context type is controlled by the
    ``distributed.worker.multiprocessing-method`` configuration key.

    Returns
    -------
    multiprocessing.BaseContext
        The multiprocessing context

    Notes
    -----
    Repeated calls with the same method will return the same object
    (since multiprocessing.get_context returns singleton instances).
    """
    global _forkserver_preload_set
    method = dask.config.get("distributed.worker.multiprocessing-method")
    ctx = multiprocessing.get_context(method)
    if method == "forkserver" and not _forkserver_preload_set:
        # Makes the test suite much faster
        preload = ["distributed"]

        from distributed.versions import optional_packages, required_packages

        for pkg in required_packages + optional_packages:
            try:
                importlib.import_module(pkg)
            except ImportError:
                pass
            else:
                preload.append(pkg)
        ctx.set_forkserver_preload(preload)
        _forkserver_preload_set = True

    return ctx


def has_arg(func, argname):
    """
    Whether the function takes an argument with the given name.
    """
    while True:
        try:
            argspec = inspect.getfullargspec(func)
            if argname in set(argspec.args) | set(argspec.kwonlyargs):
                return True
        except TypeError:
            break
        try:
            # For Tornado coroutines and other decorated functions
            func = func.__wrapped__
        except AttributeError:
            break
    return False


def get_fileno_limit():
    """
    Get the maximum number of open files per process.
    """
    if resource is not None:
        return resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    else:
        # Default ceiling for Windows when using the CRT, though it
        # is settable using _setmaxstdio().
        return 512


@toolz.memoize
def _get_ip(host, port, family):
    def hostname_fallback():
        try:
            addr_info = socket.getaddrinfo(
                socket.gethostname(),
                port,
                family,
                socket.SOCK_DGRAM,
                socket.IPPROTO_UDP,
            )[0]
            return addr_info[4][0]
        # If getaddrinfo() fails, relay the error and return a sane default
        except socket.gaierror as e:
            warnings.warn(
                f"Couldn't detect a suitable IP address ({e}). "
                "Falling back to 127.0.0.1.",
                RuntimeWarning,
            )
            return "127.0.0.1"

    # By using a UDP socket, we don't actually try to connect but
    # simply select the local address through which *host* is reachable.
    sock = socket.socket(family, socket.SOCK_DGRAM)
    try:
        sock.connect((host, port))
        ip = sock.getsockname()[0]
        if ip == "0.0.0.0":
            return hostname_fallback()
        return ip
    except OSError as e:
        warnings.warn(
            "Couldn't detect a suitable IP address for "
            "reaching %r, defaulting to hostname: %s" % (host, e),
            RuntimeWarning,
        )
        return hostname_fallback()
    finally:
        sock.close()


def get_ip(host="8.8.8.8", port=80):
    """
    Get the local IP address through which the *host* is reachable.

    It will try to get ipv4 or ipv6 adaptively depending on the *host* to reach.

    *host* defaults to a well-known Internet host (one of Google's public
    DNS servers).
    """
    if ":" in host:
        return _get_ip(host, port, family=socket.AF_INET6)
    else:
        return _get_ip(host, port, family=socket.AF_INET)


@_deprecated(use_instead="get_ip")
def get_ipv6(host="2001:4860:4860::8888", port=80):
    """
    The same as get_ip(), but for IPv6.
    """
    return _get_ip(host, port, family=socket.AF_INET6)


def get_ip_interface(ifname):
    """
    Get the local IPv4 address of a network interface.

    KeyError is raised if the interface doesn't exist.
    ValueError is raised if the interface does no have an IPv4 address
    associated with it.
    """
    net_if_addrs = psutil.net_if_addrs()

    if ifname not in net_if_addrs:
        allowed_ifnames = list(net_if_addrs.keys())
        raise ValueError(
            "{!r} is not a valid network interface. "
            "Valid network interfaces are: {}".format(ifname, allowed_ifnames)
        )

    for info in net_if_addrs[ifname]:
        if info.family == socket.AF_INET:
            return info.address
    raise ValueError(f"interface {ifname!r} doesn't have an IPv4 address")


async def All(args, quiet_exceptions=()):
    """Wait on many tasks at the same time

    Err once any of the tasks err.

    See https://github.com/tornadoweb/tornado/issues/1546

    Parameters
    ----------
    args: futures to wait for
    quiet_exceptions: tuple, Exception
        Exception types to avoid logging if they fail
    """
    tasks = gen.WaitIterator(*map(asyncio.ensure_future, args))
    results = [None for _ in args]
    while not tasks.done():
        try:
            result = await tasks.next()
        except Exception:

            @gen.coroutine
            def quiet():
                """Watch unfinished tasks

                Otherwise if they err they get logged in a way that is hard to
                control.  They need some other task to watch them so that they
                are not orphaned
                """
                for task in list(tasks._unfinished):
                    try:
                        yield task
                    except quiet_exceptions:
                        pass

            quiet()
            raise
        results[tasks.current_index] = result
    return results


async def Any(args, quiet_exceptions=()):
    """Wait on many tasks at the same time and return when any is finished

    Err once any of the tasks err.

    Parameters
    ----------
    args: futures to wait for
    quiet_exceptions: tuple, Exception
        Exception types to avoid logging if they fail
    """
    tasks = gen.WaitIterator(*map(asyncio.ensure_future, args))
    results = [None for _ in args]
    while not tasks.done():
        try:
            result = await tasks.next()
        except Exception:

            @gen.coroutine
            def quiet():
                """Watch unfinished tasks

                Otherwise if they err they get logged in a way that is hard to
                control.  They need some other task to watch them so that they
                are not orphaned
                """
                for task in list(tasks._unfinished):
                    try:
                        yield task
                    except quiet_exceptions:
                        pass

            quiet()
            raise

        results[tasks.current_index] = result
        break
    return results


class NoOpAwaitable:
    """An awaitable object that always returns None.

    Useful to return from a method that can be called in both asynchronous and
    synchronous contexts"""

    def __await__(self):
        async def f():
            return None

        return f().__await__()


class SyncMethodMixin:
    """
    A mixin for adding an `asynchronous` attribute and `sync` method to a class.

    Subclasses must define a `loop` attribute for an associated
    `tornado.IOLoop`, and may also add a `_asynchronous` attribute indicating
    whether the class should default to asynchronous behavior.
    """

    @property
    def asynchronous(self):
        """Are we running in the event loop?"""
        try:
            return in_async_call(
                self.loop, default=getattr(self, "_asynchronous", False)
            )
        except RuntimeError:
            return False

    def sync(self, func, *args, asynchronous=None, callback_timeout=None, **kwargs):
        """Call `func` with `args` synchronously or asynchronously depending on
        the calling context"""
        callback_timeout = _parse_timedelta(callback_timeout)
        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = wait_for(future, callback_timeout)
            return future
        else:
            return sync(
                self.loop, func, *args, callback_timeout=callback_timeout, **kwargs
            )


def in_async_call(loop, default=False):
    """Whether this call is currently within an async call"""
    try:
        return loop.asyncio_loop is asyncio.get_running_loop()
    except RuntimeError:
        # No *running* loop in thread. If the event loop isn't running, it
        # _could_ be started later in this thread though. Return the default.
        if not loop.asyncio_loop.is_running():
            return default
        return False


def sync(
    loop: IOLoop,
    func: Callable[..., Awaitable[T]],
    *args: AnyType,
    callback_timeout: str | float | timedelta | None = None,
    **kwargs: AnyType,
) -> T:
    """
    Run coroutine in loop running in separate thread.
    """
    timeout = _parse_timedelta(callback_timeout, "s")
    if loop.asyncio_loop.is_closed():  # type: ignore[attr-defined]
        raise RuntimeError("IOLoop is closed")

    e = threading.Event()
    main_tid = threading.get_ident()

    # set up non-locals
    result: T
    error: BaseException | None = None
    future: asyncio.Future[T]

    @gen.coroutine
    def f() -> Generator[AnyType, AnyType, None]:
        nonlocal result, error, future
        try:
            if main_tid == threading.get_ident():
                raise RuntimeError("sync() called from thread of running loop")
            yield gen.moment
            awaitable = func(*args, **kwargs)
            if timeout is not None:
                awaitable = wait_for(awaitable, timeout)
            future = asyncio.ensure_future(awaitable)
            result = yield future
        except Exception as exception:
            error = exception
        finally:
            e.set()

    def cancel() -> None:
        if future is not None:
            future.cancel()

    def wait(timeout: float | None) -> bool:
        try:
            return e.wait(timeout)
        except KeyboardInterrupt:
            loop.add_callback(cancel)
            raise

    loop.add_callback(f)
    if timeout is not None:
        if not wait(timeout):
            raise TimeoutError(f"timed out after {timeout} s.")
    else:
        while not e.is_set():
            wait(10)

    if error is not None:
        raise error
    else:
        return result


class _CollectErrorThread:
    def __init__(self, target: Callable[[], None], daemon: bool, name: str):
        self._exception: BaseException | None = None

        def wrapper() -> None:
            try:
                target()
            except BaseException as e:  # noqa: B036
                self._exception = e

        self._thread = thread = threading.Thread(
            target=wrapper, daemon=daemon, name=name
        )
        thread.start()

    def join(self, timeout: float | None = None) -> None:
        thread = self._thread
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise TimeoutError("join timed out")
        if self._exception is not None:
            try:
                raise self._exception
            finally:  # remove a reference cycle
                del self._exception


class LoopRunner:
    """
    A helper to start and stop an IO loop in a controlled way.
    Several loop runners can associate safely to the same IO loop.

    Parameters
    ----------
    loop: IOLoop (optional)
        If given, this loop will be re-used, otherwise an appropriate one
        will be looked up or created.
    asynchronous: boolean (optional, default False)
        If false (the default), the loop is meant to run in a separate
        thread and will be started if necessary.
        If true, the loop is meant to run in the thread this
        object is instantiated from, and will not be started automatically.
    """

    # All loops currently associated to loop runners
    _all_loops: ClassVar[
        weakref.WeakKeyDictionary[IOLoop, tuple[int, LoopRunner | None]]
    ] = weakref.WeakKeyDictionary()
    _lock = threading.Lock()
    _loop_thread: _CollectErrorThread | None

    def __init__(self, loop: IOLoop | None = None, asynchronous: bool = False):
        if loop is None:
            if asynchronous:
                # raises RuntimeError if there's no running loop
                try:
                    asyncio.get_running_loop()
                except RuntimeError as e:
                    raise RuntimeError(
                        "Constructing LoopRunner(asynchronous=True) without a running loop is not supported"
                    ) from e
                loop = IOLoop.current()
        elif not loop.asyncio_loop.is_running():  # type: ignore[attr-defined]
            # LoopRunner is not responsible for starting a foreign IOLoop
            raise RuntimeError(
                "Constructing LoopRunner(loop=loop) without a running loop is not supported"
            )

        self._loop = loop
        self._asynchronous = asynchronous
        self._loop_thread = None
        self._started = False
        self._stop_event = LateLoopEvent()

    def start(self):
        """
        Start the IO loop if required.  The loop is run in a dedicated
        thread.

        If the loop is already running, this method does nothing.
        """
        with self._lock:
            self._start_unlocked()

    def _start_unlocked(self) -> None:
        assert not self._started

        if self._loop is not None:
            try:
                count, real_runner = self._all_loops[self._loop]
            except KeyError:
                assert self._loop.asyncio_loop.is_running()  # type: ignore[attr-defined]
                self._started = True
                return

            self._all_loops[self._loop] = count + 1, real_runner
            self._started = True
            return

        assert self._loop_thread is None

        start_evt = threading.Event()
        start_exc = None
        loop = None

        async def amain() -> None:
            nonlocal loop
            loop = IOLoop.current()
            start_evt.set()
            await self._stop_event.wait()

        def run_loop() -> None:
            nonlocal start_exc
            try:
                asyncio_run(amain(), loop_factory=get_loop_factory())
            except BaseException as e:
                if start_evt.is_set():
                    raise
                start_exc = e
                start_evt.set()

        self._loop_thread = _CollectErrorThread(
            target=run_loop, daemon=True, name="IO loop"
        )
        start_evt.wait(timeout=10)
        if start_exc is not None:
            raise start_exc
        assert loop is not None
        self._loop = loop
        self._started = True
        self._all_loops[loop] = (1, self)

    def stop(self, timeout=10):
        """
        Stop and close the loop if it was created by us.
        Otherwise, just mark this object "stopped".
        """
        with self._lock:
            self._stop_unlocked(timeout)

    def _stop_unlocked(self, timeout):
        if not self._started:
            return

        self._started = False

        try:
            count, real_runner = self._all_loops[self._loop]
        except KeyError:
            return

        if count > 1:
            self._all_loops[self._loop] = count - 1, real_runner
            return

        assert count == 1
        del self._all_loops[self._loop]
        real_runner._real_stop(timeout)

    def _real_stop(self, timeout):
        assert self._loop_thread is not None
        try:
            self._loop.add_callback(self._stop_event.set)
            self._loop_thread.join(timeout=timeout)
        finally:
            self._loop_thread = None

    def is_started(self):
        """
        Return True between start() and stop() calls, False otherwise.
        """
        return self._started

    def run_sync(self, func, *args, **kwargs):
        """
        Convenience helper: start the loop if needed,
        run sync(func, *args, **kwargs), then stop the loop again.
        """
        if self._started:
            return sync(self.loop, func, *args, **kwargs)
        else:
            self.start()
            try:
                return sync(self.loop, func, *args, **kwargs)
            finally:
                self.stop()

    @property
    def loop(self):
        loop = self._loop
        if loop is None or not loop.asyncio_loop.is_running():
            raise RuntimeError(
                "Accessing the loop property while the loop is not running is not supported"
            )
        return self._loop


@contextlib.contextmanager
def set_thread_state(**kwargs):
    old = {}
    for k in kwargs:
        try:
            old[k] = getattr(thread_state, k)
        except AttributeError:
            pass
    for k, v in kwargs.items():
        setattr(thread_state, k, v)
    try:
        yield
    finally:
        for k in kwargs:
            try:
                v = old[k]
            except KeyError:
                delattr(thread_state, k)
            else:
                setattr(thread_state, k, v)


@contextlib.contextmanager
def tmp_text(filename, text):
    fn = os.path.join(tempfile.gettempdir(), filename)
    with open(fn, "w") as f:
        f.write(text)

    try:
        yield fn
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def is_kernel():
    """Determine if we're running within an IPython kernel

    >>> is_kernel()
    False
    """
    # http://stackoverflow.com/questions/34091701/determine-if-were-in-an-ipython-notebook-session
    if "IPython" not in sys.modules:  # IPython hasn't been imported
        return False
    from IPython import get_ipython

    # check for `kernel` attribute on the IPython instance
    return getattr(get_ipython(), "kernel", None) is not None


def key_split_group(x: object) -> str:
    """A more fine-grained version of key_split.

    >>> key_split_group(('x-2', 1))
    'x-2'
    >>> key_split_group("('x-2', 1)")
    'x-2'
    >>> key_split_group('ae05086432ca935f6eba409a8ecd4896')
    'data'
    >>> key_split_group('<module.submodule.myclass object at 0xdaf372')
    'myclass'
    >>> key_split_group('x')
    'x'
    >>> key_split_group('x-1')
    'x'
    """
    if isinstance(x, tuple):
        return x[0]
    elif isinstance(x, str):
        if x[0] == "(":
            return x.split(",", 1)[0].strip("()\"'")
        elif len(x) == 32 and re.match(r"[a-f0-9]{32}", x):
            return "data"
        elif x[0] == "<":
            return x.strip("<>").split()[0].split(".")[-1]
        else:
            return key_split(x)
    elif isinstance(x, bytes):
        return key_split_group(x.decode())
    else:
        return "Other"


@overload
def log_errors(func: Callable[P, T], /) -> Callable[P, T]: ...


@overload
def log_errors(*, pdb: bool = False, unroll_stack: int = 1) -> _LogErrors: ...


def log_errors(func=None, /, *, pdb=False, unroll_stack=0):
    """Log any errors and then reraise them.

    This can be used:

    - As a context manager::

        with log_errors(...):
            ...

    - As a bare function decorator::

        @log_errors
        def func(...):
            ...

    - As a function decorator with parameters::

        @log_errors(...)
        def func(...):
            ...

    Parameters
    ----------
    pdb: bool, optional
        Set to True to break into the debugger in case of exception
    unroll_stack: int, optional
        Number of levels of stack to unroll when determining the module's name for the
        purpose of logging. Normally you should omit this. Set to 1 if you are writing a
        helper function, context manager, or decorator.
    """
    le = _LogErrors(pdb=pdb, unroll_stack=unroll_stack)
    return le(func) if func else le


_getmodulename_with_path_map: dict[str, str] = {}


def _getmodulename_with_path(fname: str) -> str:
    """Variant of inspect.getmodulename that returns the full module path"""
    try:
        return _getmodulename_with_path_map[fname]
    except KeyError:
        pass

    for modname, mod in sys.modules.copy().items():
        fname2 = getattr(mod, "__file__", None)
        if fname2:
            _getmodulename_with_path_map[fname2] = modname

    try:
        return _getmodulename_with_path_map[fname]
    except KeyError:  # pragma: nocover
        return os.path.splitext(os.path.basename(fname))[0]


class _LogErrors:
    __slots__ = ("pdb", "unroll_stack")

    pdb: bool
    unroll_stack: int

    def __init__(self, pdb: bool, unroll_stack: int):
        self.pdb = pdb
        self.unroll_stack = unroll_stack

    def __call__(self, func: Callable[P, T], /) -> Callable[P, T]:
        self.unroll_stack += 1

        if inspect.iscoroutinefunction(func):

            async def wrapper(*args, **kwargs):
                with self:
                    return await func(*args, **kwargs)

        else:

            def wrapper(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)

        return wraps(func)(wrapper)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        from distributed.comm import CommClosedError

        if not exc_type or issubclass(exc_type, (CommClosedError, gen.Return)):
            return

        stack = traceback.extract_tb(tb)
        frame = stack[min(self.unroll_stack, len(stack) - 1)]
        modname = _getmodulename_with_path(frame.filename)

        try:
            logger = logging.getLogger(modname)
            logger.exception(exc_value)
        except Exception:  # Interpreter teardown
            pass  # pragma: nocover

        if self.pdb:
            import pdb  # pragma: nocover

            pdb.set_trace()  # pragma: nocover


def silence_logging(level, root="distributed"):
    """
    Change all StreamHandlers for the given logger to the given level
    """
    warnings.warn(
        "silence_logging is deprecated, call silence_logging_cmgr",
        DeprecationWarning,
        stacklevel=2,
    )
    if isinstance(level, str):
        level = getattr(logging, level.upper())

    old = None
    logger = logging.getLogger(root)
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            old = handler.level
            handler.setLevel(level)

    return old


@contextlib.contextmanager
def silence_logging_cmgr(
    level: str | int, root: str = "distributed"
) -> Generator[None]:
    """
    Temporarily change all StreamHandlers for the given logger to the given level
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())

    logger = logging.getLogger(root)
    with contextlib.ExitStack() as stack:
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                old = handler.level
                if old != level:
                    handler.setLevel(level)
                    stack.callback(handler.setLevel, old)
        yield


@toolz.memoize
def ensure_ip(hostname):
    """Ensure that address is an IP address

    Examples
    --------
    >>> ensure_ip('localhost')
    '127.0.0.1'
    >>> ensure_ip('')  # Maps as localhost for binding e.g. 'tcp://:8811'
    '127.0.0.1'
    >>> ensure_ip('123.123.123.123')  # pass through IP addresses
    '123.123.123.123'
    """
    if not hostname:
        hostname = "localhost"

    # Prefer IPv4 over IPv6, for compatibility
    families = [socket.AF_INET, socket.AF_INET6]
    for fam in families:
        try:
            results = socket.getaddrinfo(
                hostname, 1234, fam, socket.SOCK_STREAM  # dummy port number
            )
        except socket.gaierror as e:
            exc = e
        else:
            return results[0][4][0]

    raise exc


tblib.pickling_support.install()


def get_traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    bad = [
        os.path.join("distributed", "worker"),
        os.path.join("distributed", "scheduler"),
        os.path.join("tornado", "gen.py"),
        os.path.join("concurrent", "futures"),
        os.path.join("dask", "_task_spec"),
    ]
    while exc_traceback and any(
        b in exc_traceback.tb_frame.f_code.co_filename for b in bad
    ):
        exc_traceback = exc_traceback.tb_next
    return exc_traceback


def truncate_exception(e, n=10000):
    """Truncate exception to be about a certain length"""
    if len(str(e)) > n:
        try:
            return type(e)("Long error message", str(e)[:n])
        except Exception:
            return Exception("Long error message", type(e), str(e)[:n])
    else:
        return e


def seek_delimiter(file, delimiter, blocksize):
    """Seek current file to next byte after a delimiter bytestring

    This seeks the file to the next byte following the delimiter.  It does
    not return anything.  Use ``file.tell()`` to see location afterwards.

    Parameters
    ----------
    file: a file
    delimiter: bytes
        a delimiter like ``b'\n'`` or message sentinel
    blocksize: int
        Number of bytes to read from the file at once.
    """

    if file.tell() == 0:
        return

    last = b""
    while True:
        current = file.read(blocksize)
        if not current:
            return
        full = last + current
        try:
            i = full.index(delimiter)
            file.seek(file.tell() - (len(full) - i) + len(delimiter))
            return
        except ValueError:
            pass
        last = full[-len(delimiter) :]


def read_block(f, offset, length, delimiter=None):
    """Read a block of bytes from a file

    Parameters
    ----------
    f: file
        File-like object supporting seek, read, tell, etc..
    offset: int
        Byte offset to start read
    length: int
        Number of bytes to read
    delimiter: bytes (optional)
        Ensure reading starts and stops at delimiter bytestring

    If using the ``delimiter=`` keyword argument we ensure that the read
    starts and stops at delimiter boundaries that follow the locations
    ``offset`` and ``offset + length``.  If ``offset`` is zero then we
    start at zero.  The bytestring returned WILL include the
    terminating delimiter string.

    Examples
    --------

    >>> from io import BytesIO  # doctest: +SKIP
    >>> f = BytesIO(b'Alice, 100\\nBob, 200\\nCharlie, 300')  # doctest: +SKIP
    >>> read_block(f, 0, 13)  # doctest: +SKIP
    b'Alice, 100\\nBo'

    >>> read_block(f, 0, 13, delimiter=b'\\n')  # doctest: +SKIP
    b'Alice, 100\\nBob, 200\\n'

    >>> read_block(f, 10, 10, delimiter=b'\\n')  # doctest: +SKIP
    b'Bob, 200\\nCharlie, 300'
    """
    if delimiter:
        f.seek(offset)
        seek_delimiter(f, delimiter, 2**16)
        start = f.tell()
        length -= start - offset

        f.seek(start + length)
        seek_delimiter(f, delimiter, 2**16)
        end = f.tell()

        offset = start
        length = end - start

    f.seek(offset)
    bytes = f.read(length)
    return bytes


def ensure_bytes(s):
    """Attempt to turn `s` into bytes.

    Parameters
    ----------
    s : Any
        The object to be converted. Will correctly handled

        * str
        * bytes
        * objects implementing the buffer protocol (memoryview, ndarray, etc.)

    Returns
    -------
    b : bytes

    Raises
    ------
    TypeError
        When `s` cannot be converted

    Examples
    --------
    >>> ensure_bytes('123')
    b'123'
    >>> ensure_bytes(b'123')
    b'123'
    """
    warnings.warn(
        "`distributed.utils.ensure_bytes` is deprecated. "
        "Please switch to `dask.utils.ensure_bytes`. "
        "This will be removed in `2022.6.0`.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _ensure_bytes(s)


def ensure_memoryview(obj: bytes | bytearray | memoryview | PickleBuffer) -> memoryview:
    """Ensure `obj` is a 1-D contiguous `uint8` `memoryview`"""
    if not isinstance(obj, memoryview):
        obj = memoryview(obj)

    if not obj.nbytes:
        # Drop `obj` reference to permit freeing underlying data
        return memoryview(bytearray())
    elif not obj.contiguous:
        # Copy to contiguous form of expected shape & type
        return memoryview(bytearray(obj))
    elif obj.ndim != 1 or obj.format != "B":
        # Perform zero-copy reshape & cast
        # Use `PickleBuffer.raw()` as `memoryview.cast()` fails with F-order
        # xref: https://github.com/python/cpython/issues/91484
        return PickleBuffer(obj).raw()
    else:
        # Return `memoryview` as it already meets requirements
        return obj


def open_port(host: str = "") -> int:
    """Return a probably-open port

    There is a chance that this port will be taken by the operating system soon
    after returning from this function.
    """
    # http://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        s.listen(1)
        port = s.getsockname()[1]
        return port


def import_file(path: str) -> list[ModuleType]:
    """Loads modules for a file (.py, .zip, .egg)"""
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    names_to_import: list[str] = []
    tmp_python_path: str | None = None

    if ext in (".py",):  # , '.pyc'):
        if directory not in sys.path:
            tmp_python_path = directory
        names_to_import.append(name)
    if ext == ".py":  # Ensure that no pyc file will be reused
        cache_file = cache_from_source(path)
        with contextlib.suppress(OSError):
            os.remove(cache_file)
    if ext in (".egg", ".zip", ".pyz"):
        if path not in sys.path:
            sys.path.insert(0, path)
        names = (mod_info.name for mod_info in pkgutil.iter_modules([path]))
        names_to_import.extend(names)

    loaded: list[ModuleType] = []
    if not names_to_import:
        logger.warning("Found nothing to import from %s", filename)
    else:
        importlib.invalidate_caches()
        if tmp_python_path is not None:
            sys.path.insert(0, tmp_python_path)
        try:
            for name in names_to_import:
                logger.info("Reload module %s from %s file", name, ext)
                loaded.append(importlib.reload(importlib.import_module(name)))
        finally:
            if tmp_python_path is not None:
                sys.path.remove(tmp_python_path)
    return loaded


def asciitable(columns, rows):
    """Formats an ascii table for given columns and rows.

    Parameters
    ----------
    columns : list
        The column names
    rows : list of tuples
        The rows in the table. Each tuple must be the same length as
        ``columns``.
    """
    rows = [tuple(str(i) for i in r) for r in rows]
    columns = tuple(str(i) for i in columns)
    widths = tuple(max(max(map(len, x)), len(c)) for x, c in zip(zip(*rows), columns))
    row_template = ("|" + (" %%-%ds |" * len(columns))) % widths
    header = row_template % tuple(columns)
    bar = "+%s+" % "+".join("-" * (w + 2) for w in widths)
    data = "\n".join(row_template % r for r in rows)
    return "\n".join([bar, header, bar, data, bar])


def nbytes(frame, _bytes_like=(bytes, bytearray)):
    """Number of bytes of a frame or memoryview"""
    if isinstance(frame, _bytes_like):
        return len(frame)
    else:
        try:
            return frame.nbytes
        except AttributeError:
            return len(frame)


def json_load_robust(fn, load=json.load, timeout=None):
    """Reads a JSON file from disk that may be being written as we read"""
    deadline = Deadline.after(timeout)
    while not deadline.expires or deadline.remaining:
        if os.path.exists(fn):
            try:
                with open(fn) as f:
                    cfg = load(f)
                if cfg:
                    return cfg
            except (ValueError, KeyError):  # race with writing process
                pass
        sleep(0.1)
    else:
        raise TimeoutError(f"Could not load file after {timeout}s.")


class DequeHandler(logging.Handler):
    """A logging.Handler that records records into a deque"""

    _instances: ClassVar[weakref.WeakSet[DequeHandler]] = weakref.WeakSet()

    def __init__(self, *args, n=10000, **kwargs):
        self.deque = deque(maxlen=n)
        super().__init__(*args, **kwargs)
        self._instances.add(self)

    def emit(self, record):
        self.deque.append(record)

    def clear(self):
        """
        Clear internal storage.
        """
        self.deque.clear()

    @classmethod
    def clear_all_instances(cls):
        """
        Clear the internal storage of all live DequeHandlers.
        """
        for inst in list(cls._instances):
            inst.clear()


def reset_logger_locks():
    """Python 2's logger's locks don't survive a fork event

    https://github.com/dask/distributed/issues/1491
    """
    for name in logging.Logger.manager.loggerDict.keys():
        for handler in logging.getLogger(name).handlers:
            handler.createLock()


@functools.lru_cache(1000)
def has_keyword(func, keyword):
    return keyword in inspect.signature(func).parameters


@functools.lru_cache(1000)
def command_has_keyword(cmd, k):
    # Click is a relatively expensive import
    # That hurts startup time a little
    import click

    if cmd is not None:
        if isinstance(cmd, str):
            try:
                from importlib import import_module

                cmd = import_module(cmd)
            except ImportError:
                raise ImportError("Module for command %s is not available" % cmd)

        if isinstance(cmd.main, click.core.Command):
            cmd = cmd.main
        if isinstance(cmd, click.core.Command):
            cmd_params = {
                p.human_readable_name
                for p in cmd.params
                if isinstance(p, click.core.Option)
            }
            return k in cmd_params

    return False


# from bokeh.palettes import viridis
# palette = viridis(18)
palette = [
    "#440154",
    "#471669",
    "#472A79",
    "#433C84",
    "#3C4D8A",
    "#355D8C",
    "#2E6C8E",
    "#287A8E",
    "#23898D",
    "#1E978A",
    "#20A585",
    "#2EB27C",
    "#45BF6F",
    "#64CB5D",
    "#88D547",
    "#AFDC2E",
    "#D7E219",
    "#FDE724",
]


@toolz.memoize
def color_of(x, palette=palette):
    h = md5(str(x).encode(), usedforsecurity=False)
    n = int(h.hexdigest()[:8], 16)
    return palette[n % len(palette)]


def iscoroutinefunction(f):
    return inspect.iscoroutinefunction(f) or gen.is_coroutine_function(f)


@contextlib.contextmanager
def warn_on_duration(duration: str | float | timedelta, msg: str) -> Iterator[None]:
    """Generate a UserWarning if the operation in this context takes longer than
    *duration* and print *msg*

    The message may include a format string `{duration}` which will be formatted
    to include the actual duration it took
    """
    start = time()
    yield
    stop = time()
    diff = stop - start
    if diff > _parse_timedelta(duration):
        warnings.warn(msg.format(duration=diff), stacklevel=2)


def format_dashboard_link(host, port):
    template = dask.config.get("distributed.dashboard.link")
    if dask.config.get("distributed.scheduler.dashboard.tls.cert"):
        scheme = "https"
    else:
        scheme = "http"
    return template.format(
        **toolz.merge(os.environ, dict(scheme=scheme, host=host, port=port))
    )


def parse_ports(port: int | str | Collection[int] | None) -> list[int] | list[None]:
    """Parse input port information into list of ports

    Parameters
    ----------
    port : int, str, list[int], None
        Input port or ports. Can be an integer like 8787, a string for a
        single port like "8787", string for a sequential range of ports like
        "8000:8200", a collection of ints, or None.

    Returns
    -------
    ports : list
        List of ports

    Examples
    --------
    A single port can be specified using an integer:

    >>> parse_ports(8787)
    [8787]

    or a string:

    >>> parse_ports("8787")
    [8787]

    A sequential range of ports can be specified by a string which indicates
    the first and last ports which should be included in the sequence of ports:

    >>> parse_ports("8787:8790")
    [8787, 8788, 8789, 8790]

    An input of ``None`` is also valid and can be used to indicate that no port
    has been specified:

    >>> parse_ports(None)
    [None]

    """
    if isinstance(port, str) and ":" in port:
        port_start, port_stop = map(int, port.split(":"))
        if port_stop <= port_start:
            raise ValueError(
                "When specifying a range of ports like port_start:port_stop, "
                "port_stop must be greater than port_start, but got "
                f"{port_start=} and {port_stop=}"
            )
        return list(range(port_start, port_stop + 1))

    if isinstance(port, str):
        return [int(port)]

    if isinstance(port, int) or port is None:
        return [port]  # type: ignore

    if isinstance(port, Collection):
        if not all(isinstance(p, int) for p in port):
            raise TypeError(port)
        return list(port)  # type: ignore

    raise TypeError(port)


is_coroutine_function = iscoroutinefunction


class Log(str):
    """A container for newline-delimited string of log entries"""

    def _repr_html_(self):
        return get_template("log.html.j2").render(log=self)


class Logs(dict):
    """A container for a dict mapping names to strings of log entries"""

    def _repr_html_(self):
        return get_template("logs.html.j2").render(logs=self)


def cli_keywords(
    d: dict[str, AnyType],
    cls: Callable | None = None,
    cmd: str | ModuleType | None = None,
) -> list[str]:
    """Convert a kwargs dictionary into a list of CLI keywords

    Parameters
    ----------
    d : dict
        The keywords to convert
    cls : callable
        The callable that consumes these terms to check them for validity
    cmd : string or object
        A string with the name of a module, or the module containing a
        click-generated command with a "main" function, or the function itself.
        It may be used to parse a module's custom arguments (that is, arguments that
        are not part of Worker class), such as nworkers from dask worker CLI or
        enable_nvlink from dask-cuda-worker CLI.

    Examples
    --------
    >>> cli_keywords({"x": 123, "save_file": "foo.txt"})
    ['--x', '123', '--save-file', 'foo.txt']

    >>> from dask.distributed import Worker
    >>> cli_keywords({"x": 123}, Worker)
    Traceback (most recent call last):
    ...
    ValueError: Class distributed.worker.Worker does not support keyword x
    """
    from dask.utils import typename

    if cls or cmd:
        for k in d:
            if not has_keyword(cls, k) and not command_has_keyword(cmd, k):
                if cls and cmd:
                    raise ValueError(
                        "Neither class %s or module %s support keyword %s"
                        % (typename(cls), typename(cmd), k)
                    )
                elif cls:
                    raise ValueError(
                        f"Class {typename(cls)} does not support keyword {k}"
                    )
                else:
                    raise ValueError(
                        f"Module {typename(cmd)} does not support keyword {k}"
                    )

    def convert_value(v):
        out = str(v)
        if " " in out and "'" not in out and '"' not in out:
            out = '"' + out + '"'
        return out

    return sum(
        (["--" + k.replace("_", "-"), convert_value(v)] for k, v in d.items()), []
    )


def is_valid_xml(text):
    return xml.etree.ElementTree.fromstring(text) is not None


_offload_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="Dask-Offload")


def import_term(name: str) -> AnyType:
    """Return the fully qualified term

    Examples
    --------
    >>> import_term("math.sin") # doctest: +SKIP
    <function math.sin(x, /)>
    """
    try:
        module_name, attr_name = name.rsplit(".", 1)
    except ValueError:
        return importlib.import_module(name)

    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


async def run_in_executor_with_context(
    executor: Executor | None,
    func: Callable[P, T],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """Variant of :meth:`~asyncio.AbstractEventLoop.run_in_executor`, which
    propagates contextvars.
    Note that this limits the type of Executor to those that do not pickle objects.

    See also
    --------
    asyncio.AbstractEventLoop.run_in_executor
    offload
    https://bugs.python.org/issue34014
    """
    loop = asyncio.get_running_loop()
    context = contextvars.copy_context()
    return await loop.run_in_executor(
        executor, lambda: context.run(func, *args, **kwargs)
    )


def offload(
    func: Callable[P, T],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> Awaitable[T]:
    """Run a synchronous function in a separate thread.
    Unlike :meth:`asyncio.to_thread`, this propagates contextvars and offloads to an
    ad-hoc thread pool with a single worker.

    See also
    --------
    asyncio.to_thread
    run_in_executor_with_context
    https://bugs.python.org/issue34014
    """
    with context_meter.meter("offload"):
        return run_in_executor_with_context(_offload_executor, func, *args, **kwargs)


class EmptyContext:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass


empty_context = EmptyContext()


def clean_dashboard_address(addrs: AnyType, default_listen_ip: str = "") -> list[dict]:
    """
    Examples
    --------
    >>> clean_dashboard_address(8787)
    [{'address': '', 'port': 8787}]
    >>> clean_dashboard_address(":8787")
    [{'address': '', 'port': 8787}]
    >>> clean_dashboard_address("8787")
    [{'address': '', 'port': 8787}]
    >>> clean_dashboard_address("8787")
    [{'address': '', 'port': 8787}]
    >>> clean_dashboard_address("foo:8787")
    [{'address': 'foo', 'port': 8787}]
    >>> clean_dashboard_address([8787, 8887])
    [{'address': '', 'port': 8787}, {'address': '', 'port': 8887}]
    >>> clean_dashboard_address(":8787,:8887")
    [{'address': '', 'port': 8787}, {'address': '', 'port': 8887}]
    """

    if default_listen_ip == "0.0.0.0":
        default_listen_ip = ""  # for IPV6

    if isinstance(addrs, str):
        addrs = addrs.split(",")
    if not isinstance(addrs, list):
        addrs = [addrs]

    addresses = []
    for addr in addrs:
        try:
            addr = int(addr)
        except (TypeError, ValueError):
            pass

        if isinstance(addr, str):
            addr = addr.split(":")

        if isinstance(addr, (tuple, list)):
            if len(addr) == 2:
                host, port = (addr[0], int(addr[1]))
            elif len(addr) == 1:
                [host], port = addr, 0
            else:
                raise ValueError(addr)
        elif isinstance(addr, int):
            host = default_listen_ip
            port = addr

        addresses.append({"address": host, "port": port})
    return addresses


_deprecations = {
    "no_default": "dask.typing.no_default",
}


def __getattr__(name):
    if name in _deprecations:
        use_instead = _deprecations[name]

        warnings.warn(
            f"{name} is deprecated and will be removed in a future release. "
            f"Please use {use_instead} instead.",
            category=FutureWarning,
            stacklevel=2,
        )
        return import_term(use_instead)
    else:
        raise AttributeError(f"module {__name__} has no attribute {name}")


# Used internally by recursive_to_dict to stop infinite recursion. If an object has
# already been encountered, a string representation will be returned instead. This is
# necessary since we have multiple cyclic referencing data structures.
_recursive_to_dict_seen: ContextVar[set[int]] = ContextVar("_recursive_to_dict_seen")
_to_dict_no_nest_flag = False


def recursive_to_dict(
    obj: AnyType, *, exclude: Container[str] = (), members: bool = False
) -> AnyType:
    """Recursively convert arbitrary Python objects to a JSON-serializable
    representation. This is intended for debugging purposes only.

    The following objects are supported:

    list, tuple, set, frozenset, deque, dict, dict_keys, dict_values
        Descended into these objects recursively. Python-specific collections are
        converted to JSON-friendly variants.
    Classes that define ``_to_dict(self, *, exclude: Container[str] = ())``:
        Call the method and dump its output
    Classes that define ``_to_dict_no_nest(self, *, exclude: Container[str] = ())``:
        Like above, but prevents nested calls (see below)
    Other Python objects
        Dump the output of ``repr()``
    Objects already encountered before, regardless of type
        Dump the output of ``repr()``. This breaks circular references and shortens the
        output.

    Parameters
    ----------
    exclude:
        A list of attribute names to be excluded from the dump.
        This will be forwarded to the objects ``_to_dict`` methods and these methods
        are required to accept this parameter.
    members:
        If True, convert the top-level Python object to a dict of its public members

    **``_to_dict_no_nest`` vs. ``_to_dict``**

    The presence of the ``_to_dict_no_nest`` method signals ``recursive_to_dict`` to
    have a mutually exclusive full dict representation with other objects that also have
    the ``_to_dict_no_nest``, regardless of their class. Only the outermost object in a
    nested structure has the method invoked; all others are
    dumped as their string repr instead, even if they were not encountered before.

    Example:

    .. code-block:: python

        >>> class Person:
        ...     def __init__(self, name):
        ...         self.name = name
        ...         self.children = []
        ...         self.pets = []
        ...
        ...     def _to_dict_no_nest(self, exclude=()):
        ...         return recursive_to_dict(self.__dict__, exclude=exclude)
        ...
        ...     def __repr__(self):
        ...         return self.name

        >>> class Pet:
        ...     def __init__(self, name):
        ...         self.name = name
        ...         self.owners = []
        ...
        ...     def _to_dict_no_nest(self, exclude=()):
        ...         return recursive_to_dict(self.__dict__, exclude=exclude)
        ...
        ...     def __repr__(self):
        ...         return self.name

        >>> alice = Person("Alice")
        >>> bob = Person("Bob")
        >>> charlie = Pet("Charlie")
        >>> alice.children.append(bob)
        >>> alice.pets.append(charlie)
        >>> bob.pets.append(charlie)
        >>> charlie.owners[:] = [alice, bob]
        >>> recursive_to_dict({"people": [alice, bob], "pets": [charlie]})
        {
            "people": [
                {"name": "Alice", "children": ["Bob"], "pets": ["Charlie"]},
                {"name": "Bob", "children": [], "pets": ["Charlie"]},
            ],
            "pets": [
                {"name": "Charlie", "owners": ["Alice", "Bob"]},
            ],
        }

    If we changed the methods to ``_to_dict``, the output would instead be:

    .. code-block:: python

        {
            "people": [
                {
                    "name": "Alice",
                    "children": [
                        {
                            "name": "Bob",
                            "children": [],
                            "pets": [{"name": "Charlie", "owners": ["Alice", "Bob"]}],
                        },
                    ],
                    pets: ["Charlie"],
                ],
                "Bob",
            ],
            "pets": ["Charlie"],
        }

    Also notice that, if in the future someone will swap the creation of the
    ``children`` and ``pets`` attributes inside ``Person.__init__``, the output with
    ``_to_dict`` will change completely whereas the one with ``_to_dict_no_nest`` won't!
    """
    if isinstance(obj, (int, float, bool, str)) or obj is None:
        return obj
    if isinstance(obj, (type, bytes)):
        return repr(obj)

    if members:
        obj = {
            k: v
            for k, v in inspect.getmembers(obj)
            if not k.startswith("_") and k not in exclude and not callable(v)
        }

    # Prevent infinite recursion
    try:
        seen = _recursive_to_dict_seen.get()
    except LookupError:
        seen = set()
    seen = seen.copy()
    tok = _recursive_to_dict_seen.set(seen)
    try:
        if id(obj) in seen:
            return repr(obj)

        if hasattr(obj, "_to_dict_no_nest"):
            global _to_dict_no_nest_flag
            if _to_dict_no_nest_flag:
                return repr(obj)

            seen.add(id(obj))
            _to_dict_no_nest_flag = True
            try:
                return obj._to_dict_no_nest(exclude=exclude)
            finally:
                _to_dict_no_nest_flag = False

        seen.add(id(obj))

        if hasattr(obj, "_to_dict"):
            return obj._to_dict(exclude=exclude)
        if isinstance(obj, (list, tuple, set, frozenset, deque, KeysView, ValuesView)):
            return [recursive_to_dict(el, exclude=exclude) for el in obj]
        if isinstance(obj, dict):
            res = {}
            for k, v in obj.items():
                k = recursive_to_dict(k, exclude=exclude)
                v = recursive_to_dict(v, exclude=exclude)
                try:
                    res[k] = v
                except TypeError:
                    res[str(k)] = v
            return res

        return repr(obj)
    finally:
        tok.var.reset(tok)


class Deadline:
    """Utility class tracking a deadline and the progress toward it"""

    #: Expiry time of the deadline in seconds since the start of the monotonic timer
    #: or None if the deadline never expires
    expires_at_mono: float | None
    #: Seconds since the epoch when the deadline was created
    started_at_mono: float
    #: Seconds since the start of the monotonic timer when the deadline was created
    started_at: float

    __slots__ = tuple(__annotations__)

    def __init__(self, expires_at: float | None = None):
        self.started_at = time()
        self.started_at_mono = monotonic()
        if expires_at is not None:
            self.expires_at_mono = expires_at - self.started_at + self.started_at_mono
        else:
            self.expires_at_mono = None

    @classmethod
    def after(cls, duration: float | None = None) -> Deadline:
        """Create a new ``Deadline`` that expires in ``duration`` seconds
        or never if ``duration`` is None
        """
        inst = cls()
        if duration is not None:
            inst.expires_at_mono = inst.started_at_mono + duration
        return inst

    @property
    def expires_at(self) -> float | None:
        """Expiry time of the deadline in seconds since the unix epoch
        or None if the deadline never expires.

        Note that this can change over time if the wall clock is adjusted by the OS.
        """
        if (exp := self.expires_at_mono) is None:
            return None
        return exp - monotonic() + time()

    @property
    def duration(self) -> float | None:
        """Seconds between the creation and expiration time of the deadline
        if the deadline expires, None otherwise
        """
        if (exp := self.expires_at_mono) is None:
            return None
        return exp - self.started_at_mono

    @property
    def expires(self) -> bool:
        """Whether the deadline ever expires"""
        return self.expires_at_mono is not None

    @property
    def elapsed(self) -> float:
        """Seconds that elapsed since the deadline was created"""
        return monotonic() - self.started_at_mono

    @property
    def remaining(self) -> float | None:
        """Seconds remaining until the deadline expires if an expiry time is set,
        None otherwise
        """
        if (exp := self.expires_at_mono) is None:
            return None
        else:
            return max(0, exp - monotonic())

    @property
    def expired(self) -> bool:
        """Whether the deadline has already expired"""
        return self.remaining == 0


class RateLimiterFilter(logging.Filter):
    """A Logging filter that ensures a matching message is emitted at most every
    `rate` seconds"""

    pattern: re.Pattern
    rate: float
    _last_seen: float

    def __init__(self, pattern: str, *, name: str = "", rate: str | float = "10s"):
        super().__init__(name)
        self.pattern = re.compile(pattern)
        self.rate = _parse_timedelta(rate)
        self._last_seen = -self.rate

    def filter(self, record: logging.LogRecord) -> bool:
        if self.pattern.match(record.msg):
            now = monotonic()
            if now - self._last_seen < self.rate:
                return False
            self._last_seen = now
        return True

    @classmethod
    def reset_timer(cls, logger: logging.Logger | str) -> None:
        """Reset the timer on all RateLimiterFilters on a logger.
        Useful in unit testing.
        """
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        for filter in logger.filters:
            if isinstance(filter, cls):
                filter._last_seen = -filter.rate


if sys.version_info >= (3, 11):

    async def wait_for(fut: Awaitable[T], timeout: float) -> T:
        async with asyncio.timeout(timeout):
            return await fut

else:

    async def wait_for(fut: Awaitable[T], timeout: float) -> T:
        return await asyncio.wait_for(fut, timeout)


class TupleComparable:
    """Wrap object so that we can compare tuple, int or None

    When comparing two objects of different types Python fails

    >>> (1, 2) < 1
    Traceback (most recent call last):
        ...
    TypeError: '<' not supported between instances of 'tuple' and 'int'

    This class replaces None with 0, and wraps ints with tuples

    >>> TupleComparable((1, 2)) < TupleComparable(1)
    False
    """

    __slots__ = ("obj",)

    def __init__(self, obj):
        if obj is None:
            self.obj = (0,)
        elif isinstance(obj, tuple):
            self.obj = obj
        elif isinstance(obj, (int, float)):
            self.obj = (obj,)
        else:
            raise ValueError(f"Object must be tuple, int, float or None, got {obj}")

    def __eq__(self, other):
        return self.obj == other.obj

    def __lt__(self, other):
        return self.obj < other.obj


@functools.cache
def url_escape(url, *args, **kwargs):
    """
    Escape a URL path segment. Cache results for better performance.
    """
    return escape.url_escape(url, *args, **kwargs)
