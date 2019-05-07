from __future__ import print_function, division, absolute_import

import atexit
from collections import deque
from contextlib import contextmanager
from datetime import timedelta
import functools
from hashlib import md5
import inspect
import json
import logging
import multiprocessing
from numbers import Number
import operator
import os
import re
import shutil
import socket
from time import sleep
from importlib import import_module
import sys
import tempfile
import threading
import warnings
import weakref

import six
import tblib.pickling_support

from .compatibility import cache_from_source, getargspec, invalidate_caches, reload

try:
    import resource
except ImportError:
    resource = None

import dask
from dask import istask
import toolz
import tornado
from tornado import gen
from tornado.ioloop import IOLoop

try:
    from tornado.ioloop import PollIOLoop
except ImportError:
    PollIOLoop = None  # dropped in tornado 6.0

from .compatibility import Queue, PY3, PY2, get_thread_identity, unicode
from .metrics import time


try:
    from dask.context import thread_state
except ImportError:
    thread_state = threading.local()

logger = _logger = logging.getLogger(__name__)


no_default = "__no_default__"


def _initialize_mp_context():
    if PY3 and not sys.platform.startswith("win") and "PyPy" not in sys.version:
        method = dask.config.get("distributed.worker.multiprocessing-method")
        ctx = multiprocessing.get_context(method)
        # Makes the test suite much faster
        preload = ["distributed"]
        if "pkg_resources" in sys.modules:
            preload.append("pkg_resources")
        ctx.set_forkserver_preload(preload)
    else:
        ctx = multiprocessing

    return ctx


mp_context = _initialize_mp_context()


def funcname(func):
    """Get the name of a function."""
    while hasattr(func, "func"):
        func = func.func
    try:
        return func.__name__
    except AttributeError:
        return str(func)


def has_arg(func, argname):
    """
    Whether the function takes an argument with the given name.
    """
    while True:
        try:
            if argname in getargspec(func).args:
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
def _get_ip(host, port, family, default):
    # By using a UDP socket, we don't actually try to connect but
    # simply select the local address through which *host* is reachable.
    sock = socket.socket(family, socket.SOCK_DGRAM)
    try:
        sock.connect((host, port))
        ip = sock.getsockname()[0]
        return ip
    except EnvironmentError as e:
        # XXX Should first try getaddrinfo() on socket.gethostname() and getfqdn()
        warnings.warn(
            "Couldn't detect a suitable IP address for "
            "reaching %r, defaulting to %r: %s" % (host, default, e),
            RuntimeWarning,
        )
        return default
    finally:
        sock.close()


def get_ip(host="8.8.8.8", port=80):
    """
    Get the local IP address through which the *host* is reachable.

    *host* defaults to a well-known Internet host (one of Google's public
    DNS servers).
    """
    return _get_ip(host, port, family=socket.AF_INET, default="127.0.0.1")


def get_ipv6(host="2001:4860:4860::8888", port=80):
    """
    The same as get_ip(), but for IPv6.
    """
    return _get_ip(host, port, family=socket.AF_INET6, default="::1")


def get_ip_interface(ifname):
    """
    Get the local IPv4 address of a network interface.

    KeyError is raised if the interface doesn't exist.
    ValueError is raised if the interface does no have an IPv4 address
    associated with it.
    """
    import psutil

    for info in psutil.net_if_addrs()[ifname]:
        if info.family == socket.AF_INET:
            return info.address
    raise ValueError("interface %r doesn't have an IPv4 address" % (ifname,))


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions as e:
        pass


@gen.coroutine
def ignore_exceptions(coroutines, *exceptions):
    """ Process list of coroutines, ignoring certain exceptions

    >>> coroutines = [cor(...) for ...]  # doctest: +SKIP
    >>> x = yield ignore_exceptions(coroutines, TypeError)  # doctest: +SKIP
    """
    wait_iterator = gen.WaitIterator(*coroutines)
    results = []
    while not wait_iterator.done():
        with ignoring(*exceptions):
            result = yield wait_iterator.next()
            results.append(result)
    raise gen.Return(results)


@gen.coroutine
def All(args, quiet_exceptions=()):
    """ Wait on many tasks at the same time

    Err once any of the tasks err.

    See https://github.com/tornadoweb/tornado/issues/1546

    Parameters
    ----------
    args: futures to wait for
    quiet_exceptions: tuple, Exception
        Exception types to avoid logging if they fail
    """
    tasks = gen.WaitIterator(*args)
    results = [None for _ in args]
    while not tasks.done():
        try:
            result = yield tasks.next()
        except Exception:

            @gen.coroutine
            def quiet():
                """ Watch unfinished tasks

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
    raise gen.Return(results)


@gen.coroutine
def Any(args, quiet_exceptions=()):
    """ Wait on many tasks at the same time and return when any is finished

    Err once any of the tasks err.

    Parameters
    ----------
    args: futures to wait for
    quiet_exceptions: tuple, Exception
        Exception types to avoid logging if they fail
    """
    tasks = gen.WaitIterator(*args)
    results = [None for _ in args]
    while not tasks.done():
        try:
            result = yield tasks.next()
        except Exception:

            @gen.coroutine
            def quiet():
                """ Watch unfinished tasks

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
    raise gen.Return(results)


def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    # Tornado's PollIOLoop doesn't raise when using closed, do it ourselves
    if PollIOLoop and (
        (isinstance(loop, PollIOLoop) and getattr(loop, "_closing", False))
        or (hasattr(loop, "asyncio_loop") and loop.asyncio_loop._closed)
    ):
        raise RuntimeError("IOLoop is closed")
    try:
        if loop.asyncio_loop.is_closed():  # tornado 6
            raise RuntimeError("IOLoop is closed")
    except AttributeError:
        pass

    timeout = kwargs.pop("callback_timeout", None)

    e = threading.Event()
    main_tid = get_thread_identity()
    result = [None]
    error = [False]

    @gen.coroutine
    def f():
        try:
            if main_tid == get_thread_identity():
                raise RuntimeError("sync() called from thread of running loop")
            yield gen.moment
            thread_state.asynchronous = True
            future = func(*args, **kwargs)
            if timeout is not None:
                future = gen.with_timeout(timedelta(seconds=timeout), future)
            result[0] = yield future
        except Exception as exc:
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    loop.add_callback(f)
    if timeout is not None:
        if not e.wait(timeout):
            raise gen.TimeoutError("timed out after %s s." % (timeout,))
    else:
        while not e.is_set():
            e.wait(10)
    if error[0]:
        six.reraise(*error[0])
    else:
        return result[0]


class LoopRunner(object):
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
    _all_loops = weakref.WeakKeyDictionary()
    _lock = threading.Lock()

    def __init__(self, loop=None, asynchronous=False):
        current = IOLoop.current()
        if loop is None:
            if asynchronous:
                self._loop = current
            else:
                # We're expecting the loop to run in another thread,
                # avoid re-using this thread's assigned loop
                self._loop = IOLoop()
            self._should_close_loop = True
        else:
            self._loop = loop
            self._should_close_loop = False
        self._asynchronous = asynchronous
        self._loop_thread = None
        self._started = False
        with self._lock:
            self._all_loops.setdefault(self._loop, (0, None))

    def start(self):
        """
        Start the IO loop if required.  The loop is run in a dedicated
        thread.

        If the loop is already running, this method does nothing.
        """
        with self._lock:
            self._start_unlocked()

    def _start_unlocked(self):
        assert not self._started

        count, real_runner = self._all_loops[self._loop]
        if self._asynchronous or real_runner is not None or count > 0:
            self._all_loops[self._loop] = count + 1, real_runner
            self._started = True
            return

        assert self._loop_thread is None
        assert count == 0

        loop_evt = threading.Event()
        done_evt = threading.Event()
        in_thread = [None]
        start_exc = [None]

        def loop_cb():
            in_thread[0] = threading.current_thread()
            loop_evt.set()

        def run_loop(loop=self._loop):
            loop.add_callback(loop_cb)
            try:
                loop.start()
            except Exception as e:
                start_exc[0] = e
            finally:
                done_evt.set()

        thread = threading.Thread(target=run_loop, name="IO loop")
        thread.daemon = True
        thread.start()

        loop_evt.wait(timeout=10)
        self._started = True

        actual_thread = in_thread[0]
        if actual_thread is not thread:
            # Loop already running in other thread (user-launched)
            done_evt.wait(5)
            if not isinstance(start_exc[0], RuntimeError):
                if not isinstance(
                    start_exc[0], Exception
                ):  # track down infrequent error
                    raise TypeError("not an exception", start_exc[0])
                raise start_exc[0]
            self._all_loops[self._loop] = count + 1, None
        else:
            assert start_exc[0] is None, start_exc
            self._loop_thread = thread
            self._all_loops[self._loop] = count + 1, self

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

        count, real_runner = self._all_loops[self._loop]
        if count > 1:
            self._all_loops[self._loop] = count - 1, real_runner
        else:
            assert count == 1
            del self._all_loops[self._loop]
            if real_runner is not None:
                real_runner._real_stop(timeout)

    def _real_stop(self, timeout):
        assert self._loop_thread is not None
        if self._loop_thread is not None:
            try:
                self._loop.add_callback(self._loop.stop)
                self._loop_thread.join(timeout=timeout)
                with ignoring(KeyError):  # IOLoop can be missing
                    self._loop.close()
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
        return self._loop


@contextmanager
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


@contextmanager
def tmp_text(filename, text):
    fn = os.path.join(tempfile.gettempdir(), filename)
    with open(fn, "w") as f:
        f.write(text)

    try:
        yield fn
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def clear_queue(q):
    while not q.empty():
        q.get_nowait()


def is_kernel():
    """ Determine if we're running within an IPython kernel

    >>> is_kernel()
    False
    """
    # http://stackoverflow.com/questions/34091701/determine-if-were-in-an-ipython-notebook-session
    if "IPython" not in sys.modules:  # IPython hasn't been imported
        return False
    from IPython import get_ipython

    # check for `kernel` attribute on the IPython instance
    return getattr(get_ipython(), "kernel", None) is not None


hex_pattern = re.compile("[a-f]+")


def key_split(s):
    """
    >>> key_split('x')
    'x'
    >>> key_split('x-1')
    'x'
    >>> key_split('x-1-2-3')
    'x'
    >>> key_split(('x-2', 1))
    'x'
    >>> key_split("('x-2', 1)")
    'x'
    >>> key_split("('x', 1)")
    'x'
    >>> key_split('hello-world-1')
    'hello-world'
    >>> key_split(b'hello-world-1')
    'hello-world'
    >>> key_split('ae05086432ca935f6eba409a8ecd4896')
    'data'
    >>> key_split('<module.submodule.myclass object at 0xdaf372')
    'myclass'
    >>> key_split(None)
    'Other'
    >>> key_split('x-abcdefab')  # ignores hex
    'x'
    """
    if type(s) is bytes:
        s = s.decode()
    if type(s) is tuple:
        s = s[0]
    try:
        words = s.split("-")
        if not words[0][0].isalpha():
            result = words[0].split(",")[0].strip("'(\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (
                len(word) == 8 and hex_pattern.match(word) is not None
            ):
                result += "-" + word
            else:
                break
        if len(result) == 32 and re.match(r"[a-f0-9]{32}", result):
            return "data"
        else:
            if result[0] == "<":
                result = result.strip("<>").split()[0].split(".")[-1]
            return result
    except Exception:
        return "Other"


try:
    from functools import lru_cache
except ImportError:
    lru_cache = False
    pass
else:
    key_split = lru_cache(100000)(key_split)

if PY3:

    def key_split_group(x):
        """A more fine-grained version of key_split

        >>> key_split_group('x')
        'x'
        >>> key_split_group('x-1')
        'x-1'
        >>> key_split_group('x-1-2-3')
        'x-1-2-3'
        >>> key_split_group(('x-2', 1))
        'x-2'
        >>> key_split_group("('x-2', 1)")
        'x-2'
        >>> key_split_group('hello-world-1')
        'hello-world-1'
        >>> key_split_group(b'hello-world-1')
        'hello-world-1'
        >>> key_split_group('ae05086432ca935f6eba409a8ecd4896')
        'data'
        >>> key_split_group('<module.submodule.myclass object at 0xdaf372')
        'myclass'
        >>> key_split_group(None)
        'Other'
        >>> key_split_group('x-abcdefab')  # ignores hex
        'x-abcdefab'
        """
        typ = type(x)
        if typ is tuple:
            return x[0]
        elif typ is str:
            if x[0] == "(":
                return x.split(",", 1)[0].strip("()\"'")
            elif len(x) == 32 and re.match(r"[a-f0-9]{32}", x):
                return "data"
            elif x[0] == "<":
                return x.strip("<>").split()[0].split(".")[-1]
            else:
                return x
        elif typ is bytes:
            return key_split_group(x.decode())
        else:
            return "Other"


else:

    def key_split_group(x):
        """A more fine-grained version of key_split

        >>> key_split_group('x')
        'x'
        >>> key_split_group('x-1')
        'x-1'
        >>> key_split_group('x-1-2-3')
        'x-1-2-3'
        >>> key_split_group(('x-2', 1))
        'x-2'
        >>> key_split_group("('x-2', 1)")
        'x-2'
        >>> key_split_group('hello-world-1')
        'hello-world-1'
        >>> key_split_group(b'hello-world-1')
        'hello-world-1'
        >>> key_split_group('ae05086432ca935f6eba409a8ecd4896')
        'data'
        >>> key_split_group('<module.submodule.myclass object at 0xdaf372')
        'myclass'
        >>> key_split_group(None)
        'Other'
        >>> key_split_group('x-abcdefab')  # ignores hex
        'x-abcdefab'
        """
        typ = type(x)
        if typ is tuple:
            return x[0]
        elif typ is str or typ is unicode:
            if x[0] == "(":
                return x.split(",", 1)[0].strip("()\"'")
            elif len(x) == 32 and re.match(r"[a-f0-9]{32}", x):
                return "data"
            elif x[0] == "<":
                return x.strip("<>").split()[0].split(".")[-1]
            else:
                return x
        else:
            return "Other"


@contextmanager
def log_errors(pdb=False):
    from .comm import CommClosedError

    try:
        yield
    except (CommClosedError, gen.Return):
        raise
    except Exception as e:
        try:
            logger.exception(e)
        except TypeError:  # logger becomes None during process cleanup
            pass
        if pdb:
            import pdb

            pdb.set_trace()
        raise


def silence_logging(level, root="distributed"):
    """
    Change all StreamHandlers for the given logger to the given level
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())

    old = None
    logger = logging.getLogger(root)
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            old = handler.level
            handler.setLevel(level)

    return old


@toolz.memoize
def ensure_ip(hostname):
    """ Ensure that address is an IP address

    Examples
    --------
    >>> ensure_ip('localhost')
    '127.0.0.1'
    >>> ensure_ip('123.123.123.123')  # pass through IP addresses
    '123.123.123.123'
    """
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
    ]
    while exc_traceback and any(
        b in exc_traceback.tb_frame.f_code.co_filename for b in bad
    ):
        exc_traceback = exc_traceback.tb_next
    return exc_traceback


def truncate_exception(e, n=10000):
    """ Truncate exception to be about a certain length """
    if len(str(e)) > n:
        try:
            return type(e)("Long error message", str(e)[:n])
        except Exception:
            return Exception("Long error message", type(e), str(e)[:n])
    else:
        return e


if sys.version_info >= (3,):
    # (re-)raising StopIteration is deprecated in 3.6+
    exec(
        """def queue_to_iterator(q):
        while True:
            result = q.get()
            if isinstance(result, StopIteration):
                return result.value
            yield result
        """
    )
else:
    # Returning non-None from generator is a syntax error in 2.x
    def queue_to_iterator(q):
        while True:
            result = q.get()
            if isinstance(result, StopIteration):
                raise result
            yield result


def _dump_to_queue(seq, q):
    for item in seq:
        q.put(item)


def iterator_to_queue(seq, maxsize=0):
    q = Queue(maxsize=maxsize)

    t = threading.Thread(target=_dump_to_queue, args=(seq, q))
    t.daemon = True
    t.start()

    return q


def tokey(o):
    """ Convert an object to a string.

    Examples
    --------

    >>> tokey(b'x')
    'x'
    >>> tokey('x')
    'x'
    >>> tokey(1)
    '1'
    """
    typ = type(o)
    if typ is unicode or typ is bytes:
        return o
    else:
        return str(o)


def validate_key(k):
    """Validate a key as received on a stream.
    """
    typ = type(k)
    if typ is not unicode and typ is not bytes:
        raise TypeError("Unexpected key type %s (value: %r)" % (typ, k))


def _maybe_complex(task):
    """ Possibly contains a nested task """
    return (
        istask(task)
        or type(task) is list
        and any(map(_maybe_complex, task))
        or type(task) is dict
        and any(map(_maybe_complex, task.values()))
    )


def convert(task, dsk, extra_values):
    if type(task) is list:
        return [convert(v, dsk, extra_values) for v in task]
    if type(task) is dict:
        return {k: convert(v, dsk, extra_values) for k, v in task.items()}
    if istask(task):
        return (task[0],) + tuple(convert(x, dsk, extra_values) for x in task[1:])
    try:
        if task in dsk or task in extra_values:
            return tokey(task)
    except TypeError:
        pass
    return task


def str_graph(dsk, extra_values=()):
    return {tokey(k): convert(v, dsk, extra_values) for k, v in dsk.items()}


def seek_delimiter(file, delimiter, blocksize):
    """ Seek current file to next byte after a delimiter bytestring

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
    """ Read a block of bytes from a file

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
        seek_delimiter(f, delimiter, 2 ** 16)
        start = f.tell()
        length -= start - offset

        f.seek(start + length)
        seek_delimiter(f, delimiter, 2 ** 16)
        end = f.tell()

        offset = start
        length = end - start

    f.seek(offset)
    bytes = f.read(length)
    return bytes


@contextmanager
def tmpfile(extension=""):
    extension = "." + extension.lstrip(".")
    handle, filename = tempfile.mkstemp(extension)
    os.close(handle)
    os.remove(filename)

    yield filename

    if os.path.exists(filename):
        if os.path.isdir(filename):
            shutil.rmtree(filename)
        else:
            try:
                os.remove(filename)
            except OSError:  # sometimes we can't remove a generated temp file
                pass


def ensure_bytes(s):
    """ Turn string or bytes to bytes

    >>> ensure_bytes('123')
    b'123'
    >>> ensure_bytes(b'123')
    b'123'
    """
    if isinstance(s, bytes):
        return s
    if isinstance(s, memoryview):
        return s.tobytes()
    if isinstance(s, bytearray) or PY2 and isinstance(s, buffer):  # noqa: F821
        return bytes(s)
    if hasattr(s, "encode"):
        return s.encode()
    raise TypeError("Object %s is neither a bytes object nor has an encode method" % s)


def divide_n_among_bins(n, bins):
    """

    >>> divide_n_among_bins(12, [1, 1])
    [6, 6]
    >>> divide_n_among_bins(12, [1, 2])
    [4, 8]
    >>> divide_n_among_bins(12, [1, 2, 1])
    [3, 6, 3]
    >>> divide_n_among_bins(11, [1, 2, 1])
    [2, 6, 3]
    >>> divide_n_among_bins(11, [.1, .2, .1])
    [2, 6, 3]
    """
    total = sum(bins)
    acc = 0.0
    out = []
    for b in bins:
        now = n / total * b + acc
        now, acc = divmod(now, 1)
        out.append(int(now))
    return out


def mean(seq):
    seq = list(seq)
    return sum(seq) / len(seq)


if hasattr(sys, "is_finalizing"):

    def shutting_down(is_finalizing=sys.is_finalizing):
        return is_finalizing()


else:
    _shutting_down = [False]

    def _at_shutdown(l=_shutting_down):
        l[0] = True

    def shutting_down(l=_shutting_down):
        return l[0]

    atexit.register(_at_shutdown)


shutting_down.__doc__ = """
    Whether the interpreter is currently shutting down.
    For use in finalizers, __del__ methods, and similar; it is advised
    to early bind this function rather than look it up when calling it,
    since at shutdown module globals may be cleared.
    """


def open_port(host=""):
    """ Return a probably-open port

    There is a chance that this port will be taken by the operating system soon
    after returning from this function.
    """
    # http://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def import_file(path):
    """ Loads modules for a file (.py, .zip, .egg) """
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    names_to_import = []
    tmp_python_path = None

    if ext in (".py",):  # , '.pyc'):
        if directory not in sys.path:
            tmp_python_path = directory
        names_to_import.append(name)
    if ext == ".py":  # Ensure that no pyc file will be reused
        cache_file = cache_from_source(path)
        with ignoring(OSError):
            os.remove(cache_file)
    if ext in (".egg", ".zip", ".pyz"):
        if path not in sys.path:
            sys.path.insert(0, path)
        if ext == ".egg":
            import pkg_resources

            pkgs = pkg_resources.find_distributions(path)
            for pkg in pkgs:
                names_to_import.append(pkg.project_name)
        elif ext in (".zip", ".pyz"):
            names_to_import.append(name)

    loaded = []
    if not names_to_import:
        logger.warning("Found nothing to import from %s", filename)
    else:
        invalidate_caches()
        if tmp_python_path is not None:
            sys.path.insert(0, tmp_python_path)
        try:
            for name in names_to_import:
                logger.info("Reload module %s from %s file", name, ext)
                loaded.append(reload(import_module(name)))
        finally:
            if tmp_python_path is not None:
                sys.path.remove(tmp_python_path)
    return loaded


class itemgetter(object):
    """A picklable itemgetter.

    Examples
    --------
    >>> data = [0, 1, 2]
    >>> get_1 = itemgetter(1)
    >>> get_1(data)
    1
    """

    __slots__ = ("index",)

    def __init__(self, index):
        self.index = index

    def __call__(self, x):
        return x[self.index]

    def __reduce__(self):
        return (itemgetter, (self.index,))


def format_bytes(n):
    """ Format bytes as text

    >>> format_bytes(1)
    '1 B'
    >>> format_bytes(1234)
    '1.23 kB'
    >>> format_bytes(12345678)
    '12.35 MB'
    >>> format_bytes(1234567890)
    '1.23 GB'
    >>> format_bytes(1234567890000)
    '1.23 TB'
    >>> format_bytes(1234567890000000)
    '1.23 PB'
    """
    if n > 1e15:
        return "%0.2f PB" % (n / 1e15)
    if n > 1e12:
        return "%0.2f TB" % (n / 1e12)
    if n > 1e9:
        return "%0.2f GB" % (n / 1e9)
    if n > 1e6:
        return "%0.2f MB" % (n / 1e6)
    if n > 1e3:
        return "%0.2f kB" % (n / 1000)
    return "%d B" % n


byte_sizes = {
    "kB": 10 ** 3,
    "MB": 10 ** 6,
    "GB": 10 ** 9,
    "TB": 10 ** 12,
    "PB": 10 ** 15,
    "KiB": 2 ** 10,
    "MiB": 2 ** 20,
    "GiB": 2 ** 30,
    "TiB": 2 ** 40,
    "PiB": 2 ** 50,
    "B": 1,
    "": 1,
}
byte_sizes = {k.lower(): v for k, v in byte_sizes.items()}
byte_sizes.update({k[0]: v for k, v in byte_sizes.items() if k and "i" not in k})
byte_sizes.update({k[:-1]: v for k, v in byte_sizes.items() if k and "i" in k})


def parse_bytes(s):
    """ Parse byte string to numbers

    >>> parse_bytes('100')
    100
    >>> parse_bytes('100 MB')
    100000000
    >>> parse_bytes('100M')
    100000000
    >>> parse_bytes('5kB')
    5000
    >>> parse_bytes('5.4 kB')
    5400
    >>> parse_bytes('1kiB')
    1024
    >>> parse_bytes('1e6')
    1000000
    >>> parse_bytes('1e6 kB')
    1000000000
    >>> parse_bytes('MB')
    1000000
    """
    s = s.replace(" ", "")
    if not s[0].isdigit():
        s = "1" + s

    for i in range(len(s) - 1, -1, -1):
        if not s[i].isalpha():
            break
    index = i + 1

    prefix = s[:index]
    suffix = s[index:]

    n = float(prefix)

    multiplier = byte_sizes[suffix.lower()]

    result = n * multiplier
    return int(result)


timedelta_sizes = {
    "s": 1,
    "ms": 1e-3,
    "us": 1e-6,
    "ns": 1e-9,
    "m": 60,
    "h": 3600,
    "d": 3600 * 24,
}

tds2 = {
    "second": 1,
    "minute": 60,
    "hour": 60 * 60,
    "day": 60 * 60 * 24,
    "millisecond": 1e-3,
    "microsecond": 1e-6,
    "nanosecond": 1e-9,
}
tds2.update({k + "s": v for k, v in tds2.items()})
timedelta_sizes.update(tds2)
timedelta_sizes.update({k.upper(): v for k, v in timedelta_sizes.items()})


def parse_timedelta(s, default="seconds"):
    """ Parse timedelta string to number of seconds

    Examples
    --------
    >>> parse_timedelta('3s')
    3
    >>> parse_timedelta('3.5 seconds')
    3.5
    >>> parse_timedelta('300ms')
    0.3
    >>> parse_timedelta(timedelta(seconds=3))  # also supports timedeltas
    3
    """
    if isinstance(s, timedelta):
        return s.total_seconds()
    if isinstance(s, Number):
        s = str(s)
    s = s.replace(" ", "")
    if not s[0].isdigit():
        s = "1" + s

    for i in range(len(s) - 1, -1, -1):
        if not s[i].isalpha():
            break
    index = i + 1

    prefix = s[:index]
    suffix = s[index:] or default

    n = float(prefix)

    multiplier = timedelta_sizes[suffix.lower()]

    result = n * multiplier
    if int(result) == result:
        result = int(result)
    return result


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


if PY2:

    def nbytes(frame, _bytes_like=(bytes, bytearray, buffer)):  # noqa: F821
        """ Number of bytes of a frame or memoryview """
        if isinstance(frame, _bytes_like):
            return len(frame)
        elif isinstance(frame, memoryview):
            if frame.shape is None:
                return frame.itemsize
            else:
                return functools.reduce(operator.mul, frame.shape, frame.itemsize)
        else:
            return frame.nbytes


else:

    def nbytes(frame, _bytes_like=(bytes, bytearray)):
        """ Number of bytes of a frame or memoryview """
        if isinstance(frame, _bytes_like):
            return len(frame)
        else:
            try:
                return frame.nbytes
            except AttributeError:
                return len(frame)


def PeriodicCallback(callback, callback_time, io_loop=None):
    """
    Wrapper around tornado.IOLoop.PeriodicCallback, for compatibility
    with removal of the `io_loop` parameter in Tornado 5.0.
    """
    if tornado.version_info >= (5,):
        return tornado.ioloop.PeriodicCallback(callback, callback_time)
    else:
        return tornado.ioloop.PeriodicCallback(callback, callback_time, io_loop)


@contextmanager
def time_warn(duration, text):
    start = time()
    yield
    end = time()
    if end - start > duration:
        print("TIME WARNING", text, end - start)


def json_load_robust(fn, load=json.load):
    """ Reads a JSON file from disk that may be being written as we read """
    while not os.path.exists(fn):
        sleep(0.01)
    for i in range(10):
        try:
            with open(fn) as f:
                cfg = load(f)
            if cfg:
                return cfg
        except (ValueError, KeyError):  # race with writing process
            pass
        sleep(0.1)


def format_time(n):
    """ format integers as time

    >>> format_time(1)
    '1.00 s'
    >>> format_time(0.001234)
    '1.23 ms'
    >>> format_time(0.00012345)
    '123.45 us'
    >>> format_time(123.456)
    '123.46 s'
    """
    if n >= 1:
        return "%.2f s" % n
    if n >= 1e-3:
        return "%.2f ms" % (n * 1e3)
    return "%.2f us" % (n * 1e6)


class DequeHandler(logging.Handler):
    """ A logging.Handler that records records into a deque """

    _instances = weakref.WeakSet()

    def __init__(self, *args, **kwargs):
        n = kwargs.pop("n", 10000)
        self.deque = deque(maxlen=n)
        super(DequeHandler, self).__init__(*args, **kwargs)
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
    """ Python 2's logger's locks don't survive a fork event

    https://github.com/dask/distributed/issues/1491
    """
    for name in logging.Logger.manager.loggerDict.keys():
        for handler in logging.getLogger(name).handlers:
            handler.createLock()


# Only bother if asyncio has been loaded by Tornado
if "asyncio" in sys.modules and tornado.version_info[0] >= 5:

    jupyter_event_loop_initialized = False

    if "notebook" in sys.modules:
        import traitlets
        from notebook.notebookapp import NotebookApp

        jupyter_event_loop_initialized = traitlets.config.Application.initialized() and isinstance(
            traitlets.config.Application.instance(), NotebookApp
        )

    if not jupyter_event_loop_initialized:
        import asyncio
        import tornado.platform.asyncio

        asyncio.set_event_loop_policy(
            tornado.platform.asyncio.AnyThreadEventLoopPolicy()
        )


def has_keyword(func, keyword):
    if PY3:
        return keyword in inspect.signature(func).parameters
    else:
        # https://stackoverflow.com/questions/50100498/determine-keywords-of-a-tornado-coroutine
        if gen.is_coroutine_function(func):
            func = func.__wrapped__
        return keyword in inspect.getargspec(func).args


if lru_cache:
    has_keyword = lru_cache(1000)(has_keyword)


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
    h = md5(str(x).encode())
    n = int(h.hexdigest()[:8], 16)
    return palette[n % len(palette)]


def iscoroutinefunction(f):
    if gen.is_coroutine_function(f):
        return True
    if sys.version_info >= (3, 5) and inspect.iscoroutinefunction(f):
        return True
    return False


@contextmanager
def warn_on_duration(duration, msg):
    start = time()
    yield
    stop = time()
    if stop - start > parse_timedelta(duration):
        warnings.warn(msg, stacklevel=2)


def typename(typ):
    """ Return name of type

    Examples
    --------
    >>> from distributed import Scheduler
    >>> typename(Scheduler)
    'distributed.scheduler.Scheduler'
    """
    try:
        return typ.__module__ + "." + typ.__name__
    except AttributeError:
        return str(typ)
