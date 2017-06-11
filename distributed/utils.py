from __future__ import print_function, division, absolute_import

import atexit
from collections import Iterable
from contextlib import contextmanager
from datetime import timedelta
import inspect
import logging
import multiprocessing
import os
import re
import shutil
import socket
from importlib import import_module

import six
import sys
import tblib.pickling_support
import tempfile
import threading
import warnings

from .compatibility import cache_from_source, getargspec, invalidate_caches, reload

try:
    import resource
except ImportError:
    resource = None

from dask import istask
from toolz import memoize, valmap
from tornado import gen

from .compatibility import Queue, PY3, PY2, get_thread_identity, unicode
from .config import config


logger = logging.getLogger(__name__)


def _initialize_mp_context():
    if PY3 and not sys.platform.startswith('win'):
        method = config.get('multiprocessing-method', 'forkserver')
        ctx = multiprocessing.get_context(method)
        # Makes the test suite much faster
        preload = ['distributed']
        if 'pkg_resources' in sys.modules:
            preload.append('pkg_resources')
        ctx.set_forkserver_preload(preload)
    else:
        ctx = multiprocessing

    return ctx


mp_context = _initialize_mp_context()


def funcname(func):
    """Get the name of a function."""
    while hasattr(func, 'func'):
        func = func.func
    try:
        return func.__name__
    except:
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


@memoize
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
        warnings.warn("Couldn't detect a suitable IP address for "
                      "reaching %r, defaulting to %r: %s"
                      % (host, default, e), RuntimeWarning)
        return default
    finally:
        sock.close()


def get_ip(host='8.8.8.8', port=80):
    """
    Get the local IP address through which the *host* is reachable.

    *host* defaults to a well-known Internet host (one of Google's public
    DNS servers).
    """
    return _get_ip(host, port, family=socket.AF_INET, default='127.0.0.1')


def get_ipv6(host='2001:4860:4860::8888', port=80):
    """
    The same as get_ip(), but for IPv6.
    """
    return _get_ip(host, port, family=socket.AF_INET6, default='::1')


def get_ip_interface(ifname):
    import psutil
    L = psutil.net_if_addrs()[ifname]
    return [x.address for x in L if x.family == socket.AF_INET][0]


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
def All(*args):
    """ Wait on many tasks at the same time

    Err once any of the tasks err.

    See https://github.com/tornadoweb/tornado/issues/1546
    """
    if len(args) == 1 and isinstance(args[0], Iterable):
        args = args[0]
    tasks = gen.WaitIterator(*args)
    results = [None for _ in args]
    while not tasks.done():
        result = yield tasks.next()
        results[tasks.current_index] = result
    raise gen.Return(results)


def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    timeout = kwargs.pop('callback_timeout', None)
    def make_coro():
        coro = gen.maybe_future(func(*args, **kwargs))
        if timeout is None:
            return coro
        else:
            return gen.with_timeout(timedelta(seconds=timeout), coro)

    if not loop._running:
        try:
            return loop.run_sync(make_coro)
        except RuntimeError:  # loop already running
            pass

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
            result[0] = yield make_coro()
        except Exception as exc:
            logger.exception(exc)
            error[0] = sys.exc_info()
        finally:
            e.set()

    loop.add_callback(f)
    while not e.is_set():
        e.wait(1000000)
    if error[0]:
        six.reraise(*error[0])
    else:
        return result[0]


@contextmanager
def tmp_text(filename, text):
    fn = os.path.join(tempfile.gettempdir(), filename)
    with open(fn, 'w') as f:
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
    if 'IPython' not in sys.modules:  # IPython hasn't been imported
        return False
    from IPython import get_ipython
    # check for `kernel` attribute on the IPython instance
    return getattr(get_ipython(), 'kernel', None) is not None


hex_pattern = re.compile('[a-f]+')


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
        words = s.split('-')
        if not words[0][0].isalpha():
            result = words[0].lstrip("'(\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (len(word) == 8 and
                                       hex_pattern.match(word) is not None):
                result += '-' + word
            else:
                break
        if len(result) == 32 and re.match(r'[a-f0-9]{32}', result):
            return 'data'
        else:
            if result[0] == '<':
                result = result.strip('<>').split()[0].split('.')[-1]
            return result
    except:
        return 'Other'


try:
    from functools import lru_cache
except ImportError:
    pass
else:
    key_split = lru_cache(100000)(key_split)


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
        if x[0] == '(':
            return x.split(',', 1)[0].strip('()"\'')
        elif len(x) == 32 and re.match(r'[a-f0-9]{32}', x):
            return 'data'
        elif x[0] == '<':
            return x.strip('<>').split()[0].split('.')[-1]
        else:
            return x
    elif typ is bytes:
        return key_split_group(x.decode())
    else:
        return 'Other'


@contextmanager
def log_errors(pdb=False):
    from .comm import CommClosedError
    try:
        yield
    except (CommClosedError, gen.Return):
        raise
    except Exception as e:
        logger.exception(e)
        if pdb:
            import pdb; pdb.set_trace()
        raise


def silence_logging(level, root='distributed'):
    """
    Force all existing loggers below *root* to the given level at least
    (or keep the existing level if less verbose).
    """
    if isinstance(level, str):
        level = logging_names[level.upper()]

    for name, logger in logging.root.manager.loggerDict.items():
        if (isinstance(logger, logging.Logger)
            and logger.name.startswith(root + '.')
            and logger.level < level):
            logger.setLevel(level)


@memoize
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
            results = socket.getaddrinfo(hostname,
                                         1234,  # dummy port number
                                         fam, socket.SOCK_STREAM)
        except socket.gaierror as e:
            exc = e
        else:
            return results[0][4][0]

    raise exc


tblib.pickling_support.install()


def get_traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    bad = [os.path.join('distributed', 'worker'),
           os.path.join('distributed', 'scheduler'),
           os.path.join('tornado', 'gen.py'),
           os.path.join('concurrent', 'futures')]
    while exc_traceback and any(b in exc_traceback.tb_frame.f_code.co_filename
                                for b in bad):
        exc_traceback = exc_traceback.tb_next
    return exc_traceback


def truncate_exception(e, n=10000):
    """ Truncate exception to be about a certain length """
    if len(str(e)) > n:
        try:
            return type(e)("Long error message",
                           str(e)[:n])
        except:
            return Exception("Long error message",
                              type(e),
                              str(e)[:n])
    else:
        return e


if sys.version_info >= (3,):
    # (re-)raising StopIteration is deprecated in 3.6+
    exec("""def queue_to_iterator(q):
        while True:
            result = q.get()
            if isinstance(result, StopIteration):
                return result.value
            yield result
        """)
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
    t = type(o)
    if t is str:
        return o
    elif t is bytes:
        return o.decode('latin1')
    else:
        return str(o)


def validate_key(k):
    """Validate a key as received on a stream.
    """
    if type(k) is not str:
        raise TypeError("Unexpected key type %s (value: %r)"
                        % (type(k), k))


def _maybe_complex(task):
    """ Possibly contains a nested task """
    return (istask(task) or
            type(task) is list and any(map(_maybe_complex, task)) or
            type(task) is dict and any(map(_maybe_complex, task.values())))


def str_graph(dsk, extra_values=()):
    def convert(task):
        if type(task) is list:
            return [convert(v) for v in task]
        if type(task) is dict:
            return valmap(convert, task)
        if istask(task):
            return (task[0],) + tuple(map(convert, task[1:]))
        try:
            if task in dsk or task in extra_values:
                return tokey(task)
        except TypeError:
            pass
        return task

    return {tokey(k): convert(v) for k, v in dsk.items()}


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

    last = b''
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
        last = full[-len(delimiter):]


def read_block(f, offset, length, delimiter=None):
    """ Read a block of bytes from a file

    Parameters
    ----------
    fn: string
        Path to filename on S3
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


@contextmanager
def tmpfile(extension=''):
    extension = '.' + extension.lstrip('.')
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
    if isinstance(s, bytearray) or PY2 and isinstance(s, buffer):  # flake8: noqa
        return bytes(s)
    if hasattr(s, 'encode'):
        return s.encode()
    raise TypeError(
            "Object %s is neither a bytes object nor has an encode method" % s)


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


def open_port(host=''):
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
    """ Loads modules for a file (.py, .pyc, .zip, .egg) """
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    names_to_import = []
    tmp_python_path = None

    if ext in ('.py', '.pyc'):
        if directory not in sys.path:
            tmp_python_path = directory
        names_to_import.append(name)
        # Ensures that no pyc file will be reused
        cache_file = cache_from_source(path)
        if os.path.exists(cache_file):
            os.remove(cache_file)
    if ext in ('.egg', '.zip'):
        if path not in sys.path:
            sys.path.insert(0, path)
        if ext == '.egg':
            import pkg_resources
            pkgs = pkg_resources.find_distributions(path)
            for pkg in pkgs:
                names_to_import.append(pkg.project_name)
        elif ext == '.zip':
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
    __slots__ = ('index',)

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
    """
    if n > 1e9:
        return '%0.2f GB' % (n / 1e9)
    if n > 1e6:
        return '%0.2f MB' % (n / 1e6)
    if n > 1e3:
        return '%0.2f kB' % (n / 1000)
    return '%d B' % n


if PY2:
    def nbytes(frame):
        """ Number of bytes of a frame or memoryview """
        if type(frame) in (bytes, buffer):
            return len(frame)
        else:
            return frame.nbytes
else:
    def nbytes(frame):
        """ Number of bytes of a frame or memoryview """
        if type(frame) is bytes:
            return len(frame)
        else:
            return frame.nbytes
