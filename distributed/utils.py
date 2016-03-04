from __future__ import print_function, division, absolute_import

from collections import Iterable
from contextlib import contextmanager
import logging
import os
import re
import socket
import sys
import tblib.pickling_support
import tempfile
from threading import Thread

from dask import istask
from toolz import memoize, valmap
from tornado import gen

from .compatibility import Queue, PY3

logger = logging.getLogger(__name__)


def funcname(func):
    """Get the name of a function."""
    while hasattr(func, 'func'):
        func = func.func
    try:
        return func.__name__
    except:
        return str(func)


def get_ip(host='8.8.8.8', port=80):
    return [(s.connect((host, port)), s.getsockname()[0], s.close())
            for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
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
    """ Run coroutine in loop running in separate thread """
    if not loop._running:
        try:
            return loop.run_sync(lambda: func(*args, **kwargs))
        except RuntimeError:  # loop already running
            pass

    from threading import Event
    e = Event()
    result = [None]
    error = [False]

    @gen.coroutine
    def f():
        try:
            result[0] = yield gen.maybe_future(func(*args, **kwargs))
        except Exception as exc:
            logger.exception(exc)
            result[0] = exc
            error[0] = True
        finally:
            e.set()

    a = loop.add_callback(f)
    while not e.is_set():
        e.wait(1000000)
    if error[0]:
        raise result[0]
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


def _deps(dsk, arg):
    """ Get dependencies from keys or tasks

    Helper function for get_dependencies.

    Examples
    --------
    >>> inc = lambda x: x + 1
    >>> add = lambda x, y: x + y

    >>> dsk = {'x': 1, 'y': 2}

    >>> _deps(dsk, 'x')
    ['x']
    >>> _deps(dsk, (add, 'x', 1))
    ['x']
    >>> _deps(dsk, ['x', 'y'])
    ['x', 'y']
    >>> _deps(dsk, {'name': 'x'})
    ['x']
    >>> _deps(dsk, (add, 'x', (inc, 'y')))  # doctest: +SKIP
    ['x', 'y']
    """
    if istask(arg):
        result = []
        for a in arg[1:]:
            result.extend(_deps(dsk, a))
        return result
    if isinstance(arg, list):
        return sum([_deps(dsk, a) for a in arg], [])
    if isinstance(arg, dict):
        return sum([_deps(dsk, v) for v in arg.values()], [])
    try:
        if arg not in dsk:
            return []
    except TypeError:  # not hashable
            return []
    return [arg]


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
    >>> key_split(None)
    'Other'
    """
    if isinstance(s, bytes):
        return key_split(s.decode())
    if isinstance(s, tuple):
        return key_split(s[0])
    try:
        words = s.split('-')
        result = words[0].lstrip("'(\"")
        for word in words[1:]:
            if word.isalpha():
                result += '-' + word
            else:
                break
        if len(result) == 32 and re.match(r'[a-f0-9]{32}', result):
            return 'data'
        else:
            return result
    except:
        return 'Other'


@contextmanager
def log_errors():
    try:
        yield
    except gen.Return:
        raise
    except Exception as e:
        logger.exception(e)
        raise


@memoize
def ensure_ip(hostname):
    """ Ensure that address is an IP address

    Examples
    --------
    >>> ensure_ip('localhost')
    '127.0.0.1'
    >>> ensure_ip('123.123.123.123')  # pass through IP addresses
    '123.123.123.123'
    >>> ensure_ip('localhost:5000')
    '127.0.0.1:5000'
    """
    if ':' in hostname:
        host, port = hostname.split(':')
        return ':'.join([ensure_ip(host), port])
    if PY3 and isinstance(hostname, bytes):
        hostname = hostname.decode()
    if re.match('\d+\.\d+\.\d+\.\d+', hostname):  # is IP
        return hostname
    else:
        try:
            return socket.gethostbyname(hostname)
        except Exception as e:
            logger.warn("Could not resolve hostname: %s", hostname,
                        exc_info=True)
            raise


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


def queue_to_iterator(q):
    while True:
        result = q.get()
        if result == StopIteration:
            break
        yield result

def _dump_to_queue(seq, q):
    for item in seq:
        q.put(item)

def iterator_to_queue(seq, maxsize=0):
    q = Queue(maxsize=maxsize)

    t = Thread(target=_dump_to_queue, args=(seq, q))
    t.daemon = True
    t.start()

    return q


def tokey(o):
    """ Convert an object to a bytestring, using str

    Examples
    --------

    >>> tokey(b'x')
    b'x'
    >>> tokey('x')
    'x'
    >>> tokey(1)
    '1'
    """
    if isinstance(o, (str, bytes)):
        return o
    else:
        return str(o)


def _maybe_complex(task):
    """ Possibly contains a nested task """
    return (istask(task) or
            isinstance(task, list) and any(map(_maybe_complex, task)) or
            isinstance(task, dict) and any(map(_maybe_complex, task.values())))


def str_graph(dsk):
    def convert(task):
        if isinstance(task, list):
            return [convert(v) for v in task]
        if isinstance(task, dict):
            return valmap(convert, task)
        if istask(task):
            return (task[0],) + tuple(map(convert, task[1:]))
        try:
            if task in dsk:
                return tokey(task)
        except TypeError:
            pass
        return task

    return {tokey(k): convert(v) for k, v in dsk.items()}


import logging
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

# http://stackoverflow.com/questions/21234772/python-tornado-disable-logging-to-stderr
stream = logging.StreamHandler(sys.stderr)
stream.setLevel(logging.CRITICAL)
logging.getLogger('tornado').addHandler(stream)
logging.getLogger('tornado').propagate = False


from contextlib import contextmanager
import shutil


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
        eof = not f.read(1)

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
