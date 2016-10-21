from __future__ import print_function, division, absolute_import

import logging
import sys

if sys.version_info[0] == 2:
    from Queue import Queue, Empty
    from io import BytesIO
    from thread import get_ident as get_thread_identity
    reload = reload
    unicode = unicode
    PY2 = True
    PY3 = False
    ConnectionRefusedError = OSError
    FileExistsError = OSError

    import gzip
    def gzip_decompress(b):
        f = gzip.GzipFile(fileobj=BytesIO(b))
        result = f.read()
        f.close()
        return result

    def gzip_compress(b):
        bio = BytesIO()
        f = gzip.GzipFile(fileobj=bio, mode='w')
        f.write(b)
        f.close()
        bio.seek(0)
        result = bio.read()
        return result

    def isqueue(o):
        return (hasattr(o, 'queue') and
                hasattr(o, '__module__') and
                o.__module__ == 'Queue')

    logging_names = logging._levelNames

if sys.version_info[0] == 3:
    from queue import Queue, Empty
    from importlib import reload
    from threading import get_ident as get_thread_identity
    PY2 = False
    PY3 = True
    unicode = str
    from gzip import decompress as gzip_decompress
    from gzip import compress as gzip_compress
    ConnectionRefusedError = ConnectionRefusedError
    FileExistsError = FileExistsError

    def isqueue(o):
        return isinstance(o, Queue)

    logging_names = logging._levelToName.copy()
    logging_names.update(logging._nameToLevel)


WINDOWS = sys.platform.startswith('win')


try:
    from json.decoder import JSONDecodeError
except (ImportError, AttributeError):
    JSONDecodeError = ValueError

try:
    from functools import singledispatch
except ImportError:
    from singledispatch import singledispatch
