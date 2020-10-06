import logging
import platform
import sys

import tornado

logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

PYPY = platform.python_implementation().lower() == "pypy"
WINDOWS = sys.platform.startswith("win")
TORNADO6 = tornado.version_info[0] >= 6
PY37 = sys.version_info[:2] >= (3, 7)

if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop
else:

    def get_running_loop():
        from asyncio import _get_running_loop

        loop = _get_running_loop()
        if loop is None:
            raise RuntimeError("no running event loop")
        return loop
