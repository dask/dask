import logging
import platform
import sys

logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

PYPY = platform.python_implementation().lower() == "pypy"
WINDOWS = sys.platform.startswith("win")

if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop
else:
    from asyncio import _get_running_loop as get_running_loop  # noqa: F401
