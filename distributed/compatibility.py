import logging
import platform
import sys

logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

PYPY = platform.python_implementation().lower() == "pypy"
WINDOWS = sys.platform.startswith("win")
