import sys

if sys.version_info[0] == 2:
    from Queue import Queue

if sys.version_info[0] == 3:
    from queue import Queue
