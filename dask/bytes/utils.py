import sys
from toolz import identity

if sys.version_info[0] < 3:
    class SeekableFile(object):
        def __init__(self, file):
            if isinstance(file, SeekableFile):  # idempotent
                file = file.file
            self.file = file

        def seekable(self):
            return True

        def readable(self):
            try:
                return self.file.readable()
            except AttributeError:
                return 'r' in self.file.mode

        def writable(self):
            try:
                return self.file.readable()
            except AttributeError:
                return 'w' in self.file.mode

        @property
        def read1(self):  # https://bugs.python.org/issue12591
            return self.file.read

        def __getattr__(self, key):
            return getattr(self.file, key)
else:
    SeekableFile = identity


