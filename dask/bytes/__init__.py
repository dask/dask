from ..utils import ignoring
from .core import read_bytes, open_files, open_text_files

with ignoring(ImportError, SyntaxError):
    from . import s3
with ignoring(ImportError):
    from . import local
