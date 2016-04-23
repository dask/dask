storage_systems = dict()

from ..utils import ignoring

with ignoring(ImportError):
    from . import s3
with ignoring(ImportError):
    from . import file
