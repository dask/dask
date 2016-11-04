from __future__ import absolute_import, division, print_function

from .core import istask
from .context import set_options
from .async import get_sync as get
try:
    from .delayed import do, delayed, value
except ImportError:
    pass
try:
    from .base import visualize, compute
except ImportError:
    pass

from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
