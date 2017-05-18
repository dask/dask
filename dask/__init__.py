from __future__ import absolute_import, division, print_function

from .core import istask
from .context import set_options
from .local import get_sync as get
try:
    from .delayed import delayed
except ImportError:
    pass
try:
    from .base import visualize, compute, persist
except ImportError:
    pass

# dask.async is deprecated. For now we import it to the top namespace to be
# compatible with prior releases. This should be removed in a future release:
import dask.async

from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
