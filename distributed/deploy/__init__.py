from __future__ import print_function, division, absolute_import

from ..utils import ignoring

from .local import LocalCluster
from .adaptive import Adaptive
with ignoring(ImportError):
    from .ssh import SSHCluster
