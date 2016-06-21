from ..utils import ignoring

from .local import LocalCluster
with ignoring(ImportError):
    from .ssh import SSHCluster
