from ..utils import ignoring

from .cluster import Cluster
from .local import LocalCluster
from .spec import SpecCluster, ProcessInterface
from .adaptive import Adaptive

with ignoring(ImportError):
    from .ssh import SSHCluster
