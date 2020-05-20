from contextlib import suppress

from .cluster import Cluster
from .local import LocalCluster
from .ssh import SSHCluster
from .spec import SpecCluster, ProcessInterface
from .adaptive import Adaptive

with suppress(ImportError):
    from .ssh import SSHCluster
