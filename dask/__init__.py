from .core import istask, get, set
from multipledispatch import halt_ordering, restart_ordering

halt_ordering()
from .obj import Array
restart_ordering()

__version__ = '0.1.0'
