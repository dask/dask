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

__version__ = '0.9.0'
