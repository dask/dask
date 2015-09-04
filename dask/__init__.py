from __future__ import absolute_import, division, print_function

from .core import istask, get
from .context import set_options

try:
    from .imperative import do, value
except ImportError:
    pass

__version__ = '0.7.1'
