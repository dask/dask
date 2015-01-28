from __future__ import absolute_import, division, print_function
from multipledispatch import halt_ordering, restart_ordering
import blaze

halt_ordering()
from .core import Array
from .blaze import np  # need to go through import process here
from .into import np   # Otherwise someone might import later
                       # without ordering halted
restart_ordering()
