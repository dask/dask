"""
Support for pandas ExtensionArray in dask.dataframe.

See :ref:`extensionarrays` for more.
"""
from ..utils import Dispatch

make_array_nonempty = Dispatch("make_array_nonempty")
