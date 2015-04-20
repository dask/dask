from warnings import warn

warn('''

Please pardon our dust.
Dask.dataframe is experimental and not ready for public use.
Expect errors today and breaks in the APIs in the future.

Please report issues to https://github.com/ContinuumIO/dask/issues/new
''')


from .core import (DataFrame, Series, Index, _Frame, read_csv, from_array,
        from_bcolz)
from .shuffle import from_pframe
