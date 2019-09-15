# flake8: noqa
try:
    from distributed import *
except ImportError:
    msg = (
        "Dask's distributed scheduler is not installed.\n\n"
        "Please either conda or pip install dask distributed:\n\n"
        "  conda install dask distributed          # either conda install\n"
        "  pip install dask distributed --upgrade  # or pip install"
    )
    raise ImportError(msg)
