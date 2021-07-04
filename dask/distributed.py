# flake8: noqa
try:
    from distributed import *
except ImportError as e:
    if e.msg == "No module named 'distributed'":
        msg = (
            "Dask's distributed scheduler is not installed.\n\n"
            "Please either conda or pip install dask distributed:\n\n"
            "  conda install dask distributed          # either conda install\n"
            '  python -m pip install "dask[distributed]" --upgrade  # or python -m pip install'
        )
        raise ImportError(msg) from e
    else:
        raise


def __getattr__(value):
    try:
        import distributed
    except ImportError as e:
        msg = (
            "dask.distributed is not installed.\n\n"
            "Please either conda or pip install distributed:\n\n"
            "  conda install dask distributed             # either conda install\n"
            "  python -m pip install dask[distributed] --upgrade    # or pip install"
        )
        raise ImportError(msg) from e
    return getattr(distributed, value)
