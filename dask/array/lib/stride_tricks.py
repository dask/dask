from __future__ import annotations


def sliding_window_view(*args, **kwargs):
    import dask.array as da

    return da.overlap.sliding_window_view(*args, **kwargs)
