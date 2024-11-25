from __future__ import annotations

import dask.array as da


def log_transform(data):
    if (data <= 0).any().compute():
        raise ValueError("Log transformation is only defined for positive values.")

    return da.log(data)


def binarize(data, threshold=0):
    return (data > threshold).astype(int)
