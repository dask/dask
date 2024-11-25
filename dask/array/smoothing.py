import dask.array as da
import numpy as np

def moving_average(data, window_size):
    if window_size < 1:
        raise ValueError("Window size must be at least 1.")
    if not isinstance(window_size, int):
        raise TypeError("Window size must be an integer.")
    if window_size > data.shape[0]:
        raise ValueError("Window size cannot be larger than the data size.")

    kernel = np.ones(window_size) / window_size

    def convolve_chunk(block):
        return np.convolve(block, kernel, mode="valid")

    depth = (window_size - 1) // 2
    smoothed = data.map_overlap(
        convolve_chunk,
        depth=depth,
        boundary="reflect",
        trim=True,
        dtype=data.dtype,
    )
    return smoothed
