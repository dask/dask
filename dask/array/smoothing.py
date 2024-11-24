import dask.array as da
import numpy as np

def moving_average(data, window_size):
    """
    Apply a moving average to smooth data.

    Parameters:
    -----------
    data : dask.array.Array
        Input array to smooth.
    window_size : int
        The size of the moving window.

    Returns:
    --------
    dask.array.Array
        Smoothed array with reduced shape due to valid convolution.
    """
    if window_size < 1:
        raise ValueError("Window size must be at least 1.")
    if not isinstance(window_size, int):
        raise TypeError("Window size must be an integer.")
    if window_size > data.shape[0]:
        raise ValueError("Window size cannot be larger than the data size.")

    # Define the kernel for the moving average
    kernel = np.ones(window_size) / window_size

    # Convolution function for each block
    def convolve_chunk(block):
        # Apply valid convolution on the chunk
        return np.convolve(block, kernel, mode="valid")

    # Use map_blocks to apply convolution while handling chunk overlaps
    depth = (window_size - 1) // 2
    smoothed = data.map_overlap(
        convolve_chunk,
        depth=depth,
        boundary="reflect",
        trim=True,
        dtype=data.dtype,
    )
    return smoothed
