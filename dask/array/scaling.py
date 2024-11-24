import dask.array as da

def min_max_scale(data, feature_range=(0, 1)):
    """
    Scale data to a specified range using min-max scaling.

    Parameters:
    -----------
    data : dask.array.Array
        Input array to scale.
    feature_range : tuple, optional
        Desired range of transformed data (default is (0, 1)).

    Returns:
    --------
    dask.array.Array
        Scaled array.
    """
    min_val, max_val = feature_range
    data_min = da.min(data)
    data_max = da.max(data)

    if (data_max - data_min).compute() == 0:
        raise ValueError("Cannot scale data with constant values.")
    
    scaled_data = (data - data_min) / (data_max - data_min) * (max_val - min_val) + min_val
    return scaled_data

def l2_normalize(data, axis=0):
    """
    Apply L2 normalization to data.

    Parameters:
    -----------
    data : dask.array.Array
        Input array to normalize.
    axis : int, optional
        Axis along which to compute the L2 norm (default is 0).

    Returns:
    --------
    dask.array.Array
        L2-normalized array.
    """
    l2_norm = da.sqrt(da.sum(data**2, axis=axis, keepdims=True))

    if (l2_norm == 0).any().compute():
        raise ValueError("L2 norm is zero, cannot normalize.")
    
    return data / l2_norm

