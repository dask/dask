import dask.array as da

def log_transform(data):
    """
    Apply a logarithmic transformation to a Dask array.

    Parameters:
    -----------
    data : dask.array.Array
        Input array to transform.

    Returns:
    --------
    dask.array.Array
        Log-transformed array.
    """
    if (data <= 0).any().compute():
        raise ValueError("Log transformation is only defined for positive values.")
    
    return da.log(data)

def binarize(data, threshold=0):
    """
    Binarize data based on a threshold.

    Parameters:
    -----------
    data : dask.array.Array
        Input array to binarize.
    threshold : float, optional
        Threshold value (default is 0).

    Returns:
    --------
    dask.array.Array
        Binarized array (0 or 1).
    """
    return (data > threshold).astype(int)

