import dask.array as da

def min_max_scale(data, feature_range=(0, 1)):
    min_val, max_val = feature_range
    data_min = da.min(data)
    data_max = da.max(data)

    if (data_max - data_min).compute() == 0:
        raise ValueError("Cannot scale data with constant values.")
    
    scaled_data = (data - data_min) / (data_max - data_min) * (max_val - min_val) + min_val
    return scaled_data

def l2_normalize(data, axis=0):
    l2_norm = da.sqrt(da.sum(data**2, axis=axis, keepdims=True))

    if (l2_norm == 0).any().compute():
        raise ValueError("L2 norm is zero, cannot normalize.")
    
    return data / l2_norm
