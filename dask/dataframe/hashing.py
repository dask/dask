import numpy as np
import pandas as pd

from pandas.core.categorical import is_categorical_dtype


def hash_pandas_object(obj):
    if isinstance(obj, (pd.Series, pd.Index)):
        h = hash_array(obj.values).astype('uint64')
    elif isinstance(obj, pd.DataFrame):
        cols = obj.iteritems()
        first_series = next(cols)[1]
        h = hash_array(first_series.values).astype('uint64')
        for _, col in cols:
            h = np.multiply(h, np.uint(3), h)
            h = np.add(h, hash_array(col.values), h)
    else:
        raise TypeError("Unexpected type %s" % type(obj))
    return h


def hash_array(vals):
    """Given a 1d array, return an array of deterministic integers."""
    dt = vals.dtype
    if is_categorical_dtype(dt):
        return vals.codes
    elif np.issubdtype(dt, np.integer):
        return vals.view('u' + str(dt.itemsize))
    elif np.issubdtype(dt, np.floating):
        return np.nan_to_num(vals).view('u' + str(dt.itemsize))
    elif dt == np.bool:
        return vals.view('uint8')
    elif np.issubdtype(dt, np.datetime64) or np.issubdtype(dt, np.timedelta64):
        return vals.view('uint64')
    else:
        return np.array([hash(x) for x in vals], dtype=np.uint64)
