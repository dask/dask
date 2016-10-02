from __future__ import absolute_import, division, print_function

import pandas as pd

from .core import Series, DataFrame, map_partitions
from .categorical import is_categorical_dtype, _get_categorical_columns


def get_dummies(data, prefix=None, prefix_sep='_', dummy_na=False,
                columns=None, sparse=False, drop_first=False):
    """
    Convert categorical variable into dummy/indicator variables. data must
    have category dtype to infer result's column name

    Parameters
    ----------
    data : Series or DataFrame with category dtype
    prefix : string, list of strings, or dict of strings, default None
        String to append DataFrame column names
        Pass a list with length equal to the number of columns
        when calling get_dummies on a DataFrame. Alternativly, `prefix`
        can be a dictionary mapping column names to prefixes.
    prefix_sep : string, default '_'
        If appending prefix, separator/delimiter to use. Or pass a
        list or dictionary as with `prefix.`
    dummy_na : bool, default False
        Add a column to indicate NaNs, if False NaNs are ignored.
    drop_first : bool, default False
        Whether to get k-1 dummies out of k categorical levels by removing the
        first level.
    Returns
    -------
    dummies : DataFrame
    """

    if isinstance(data, (pd.Series, pd.DataFrame)):
        return pd.get_dummies(data, prefix=prefix,
                              prefix_sep=prefix_sep, dummy_na=dummy_na,
                              columns=columns, sparse=sparse,
                              drop_first=drop_first)

    if isinstance(data, Series) and not is_categorical_dtype(data):
        raise ValueError('data must have category dtype')
    elif isinstance(data, DataFrame):
        if columns is None:
            columns = _get_categorical_columns(data)
        else:
            if not all(is_categorical_dtype(data[c]) for c in columns):
                raise ValueError('target columns must have category dtype')

    if sparse:
        raise NotImplementedError('sparse=True is not supported')

    return map_partitions(pd.get_dummies, data, prefix=prefix,
                          prefix_sep=prefix_sep, dummy_na=dummy_na,
                          columns=columns, sparse=sparse,
                          drop_first=drop_first)
