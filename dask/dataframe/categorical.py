from __future__ import absolute_import, division, print_function

import pandas as pd
from toolz import partial

from dask.base import compute


def _categorize_block(df, categories):
    """ Categorize a dataframe with given categories

    df: DataFrame
    categories: dict mapping column name to iterable of categories
    """
    df = df.copy()
    for col, vals in categories.items():
        df[col] = pd.Categorical(df[col], categories=vals, ordered=False)
    return df


def categorize(df, columns=None, **kwargs):
    """
    Convert columns of dataframe to category dtype

    This aids performance, both in-memory and in spilling to disk
    """
    if columns is None:
        dtypes = df.dtypes
        columns = [name for name, dt in zip(dtypes.index, dtypes.values)
                   if dt == 'O']
    if not isinstance(columns, (list, tuple)):
        columns = [columns]

    distincts = [df[col].dropna().drop_duplicates() for col in columns]
    values = compute(*distincts, **kwargs)

    func = partial(_categorize_block, categories=dict(zip(columns, values)))

    meta = func(df._meta)
    return df.map_partitions(func, meta=meta)
