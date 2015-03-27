import pandas as pd

"""
Strip and re-apply category information
---------------------------------------

Categoricals are great for storage as long as we don't repeatedly store the
categories themselves many thousands of times.  Lets collect category
information once, strip it from all of the shards, store, then re-apply as we
load them back in
"""

def strip_categories(df):
    """ Strip category information from dataframe

    Operates in place.

    >>> df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice'])})
    >>> df
           a
    0  Alice
    1    Bob
    2  Alice
    >>> strip_categories(df)
       a
    0  0
    1  1
    2  0
    """
    for name in df.columns:
        if isinstance(df.dtypes[name], pd.core.common.CategoricalDtype):
            df[name] = df[name].cat.codes
    return df


def categorical_metadata(df):
    """ Collects category metadata

    >>> df = pd.DataFrame({'a': pd.Categorical(['Alice', 'Bob', 'Alice'])})
    >>> categorical_metadata(df)  # doctest: +SKIP
    {'a': {'ordered': True, 'categories': Index([u'Alice', u'Bob'], dtype='object')}}
    """
    result = dict()
    for name in df.columns:
        if isinstance(df.dtypes[name], pd.core.common.CategoricalDtype):
            result[name] = {'categories': df[name].cat.categories,
                            'ordered': df[name].cat.ordered}
    return result


def reapply_categories(df, metadata):
    """ Reapply stripped off categories

    Operates in place

    >>> df = pd.DataFrame({'a': [0, 1, 0]})
    >>> metadata = {'a': {'ordered': True,
    ...                   'categories': pd.Index(['Alice', 'Bob'], dtype='object')}}

    >>> reapply_categories(df, metadata)
           a
    0  Alice
    1    Bob
    2  Alice
    """
    if isinstance(df, pd.Series):
        if df.name in metadata:
            d = metadata[df.name]
            return pd.Series(pd.Categorical.from_codes(df.values,
                                d['categories'], d['ordered']))

    for name, d in metadata.items():
        if name in df.columns:
            df[name] = pd.Categorical.from_codes(df[name].values,
                                         d['categories'], d['ordered'])
    return df
