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

    if hasattr(pd, 'CategoricalIndex'):  # Pandas 0.16.1
        if isinstance(df.index, pd.CategoricalIndex):
            df.index = pd.Index(df.index.codes)

    return df


def categorical_metadata(df):
    """ Collects category metadata

    >>> cat = pd.Categorical(['Alice', 'Bob', 'Alice'], ordered=False)
    >>> df = pd.DataFrame({'a': cat})
    >>> categorical_metadata(df)  # doctest: +SKIP
    {'a': {'ordered': False, 'categories': Index([u'Alice', u'Bob'], dtype='object')}}
    """
    result = dict()
    for name in df.columns:
        if isinstance(df.dtypes[name], pd.core.common.CategoricalDtype):
            result[name] = {'categories': df[name].cat.categories,
                            'ordered': df[name].cat.ordered}

    if hasattr(pd, 'CategoricalIndex'):  # Pandas 0.16.1
        if isinstance(df.index, pd.CategoricalIndex):
            result['_index'] = {'categories': df.index.categories,
                                'ordered': False}

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
            df = pd.Series(pd.Categorical.from_codes(df.values,
                                d['categories'], d['ordered']))
    else:
        for name, d in metadata.items():
            if name in df.columns:
                df[name] = pd.Categorical.from_codes(df[name].values,
                                             d['categories'], d['ordered'])

    if hasattr(pd, 'CategoricalIndex'):  # Pandas 0.16.1
        if '_index' in metadata:
            cat = pd.Categorical.from_codes(df.index.values,
                                            metadata['_index']['categories'],
                                            metadata['_index']['ordered'])
            df.index = pd.CategoricalIndex(cat, df.index.name)

    return df
