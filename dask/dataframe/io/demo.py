from __future__ import absolute_import, division, print_function

import pandas as pd
import numpy as np

from ..core import tokenize, DataFrame
from ...utils import random_state_data

__all__ = ['make_timeseries']


def make_float(n, rstate):
    return rstate.rand(n) * 2 - 1


def make_int(n, rstate):
    return rstate.poisson(1000, size=n)


names = ['Alice', 'Bob', 'Charlie', 'Dan', 'Edith', 'Frank', 'George',
         'Hannah', 'Ingrid', 'Jerry', 'Kevin', 'Laura', 'Michael', 'Norbert',
         'Oliver', 'Patricia', 'Quinn', 'Ray', 'Sarah', 'Tim', 'Ursula',
         'Victor', 'Wendy', 'Xavier', 'Yvonne', 'Zelda']


def make_string(n, rstate):
    return rstate.choice(names, size=n)


def make_categorical(n, rstate):
    return pd.Categorical.from_codes(rstate.randint(0, len(names), size=n),
                                     names)


make = {float: make_float,
        int: make_int,
        str: make_string,
        object: make_string,
        'category': make_categorical}


def make_timeseries_part(start, end, dtypes, freq, state_data):
    index = pd.DatetimeIndex(start=start, end=end, freq=freq)
    state = np.random.RandomState(state_data)
    columns = dict((k, make[dt](len(index), state)) for k, dt in dtypes.items())
    df = pd.DataFrame(columns, index=index, columns=sorted(columns))
    if df.index[-1] == end:
        df = df.iloc[:-1]
    return df


def make_timeseries(start, end, dtypes, freq, partition_freq, seed=None):
    """ Create timeseries dataframe with random data

    Parameters
    ----------
    start: datetime (or datetime-like string)
        Start of time series
    end: datetime (or datetime-like string)
        End of time series
    dtypes: dict
        Mapping of column names to types.
        Valid types include {float, int, str, 'category'}
    freq: string
        String like '2s' or '1H' or '12W' for the time series frequency
    partition_freq: string
        String like '1M' or '2Y' to divide the dataframe into partitions
    seed: int (optional)
        Randomstate seed

    >>> import dask.dataframe as dd
    >>> df = dd.demo.make_timeseries('2000', '2010',
    ...                              {'value': float, 'name': str, 'id': int},
    ...                              freq='2H', partition_freq='1D', seed=1)
    >>> df.head()
                           id      name     value
    2000-01-01 00:00:00   969     Jerry -0.309014
    2000-01-01 02:00:00  1010       Ray -0.760675
    2000-01-01 04:00:00  1016  Patricia -0.063261
    2000-01-01 06:00:00   960   Charlie  0.788245
    2000-01-01 08:00:00  1031     Kevin  0.466002
    """
    divisions = list(pd.DatetimeIndex(start=start, end=end,
                                      freq=partition_freq))
    state_data = random_state_data(len(divisions) - 1, seed)
    name = 'make-timeseries-' + tokenize(start, end, dtypes, freq, partition_freq)
    dsk = {(name, i): (make_timeseries_part, divisions[i], divisions[i + 1],
                       dtypes, freq, state_data[i])
           for i in range(len(divisions) - 1)}
    head = make_timeseries_part('2000', '2000', dtypes, '1H', state_data[0])
    return DataFrame(dsk, name, head, divisions)
