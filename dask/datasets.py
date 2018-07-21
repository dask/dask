from __future__ import absolute_import, print_function, division


def timeseries(
    start='2000-01-01',
    end='2000-01-31',
    freq='1s',
    partition_freq='1d',
    dtypes={'name': str, 'id': int, 'x': float, 'y': float},
    seed=None,
):
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

    Examples
    --------
    >>> import dask
    >>> df = dask.datasets.timeseries()
    >>> df.head()  # doctest: +SKIP
              timestamp    id     name         x         y
    2000-01-01 00:00:00   967    Jerry -0.031348 -0.040633
    2000-01-01 00:00:01  1066  Michael -0.262136  0.307107
    2000-01-01 00:00:02   988    Wendy -0.526331  0.128641
    2000-01-01 00:00:03  1016   Yvonne  0.620456  0.767270
    2000-01-01 00:00:04   998   Ursula  0.684902 -0.463278
    """
    from dask.dataframe.io.demo import make_timeseries
    return make_timeseries(start=start, end=end, freq=freq,
                           partition_freq=partition_freq,
                           seed=seed, dtypes=dtypes)
