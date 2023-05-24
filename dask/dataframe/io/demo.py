from __future__ import annotations

import numpy as np
import pandas as pd

from dask.dataframe.core import tokenize
from dask.dataframe.io.io import from_map
from dask.dataframe.io.utils import DataFrameIOFunction
from dask.utils import random_state_data

__all__ = ["make_timeseries"]


def make_float(n, rstate):
    return rstate.rand(n) * 2 - 1


def make_int(n, rstate, lam=1000):
    return rstate.poisson(lam, size=n)


names = [
    "Alice",
    "Bob",
    "Charlie",
    "Dan",
    "Edith",
    "Frank",
    "George",
    "Hannah",
    "Ingrid",
    "Jerry",
    "Kevin",
    "Laura",
    "Michael",
    "Norbert",
    "Oliver",
    "Patricia",
    "Quinn",
    "Ray",
    "Sarah",
    "Tim",
    "Ursula",
    "Victor",
    "Wendy",
    "Xavier",
    "Yvonne",
    "Zelda",
]


def make_string(n, rstate):
    return rstate.choice(names, size=n)


def make_categorical(n, rstate):
    return pd.Categorical.from_codes(rstate.randint(0, len(names), size=n), names)


make = {
    float: make_float,
    int: make_int,
    str: make_string,
    object: make_string,
    "category": make_categorical,
}


class MakeTimeseriesPart(DataFrameIOFunction):
    """
    Wrapper Class for ``make_timeseries_part``
    Makes a timeseries partition.
    """

    def __init__(self, dtypes, freq, kwargs, columns=None):
        self._columns = columns or list(dtypes.keys())
        self.dtypes = dtypes
        self.freq = freq
        self.kwargs = kwargs

    @property
    def columns(self):
        return self._columns

    def project_columns(self, columns):
        """Return a new MakeTimeseriesPart object with
        a sub-column projection.
        """
        if columns == self.columns:
            return self
        return MakeTimeseriesPart(
            self.dtypes,
            self.freq,
            self.kwargs,
            columns=columns,
        )

    def __call__(self, part):
        divisions, state_data = part
        if isinstance(state_data, int):
            state_data = random_state_data(1, state_data)
        return make_timeseries_part(
            divisions[0],
            divisions[1],
            self.dtypes,
            self.columns,
            self.freq,
            state_data,
            self.kwargs,
        )


def make_timeseries_part(start, end, dtypes, columns, freq, state_data, kwargs):
    index = pd.date_range(start=start, end=end, freq=freq, name="timestamp")
    state = np.random.RandomState(state_data)
    data = {}
    for k, dt in dtypes.items():
        kws = {
            kk.rsplit("_", 1)[1]: v
            for kk, v in kwargs.items()
            if kk.rsplit("_", 1)[0] == k
        }
        # Note: we compute data for all dtypes in order, not just those in the output
        # columns. This ensures the same output given the same state_data, regardless
        # of whether there is any column projection.
        # cf. https://github.com/dask/dask/pull/9538#issuecomment-1267461887
        result = make[dt](len(index), state, **kws)
        if k in columns:
            data[k] = result
    df = pd.DataFrame(data, index=index, columns=columns)
    if df.index[-1] == end:
        df = df.iloc[:-1]
    return df


def make_timeseries(
    start="2000-01-01",
    end="2000-12-31",
    dtypes=None,
    freq="10s",
    partition_freq="1M",
    seed=None,
    **kwargs,
):
    """Create timeseries dataframe with random data

    Parameters
    ----------
    start: datetime (or datetime-like string)
        Start of time series
    end: datetime (or datetime-like string)
        End of time series
    dtypes: dict (optional)
        Mapping of column names to types.
        Valid types include {float, int, str, 'category'}
    freq: string
        String like '2s' or '1H' or '12W' for the time series frequency
    partition_freq: string
        String like '1M' or '2Y' to divide the dataframe into partitions
    seed: int (optional)
        Randomstate seed
    kwargs:
        Keywords to pass down to individual column creation functions.
        Keywords should be prefixed by the column name and then an underscore.

    Examples
    --------
    >>> import dask.dataframe as dd
    >>> df = dd.demo.make_timeseries('2000', '2010',
    ...                              {'value': float, 'name': str, 'id': int},
    ...                              freq='2H', partition_freq='1D', seed=1)
    >>> df.head()  # doctest: +SKIP
                           id      name     value
    2000-01-01 00:00:00   969     Jerry -0.309014
    2000-01-01 02:00:00  1010       Ray -0.760675
    2000-01-01 04:00:00  1016  Patricia -0.063261
    2000-01-01 06:00:00   960   Charlie  0.788245
    2000-01-01 08:00:00  1031     Kevin  0.466002
    """
    if dtypes is None:
        dtypes = {"name": str, "id": int, "x": float, "y": float}

    divisions = list(pd.date_range(start=start, end=end, freq=partition_freq))
    npartitions = len(divisions) - 1
    if seed is None:
        # Get random integer seed for each partition. We can
        # call `random_state_data` in `MakeTimeseriesPart`
        state_data = np.random.randint(2e9, size=npartitions)
    else:
        state_data = random_state_data(npartitions, seed)

    # Build parts
    parts = []
    for i in range(len(divisions) - 1):
        parts.append((divisions[i : i + 2], state_data[i]))

    # Construct the output collection with from_map
    return from_map(
        MakeTimeseriesPart(dtypes, freq, kwargs),
        parts,
        meta=make_timeseries_part(
            "2000", "2000", dtypes, list(dtypes.keys()), "1H", state_data[0], kwargs
        ),
        divisions=divisions,
        label="make-timeseries",
        token=tokenize(start, end, dtypes, freq, partition_freq, state_data),
        enforce_metadata=False,
    )
