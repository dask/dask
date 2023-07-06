from __future__ import annotations

import string
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

from dask.dataframe.core import tokenize
from dask.dataframe.io.io import from_map
from dask.dataframe.io.utils import DataFrameIOFunction
from dask.utils import random_state_data

__all__ = ["make_timeseries", "with_spec", "ColumnSpec", "IndexSpec", "DatasetSpec"]


@dataclass
class ColumnSpec:
    prefix: str | None = None
    dtype: str | type | None = None
    number: int = 1
    choices: list = field(default_factory=list)
    nunique: int | None = None
    low: int | None = None
    high: int | None = None
    length: int | None = None
    random: bool = False


@dataclass
class IndexSpec:
    dtype: str | type = int
    freq: int | str = 1
    monotonic_increasing: bool = True


@dataclass
class DatasetSpec:
    npartitions: int = 1
    nrecords: int = 1000
    index_spec: IndexSpec = field(default_factory=IndexSpec)
    column_specs: list[ColumnSpec] = field(default_factory=list)


def make_float(n, rstate, random=False, dtype=None, **kwargs):
    if random:
        data = rstate.random(size=n, **kwargs)
        if dtype:
            data = data.astype(dtype)
        return data
    return rstate.rand(n) * 2 - 1


def make_int(n, rstate, lam=1000, random=False, **kwargs):
    if random:
        return rstate.randint(size=n, **kwargs)
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


def make_random_string(n, rstate, length: int = 25) -> list[str]:
    choices = list(string.ascii_letters + string.digits + string.punctuation + " ")
    return ["".join(rstate.choice(choices, size=length)) for _ in range(n)]


def make_string(n, rstate, choices=None, random=False, length=None, **_):
    if random:
        return make_random_string(n, rstate, length=length)
    choices = choices or names
    return rstate.choice(choices, size=n)


def make_categorical(n, rstate, choices=None, **_):
    choices = choices or names
    return pd.Categorical.from_codes(rstate.randint(0, len(choices), size=n), choices)


make: dict[type | str, Callable] = {
    float: make_float,
    int: make_int,
    str: make_string,
    object: make_string,
    "category": make_categorical,
    "int8": make_int,
    "int16": make_int,
    "int32": make_int,
    "int64": make_int,
    "float8": make_float,
    "float16": make_float,
    "float32": make_float,
    "float64": make_float,
}


class MakeDataframePart(DataFrameIOFunction):
    """
    Wrapper Class for ``make_dataframe_part``
    Makes a timeseries partition.
    """

    def __init__(self, index_dtype, dtypes, kwargs, columns=None):
        self.index_dtype = index_dtype
        self._columns = columns or list(dtypes.keys())
        self.dtypes = dtypes
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
        return MakeDataframePart(
            self.index_dtype,
            self.dtypes,
            self.kwargs,
            columns=columns,
        )

    def __call__(self, part):
        divisions, state_data = part
        if isinstance(state_data, int):
            state_data = random_state_data(1, state_data)
        return make_dataframe_part(
            self.index_dtype,
            divisions[0],
            divisions[1],
            self.dtypes,
            self.columns,
            state_data,
            self.kwargs,
        )


def make_dataframe_part(index_dtype, start, end, dtypes, columns, state_data, kwargs):
    state = np.random.RandomState(state_data)
    if pd.api.types.is_datetime64_any_dtype(index_dtype):
        index = pd.date_range(
            start=start, end=end, freq=kwargs.get("freq"), name="timestamp"
        )
    elif pd.api.types.is_integer_dtype(index_dtype):
        step = kwargs.get("freq")
        index = pd.RangeIndex(start=start, stop=end + step, step=step).astype(
            index_dtype
        )
    else:
        raise TypeError(f"Unhandled index dtype: {index_dtype}")
    df = make_partition(columns, dtypes, index, kwargs, state)
    while df.index[-1] >= end:
        df = df.iloc[:-1]
    return df


def make_partition(columns: list, dtypes: dict[str, type | str], index, kwargs, state):
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
    return pd.DataFrame(data, index=index, columns=columns)


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
        # call `random_state_data` in `MakeDataframePart`
        state_data = np.random.randint(2e9, size=npartitions)
    else:
        state_data = random_state_data(npartitions, seed)

    # Build parts
    parts = []
    for i in range(len(divisions) - 1):
        parts.append((divisions[i : i + 2], state_data[i]))

    kwargs["freq"] = freq
    index_dtype = "datetime64[ns]"
    meta_start, meta_end = list(pd.date_range(start="2000", freq=freq, periods=2))

    # Construct the output collection with from_map
    return from_map(
        MakeDataframePart(index_dtype, dtypes, kwargs),
        parts,
        meta=make_dataframe_part(
            index_dtype,
            meta_start,
            meta_end,
            dtypes,
            list(dtypes.keys()),
            state_data[0],
            kwargs,
        ),
        divisions=divisions,
        label="make-timeseries",
        token=tokenize(start, end, dtypes, freq, partition_freq, state_data),
        enforce_metadata=False,
    )


def with_spec(spec: DatasetSpec, seed: int | None = None):
    """Generate a random dataset according to provided spec

    Parameters
    ----------
    spec : DatasetSpec
        Specify all the parameters of the dataset
    seed: int (optional)
        Randomstate seed
    """
    if len(spec.column_specs) == 0:
        spec.column_specs = [
            ColumnSpec(prefix="i", dtype=int, low=0, high=1_000_000, random=True),
            ColumnSpec(prefix="f", dtype=float, random=True),
            ColumnSpec(prefix="c", dtype="category", choices=["a", "b", "c", "d"]),
            ColumnSpec(prefix="s", dtype=str, length=25),
        ]

    columns = []
    dtypes = {}
    step = int(spec.index_spec.freq)
    kwargs: dict[str, Any] = {"freq": step}
    for col in spec.column_specs:
        if col.prefix:
            prefix = col.prefix
        elif isinstance(col.dtype, str):
            prefix = col.dtype
        elif hasattr(col.dtype, "name"):
            prefix = col.dtype.name  # type: ignore
        else:
            prefix = col.dtype.__name__  # type: ignore
        for i in range(col.number):
            col_name = f"{prefix}{i + 1}"
            columns.append(col_name)
            dtypes[col_name] = col.dtype
            kwargs.update(
                {
                    f"{col_name}_{k}": v
                    for k, v in asdict(col).items()
                    if k not in {"prefix", "number"} and v not in (None, [])
                }
            )

    partition_freq = spec.nrecords * step // spec.npartitions
    end = spec.nrecords * step - 1
    divisions = list(pd.RangeIndex(0, stop=end, step=partition_freq))
    if divisions[-1] < (end + 1):
        divisions.append(end + 1)

    npartitions = len(divisions) - 1
    if seed is None:
        state_data = np.random.randint(int(2e9), size=npartitions)
    else:
        state_data = np.ndarray(random_state_data(npartitions, seed))

    parts = [(divisions[i : i + 2], state_data[i]) for i in range(npartitions)]

    return from_map(
        MakeDataframePart(spec.index_spec.dtype, dtypes, kwargs, columns=columns),
        parts,
        meta=make_dataframe_part(
            spec.index_spec.dtype, 0, step, dtypes, columns, state_data[0], kwargs
        ),
        divisions=divisions,
        label="make-random",
        token=tokenize(0, spec.nrecords, dtypes, step, partition_freq, state_data),
        enforce_metadata=False,
    )
