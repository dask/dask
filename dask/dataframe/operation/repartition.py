from dataclasses import dataclass
from functools import cached_property
from pprint import pformat
from typing import Any

import pandas as pd
from tlz import unique

from dask.base import tokenize
from dask.core import keys_in_tasks
from dask.dataframe.operation.core import (
    FrameCreation,
    PartitionKey,
    _Frame,
    _FrameOperation,
    _SimpleFrameOperation,
    new_dd_collection,
)
from dask.utils import is_dataframe_like, is_series_like

no_default = "__no_default__"


@dataclass(frozen=True)
class RepartitionDivisions(_SimpleFrameOperation):

    source: _FrameOperation
    _divisions: tuple
    force: bool
    _meta: Any = no_default

    @cached_property
    def token(self) -> str:
        return tokenize(self.meta, self.divisions, self.dependencies, self.force)

    @cached_property
    def name(self) -> str:
        return f"repartition-merge-{self.token}"

    def subgraph(self, keys: list[PartitionKey]) -> tuple[dict, dict]:

        # Use repartition_divisions to build graph
        tmp = f"repartition-split-{self.token}"
        out = self.name
        original_divisions = self.source.divisions
        new_divisions = self.divisions
        original_name = self.source.name
        dsk = repartition_divisions(
            original_divisions, new_divisions, original_name, tmp, out, force=self.force
        )

        # Use keys_in_tasks to find key_dependencies in source
        keys = self.source.collection_keys
        dep_keys = set()
        for v in dsk.values():
            dep_keys |= keys_in_tasks(keys, [v])
        dep_keys = {keys_in_tasks(keys, [v]) for k, v in dsk.items()}

        return dsk, {self.source: list(dep_keys)}

    def __hash__(self):
        return hash(self.name)


def repartition(df, divisions=None, force=False):
    if isinstance(df, _Frame):
        return new_dd_collection(RepartitionDivisions(df.operation, divisions, force))
    elif is_dataframe_like(df) or is_series_like(df):
        from dask.dataframe.utils import shard_df_on_index

        dfs = shard_df_on_index(df, divisions[1:-1])
        return new_dd_collection(
            FrameCreation(
                lambda x: x,
                dfs,
                "repartition-dataframe",
                df,
                divisions,
            )
        )
    raise ValueError("Data must be DataFrame or Series")


def check_divisions(divisions):
    if not isinstance(divisions, (list, tuple)):
        raise ValueError("New division must be list or tuple")
    divisions = list(divisions)
    if len(divisions) == 0:
        raise ValueError("New division must not be empty")
    if divisions != sorted(divisions):
        raise ValueError("New division must be sorted")
    if len(divisions[:-1]) != len(list(unique(divisions[:-1]))):
        msg = "New division must be unique, except for the last element"
        raise ValueError(msg)


def repartition_divisions(a, b, name, out1, out2, force=False):
    """dask graph to repartition dataframe by new divisions

    Parameters
    ----------
    a : tuple
        old divisions
    b : tuple, listmypy
    out2 : str
        name of new dataframe
    force : bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(repartition_divisions([1, 3, 7], [1, 4, 6, 7], 'a', 'b', 'c'))  # doctest: +ELLIPSIS
    {('b', 0): (<function boundary_slice at ...>, ('a', 0), 1, 3, False),
     ('b', 1): (<function boundary_slice at ...>, ('a', 1), 3, 4, False),
     ('b', 2): (<function boundary_slice at ...>, ('a', 1), 4, 6, False),
     ('b', 3): (<function boundary_slice at ...>, ('a', 1), 6, 7, True),
     ('c', 0): (<function concat at ...>, [('b', 0), ('b', 1)]),
     ('c', 1): ('b', 2),
     ('c', 2): ('b', 3)}
    """
    from dask.dataframe import methods

    check_divisions(b)

    if len(b) < 2:
        # minimum division is 2 elements, like [0, 0]
        raise ValueError("New division must be longer than 2 elements")

    if force:
        if a[0] < b[0]:
            msg = (
                "left side of the new division must be equal or smaller "
                "than old division"
            )
            raise ValueError(msg)
        if a[-1] > b[-1]:
            msg = (
                "right side of the new division must be equal or larger "
                "than old division"
            )
            raise ValueError(msg)
    else:
        if a[0] != b[0]:
            msg = "left side of old and new divisions are different"
            raise ValueError(msg)
        if a[-1] != b[-1]:
            msg = "right side of old and new divisions are different"
            raise ValueError(msg)

    def _is_single_last_div(x):
        """Whether last division only contains single label"""
        return len(x) >= 2 and x[-1] == x[-2]

    c = [a[0]]
    d = dict()
    low = a[0]

    i, j = 1, 1  # indices for old/new divisions
    k = 0  # index for temp divisions

    last_elem = _is_single_last_div(a)

    # process through old division
    # left part of new division can be processed in this loop
    while i < len(a) and j < len(b):
        if a[i] < b[j]:
            # tuple is something like:
            # (methods.boundary_slice, ('from_pandas-#', 0), 3, 4, False))
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, a[i], False)
            low = a[i]
            i += 1
        elif a[i] > b[j]:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            j += 1
        else:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            if len(a) == i + 1 or a[i] < a[i + 1]:
                j += 1
            i += 1
        c.append(low)
        k += 1

    # right part of new division can remain
    if a[-1] < b[-1] or b[-1] == b[-2]:
        for _j in range(j, len(b)):
            # always use right-most of old division
            # because it may contain last element
            m = len(a) - 2
            d[(out1, k)] = (methods.boundary_slice, (name, m), low, b[_j], False)
            low = b[_j]
            c.append(low)
            k += 1
    else:
        # even if new division is processed through,
        # right-most element of old division can remain
        if last_elem and i < len(a):
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), a[i], a[i], False)
            k += 1
        c.append(a[-1])

    # replace last element of tuple with True
    d[(out1, k - 1)] = d[(out1, k - 1)][:-1] + (True,)

    i, j = 0, 1

    last_elem = _is_single_last_div(c)

    while j < len(b):
        tmp = []
        while c[i] < b[j]:
            tmp.append((out1, i))
            i += 1
        while (
            last_elem
            and c[i] == b[-1]
            and (b[-1] != b[-2] or j == len(b) - 1)
            and i < k
        ):
            # append if last split is not included
            tmp.append((out1, i))
            i += 1
        if len(tmp) == 0:
            # dummy slice to return empty DataFrame or Series,
            # which retain original data attributes (columns / name)
            d[(out2, j - 1)] = (methods.boundary_slice, (name, 0), a[0], a[0], False)
        elif len(tmp) == 1:
            d[(out2, j - 1)] = tmp[0]
        else:
            if not tmp:
                raise ValueError(
                    "check for duplicate partitions\nold:\n%s\n\n"
                    "new:\n%s\n\ncombined:\n%s" % (pformat(a), pformat(b), pformat(c))
                )
            d[(out2, j - 1)] = (methods.concat, tmp)
        j += 1
    return d


def repartition_freq(df, freq=None):
    """Repartition a timeseries dataframe by a new frequency"""
    from dask.dataframe import methods

    if not isinstance(df.divisions[0], pd.Timestamp):
        raise TypeError("Can only repartition on frequency for timeseries")

    freq = _map_freq_to_period_start(freq)

    try:
        start = df.divisions[0].ceil(freq)
    except ValueError:
        start = df.divisions[0]
    divisions = methods.tolist(
        pd.date_range(start=start, end=df.divisions[-1], freq=freq)
    )
    if not len(divisions):
        divisions = [df.divisions[0], df.divisions[-1]]
    else:
        divisions.append(df.divisions[-1])
        if divisions[0] != df.divisions[0]:
            divisions = [df.divisions[0]] + divisions

    return df.repartition(divisions=divisions)


def _map_freq_to_period_start(freq):
    """Ensure that the frequency pertains to the **start** of a period.

    If e.g. `freq='M'`, then the divisions are:
        - 2021-31-1 00:00:00 (start of February partition)
        - 2021-2-28 00:00:00 (start of March partition)
        - ...

    but this **should** be:
        - 2021-2-1 00:00:00 (start of February partition)
        - 2021-3-1 00:00:00 (start of March partition)
        - ...

    Therefore, we map `freq='M'` to `freq='MS'` (same for quarter and year).
    """

    if not isinstance(freq, str):
        return freq

    offset = pd.tseries.frequencies.to_offset(freq)
    offset_type_name = type(offset).__name__

    if not offset_type_name.endswith("End"):
        return freq

    new_offset = offset_type_name[: -len("End")] + "Begin"
    try:
        new_offset_type = getattr(pd.tseries.offsets, new_offset)
        if "-" in freq:
            _, anchor = freq.split("-")
            anchor = "-" + anchor
        else:
            anchor = ""
        n = str(offset.n) if offset.n != 1 else ""
        return f"{n}{new_offset_type._prefix}{anchor}"
    except AttributeError:
        return freq
