import functools
from operator import getitem
from pprint import pformat

import numpy as np
import pandas as pd
from dask.dataframe import methods
from dask.dataframe.core import split_evenly
from dask.dataframe.utils import is_series_like
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype
from tlz import unique

from dask_expr._expr import Expr, Projection


class Repartition(Expr):
    """Abstract repartitioning expression"""

    _parameters = ["frame", "n", "new_divisions", "force"]
    _defaults = {"n": None, "new_divisions": None, "force": False}

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        if self.n is not None:
            x = self.optimize(fuse=False)
            return x._divisions()
        return self.new_divisions

    def _lower(self):
        if type(self) != Repartition:
            # This lower logic should not be inherited
            return None
        if self.n is not None:
            if self.n < self.frame.npartitions:
                return RepartitionToFewer(self.frame, self.n)
            else:
                original_divisions = divisions = pd.Series(
                    self.frame.divisions
                ).drop_duplicates()
                if self.frame.known_divisions and (
                    is_datetime64_any_dtype(divisions.dtype)
                    or is_numeric_dtype(divisions.dtype)
                ):
                    npartitions = self.n
                    df = self.frame
                    if is_datetime64_any_dtype(divisions.dtype):
                        divisions = divisions.values.astype("float64")

                    if is_series_like(divisions):
                        divisions = divisions.values

                    n = len(divisions)
                    divisions = np.interp(
                        x=np.linspace(0, n, npartitions + 1),
                        xp=np.linspace(0, n, n),
                        fp=divisions,
                    )
                    if is_datetime64_any_dtype(original_divisions.dtype):
                        divisions = methods.tolist(
                            pd.Series(divisions).astype(original_divisions.dtype)
                        )
                    elif np.issubdtype(original_divisions.dtype, np.integer):
                        divisions = divisions.astype(original_divisions.dtype)

                    if isinstance(divisions, np.ndarray):
                        divisions = divisions.tolist()

                    divisions = list(divisions)
                    divisions[0] = df.divisions[0]
                    divisions[-1] = df.divisions[-1]

                    # Ensure the computed divisions are unique
                    divisions = list(unique(divisions[:-1])) + [divisions[-1]]
                    return RepartitionDivisions(df, divisions, self.force)
                else:
                    return RepartitionToMore(self.frame, self.n)
        elif self.new_divisions:
            if tuple(self.new_divisions) == self.frame.divisions:
                return self.frame
            return RepartitionDivisions(self.frame, self.new_divisions, self.force)
        else:
            raise NotImplementedError()

    def _simplify_up(self, parent):
        # Reorder with column projection
        if isinstance(parent, Projection):
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])


class RepartitionToFewer(Repartition):
    """Reduce the partition count"""

    _parameters = ["frame", "n"]

    def _divisions(self):
        return tuple(self.frame.divisions[i] for i in self._partitions_boundaries)

    @functools.cached_property
    def _partitions_boundaries(self):
        npartitions = self.n
        npartitions_input = self.frame.npartitions
        assert npartitions_input > self.n

        npartitions_ratio = npartitions_input / npartitions
        new_partitions_boundaries = [
            int(new_partition_index * npartitions_ratio)
            for new_partition_index in range(npartitions + 1)
        ]

        if not isinstance(new_partitions_boundaries, list):
            new_partitions_boundaries = list(new_partitions_boundaries)
        if new_partitions_boundaries[0] > 0:
            new_partitions_boundaries.insert(0, 0)
        if new_partitions_boundaries[-1] < self.frame.npartitions:
            new_partitions_boundaries.append(self.frame.npartitions)
        return new_partitions_boundaries

    def _layer(self):
        new_partitions_boundaries = self._partitions_boundaries
        return {
            (self._name, i): (
                methods.concat,
                [(self.frame._name, j) for j in range(start, end)],
            )
            for i, (start, end) in enumerate(
                zip(new_partitions_boundaries, new_partitions_boundaries[1:])
            )
        }


class RepartitionToMore(Repartition):
    """Increase the partition count"""

    _parameters = ["frame", "n"]

    def _divisions(self):
        return (None,) * (1 + sum(self._nsplits))

    @functools.cached_property
    def _nsplits(self):
        df = self.frame
        div, mod = divmod(self.n, df.npartitions)
        nsplits = [div] * df.npartitions
        nsplits[-1] += mod
        if len(nsplits) != df.npartitions:
            raise ValueError(f"nsplits should have len={df.npartitions}")
        return nsplits

    def _layer(self):
        dsk = {}
        nsplits = self._nsplits
        df = self.frame
        new_name = self._name
        split_name = f"split-{new_name}"
        j = 0
        for i, k in enumerate(nsplits):
            if k == 1:
                dsk[new_name, j] = (df._name, i)
                j += 1
            else:
                dsk[split_name, i] = (split_evenly, (df._name, i), k)
                for jj in range(k):
                    dsk[new_name, j] = (getitem, (split_name, i), jj)
                    j += 1
        return dsk


class RepartitionDivisions(Repartition):
    """Repartition to specific divisions"""

    _parameters = ["frame", "new_divisions", "force"]
    _defaults = {"force": False}

    def _divisions(self):
        return self.new_divisions

    def _layer(self):
        # Simplify copy from dask.dataframe
        token = self._name.split("-")[-1]
        a = self.frame.divisions
        b = self.new_divisions
        name = self.frame._name
        out1 = "repartition-split-" + token
        out2 = self._name
        force = self.force

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
                d[(out1, k)] = (
                    methods.boundary_slice,
                    (name, i - 1),
                    a[i],
                    a[i],
                    False,
                )
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
                d[(out2, j - 1)] = (
                    methods.boundary_slice,
                    (name, 0),
                    a[0],
                    a[0],
                    False,
                )
            elif len(tmp) == 1:
                d[(out2, j - 1)] = tmp[0]
            else:
                if not tmp:
                    raise ValueError(
                        "check for duplicate partitions\nold:\n%s\n\n"
                        "new:\n%s\n\ncombined:\n%s"
                        % (pformat(a), pformat(b), pformat(c))
                    )
                d[(out2, j - 1)] = (methods.concat, tmp)
            j += 1
        return d
