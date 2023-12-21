import functools
import warnings

import pandas as pd
from dask.dataframe import methods
from dask.dataframe.dispatch import make_meta, meta_nonempty
from dask.dataframe.multi import concat_and_check
from dask.dataframe.utils import check_meta, strip_unknown_categories
from dask.utils import apply, is_dataframe_like, is_series_like

from dask_expr._expr import (
    AsType,
    Blockwise,
    Expr,
    Projection,
    are_co_aligned,
    determine_column_projection,
)


class Concat(Expr):
    _parameters = [
        "join",
        "ignore_order",
        "_kwargs",
        "axis",
        "ignore_unknown_divisions",
    ]
    _defaults = {
        "join": "outer",
        "ignore_order": False,
        "_kwargs": {},
        "axis": 0,
        "ignore_unknown_divisions": False,
    }

    def __str__(self):
        s = (
            "frames="
            + str(self.dependencies())
            + ", "
            + ", ".join(
                str(param) + "=" + str(operand)
                for param, operand in zip(self._parameters, self.operands)
                if operand != self._defaults.get(param)
            )
        )
        return f"{type(self).__name__}({s})"

    @property
    def _frames(self):
        return self.dependencies()

    @functools.cached_property
    def _meta(self):
        # ignore DataFrame without columns to avoid dtype upcasting
        meta = make_meta(
            methods.concat(
                [
                    meta_nonempty(df._meta)
                    for df in self._frames
                    if df.ndim < 2 or len(df._meta.columns) > 0
                ],
                join=self.join,
                filter_warning=False,
                axis=self.axis,
                ignore_order=self.ignore_order,
                **self._kwargs,
            )
        )
        return strip_unknown_categories(meta)

    def _divisions(self):
        dfs = self._frames

        if self.axis == 1:
            return (None,) * (max(df.npartitions for df in dfs) + 1)

        if all(df.known_divisions for df in dfs):
            # each DataFrame's division must be greater than previous one
            if all(
                dfs[i].divisions[-1] < dfs[i + 1].divisions[0]
                for i in range(len(dfs) - 1)
            ):
                divisions = []
                for df in dfs[:-1]:
                    # remove last to concatenate with next
                    divisions += df.divisions[:-1]
                divisions += dfs[-1].divisions
                return divisions

        return [None] * (sum(df.npartitions for df in dfs) + 1)

    def _lower(self):
        dfs = self._frames
        if self.axis == 1:
            if are_co_aligned(*self._frames) or {df.npartitions for df in dfs} == {1}:
                return ConcatIndexed(self.ignore_order, self._kwargs, self.axis, *dfs)

            elif (
                all(not df.known_divisions for df in dfs)
                and len({df.npartitions for df in dfs}) == 1
            ):
                if not self.ignore_unknown_divisions:
                    warnings.warn(
                        "Concatenating dataframes with unknown divisions.\n"
                        "We're assuming that the indices of each dataframes"
                        " are \n aligned. This assumption is not generally "
                        "safe."
                    )
                return ConcatUnindexed(self.ignore_order, self._kwargs, self.axis, *dfs)
            else:
                raise NotImplementedError

        cast_dfs = []
        for df in dfs:
            # dtypes of all dfs need to be coherent
            # refer to https://github.com/dask/dask/issues/4685
            # and https://github.com/dask/dask/issues/5968.
            if is_dataframe_like(df._meta):
                shared_columns = list(set(df.columns).intersection(self._meta.columns))
                needs_astype = {
                    col: self._meta[col].dtype
                    for col in shared_columns
                    if df._meta[col].dtype != self._meta[col].dtype
                    and not isinstance(df[col]._meta.dtype, pd.CategoricalDtype)
                }

                if needs_astype:
                    cast_dfs.append(AsType(df, dtypes=needs_astype))
                else:
                    cast_dfs.append(df)
            elif is_series_like(df) and is_series_like(self._meta):
                if not df.dtype == self._meta.dtype and not isinstance(
                    df.dtype, pd.CategoricalDtype
                ):
                    cast_dfs.append(AsType(df, dtypes=self._meta.dtype))
                else:
                    cast_dfs.append(df)
            else:
                cast_dfs.append(df)

        return StackPartition(
            self.join,
            self.ignore_order,
            self._kwargs,
            *cast_dfs,
        )

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Projection):

            def get_columns_or_name(e: Expr):
                return e.columns if e.ndim == 2 else [e.name]

            columns = determine_column_projection(self, parent, dependents)
            columns_frame = [
                [col for col in get_columns_or_name(frame) if col in columns]
                for frame in self._frames
            ]
            if all(
                sorted(cols) == sorted(get_columns_or_name(frame))
                for frame, cols in zip(self._frames, columns_frame)
            ):
                return

            frames = [
                frame[cols]
                if sorted(cols) != sorted(get_columns_or_name(frame))
                else frame
                for frame, cols in zip(self._frames, columns_frame)
                if len(cols) > 0
            ]
            return type(parent)(
                type(self)(
                    self.join,
                    self.ignore_order,
                    self._kwargs,
                    self.axis,
                    self.ignore_unknown_divisions,
                    *frames,
                ),
                *parent.operands[1:],
            )


class StackPartition(Concat):
    _parameters = ["join", "ignore_order", "_kwargs"]
    _defaults = {"join": "outer", "ignore_order": False, "_kwargs": {}}

    @property
    def axis(self):
        return 0

    def _layer(self):
        dsk, i = {}, 0
        kwargs = self._kwargs.copy()
        kwargs["ignore_order"] = self.ignore_order
        ctr = 0
        for df in self._frames:
            try:
                check_meta(df._meta, self._meta)
                match = True
            except (ValueError, TypeError):
                match = False

            for i in range(df.npartitions):
                if match:
                    dsk[(self._name, ctr)] = df._name, i
                else:
                    dsk[(self._name, ctr)] = (
                        apply,
                        methods.concat,
                        [
                            [self._meta, (df._name, i)],
                            self.axis,
                            self.join,
                            False,
                            True,
                        ],
                        kwargs,
                    )
                ctr += 1
        return dsk

    def _lower(self):
        return


class ConcatUnindexed(Blockwise):
    _parameters = ["ignore_order", "_kwargs", "axis"]
    _defaults = {"ignore_order": False, "_kwargs": {}, "axis": 1}
    _keyword_only = ["ignore_order", "_kwargs", "axis"]

    @functools.cached_property
    def _meta(self):
        return methods.concat(
            [df._meta for df in self.dependencies()],
            ignore_order=self.ignore_order,
            axis=self.axis,
            **self.operand("_kwargs"),
        )

    @staticmethod
    def operation(*args, ignore_order, _kwargs, axis):
        return concat_and_check(args, ignore_order=ignore_order)


class ConcatIndexed(ConcatUnindexed):
    @staticmethod
    def operation(*args, ignore_order, _kwargs, axis):
        return methods.concat(args, ignore_order=ignore_order, axis=axis)
