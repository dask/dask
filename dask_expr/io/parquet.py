from __future__ import annotations

import itertools
import operator
from collections import defaultdict
from functools import cached_property

import dask
import pyarrow.parquet as pq
from dask.dataframe.io.parquet.core import (
    ParquetFunctionWrapper,
    aggregate_row_groups,
    get_engine,
    set_index_columns,
    sorted_columns,
)
from dask.dataframe.io.parquet.utils import _split_user_options
from dask.dataframe.io.utils import _is_local_fs
from dask.delayed import delayed
from dask.utils import natural_sort_key

from dask_expr.expr import (
    EQ,
    GE,
    GT,
    LE,
    LT,
    NE,
    And,
    Expr,
    Filter,
    Lengths,
    Literal,
    Or,
    Projection,
)
from dask_expr.io import BlockwiseIO, PartitionsFiltered
from dask_expr.reductions import Len

NONE_LABEL = "__null_dask_index__"


class ReadParquet(PartitionsFiltered, BlockwiseIO):
    """Read a parquet dataset"""

    _parameters = [
        "path",
        "columns",
        "filters",
        "categories",
        "index",
        "storage_options",
        "calculate_divisions",
        "ignore_metadata_file",
        "metadata_task_size",
        "split_row_groups",
        "blocksize",
        "aggregate_files",
        "parquet_file_extension",
        "filesystem",
        "kwargs",
        "_partitions",
        "_series",
    ]
    _defaults = {
        "columns": None,
        "filters": None,
        "categories": None,
        "index": None,
        "storage_options": None,
        "calculate_divisions": False,
        "ignore_metadata_file": False,
        "metadata_task_size": None,
        "split_row_groups": "infer",
        "blocksize": "default",
        "aggregate_files": None,
        "parquet_file_extension": (".parq", ".parquet", ".pq"),
        "filesystem": "fsspec",
        "kwargs": None,
        "_partitions": None,
        "_series": False,
    }
    _pq_length_stats = None

    @property
    def engine(self):
        return get_engine("pyarrow")

    @property
    def columns(self):
        columns_operand = self.operand("columns")
        if columns_operand is None:
            return self._meta.columns
        else:
            import pandas as pd

            return pd.Index(_list_columns(columns_operand))

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            # Column projection
            operands = list(self.operands)
            operands[self._parameters.index("columns")] = _list_columns(
                parent.operand("columns")
            )
            if isinstance(parent.operand("columns"), (str, int)):
                operands[self._parameters.index("_series")] = True
            return ReadParquet(*operands)

        if isinstance(parent, Filter) and isinstance(
            parent.predicate, (LE, GE, LT, GT, EQ, NE, And, Or)
        ):
            # Predicate pushdown
            filters = _DNF.extract_pq_filters(self, parent.predicate)
            if filters:
                kwargs = dict(zip(self._parameters, self.operands))
                kwargs["filters"] = filters.combine(kwargs["filters"]).to_list_tuple()
                return ReadParquet(**kwargs)

        if isinstance(parent, Lengths):
            _lengths = self._get_lengths()
            if _lengths:
                return Literal(_lengths)

        if isinstance(parent, Len):
            _lengths = self._get_lengths()
            if _lengths:
                return Literal(sum(_lengths))

    @cached_property
    def _dataset_info(self):
        # Process and split user options
        (
            dataset_options,
            read_options,
            open_file_options,
            other_options,
        ) = _split_user_options(**(self.kwargs or {}))

        # Extract global filesystem and paths
        fs, paths, dataset_options, open_file_options = self.engine.extract_filesystem(
            self.path,
            self.filesystem,
            dataset_options,
            open_file_options,
            self.storage_options,
        )
        read_options["open_file_options"] = open_file_options
        paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

        auto_index_allowed = False
        index_operand = self.operand("index")
        if index_operand is None:
            # User is allowing auto-detected index
            auto_index_allowed = True
        if index_operand and isinstance(index_operand, str):
            index = [index_operand]
        else:
            index = index_operand

        blocksize = self.blocksize
        if self.split_row_groups in ("infer", "adaptive"):
            # Using blocksize to plan partitioning
            if self.blocksize == "default":
                if hasattr(self.engine, "default_blocksize"):
                    blocksize = self.engine.default_blocksize()
                else:
                    blocksize = "128MiB"
        else:
            # Not using blocksize - Set to `None`
            blocksize = None

        dataset_info = self.engine._collect_dataset_info(
            paths,
            fs,
            self.categories,
            index,
            self.calculate_divisions,
            self.filters,
            self.split_row_groups,
            blocksize,
            self.aggregate_files,
            self.ignore_metadata_file,
            self.metadata_task_size,
            self.parquet_file_extension,
            {
                "read": read_options,
                "dataset": dataset_options,
                **other_options,
            },
        )

        # Infer meta, accounting for index and columns arguments.
        meta = self.engine._create_dd_meta(dataset_info)
        index = dataset_info["index"]
        index = [index] if isinstance(index, str) else index
        meta, index, columns = set_index_columns(
            meta, index, self.operand("columns"), auto_index_allowed
        )
        if meta.index.name == NONE_LABEL:
            meta.index.name = None
        dataset_info["meta"] = meta
        dataset_info["index"] = index
        dataset_info["columns"] = columns

        return dataset_info

    @property
    def _meta(self):
        meta = self._dataset_info["meta"]
        if self._series:
            column = _list_columns(self.operand("columns"))[0]
            return meta[column]
        return meta

    @cached_property
    def _plan(self):
        dataset_info = self._dataset_info
        parts, stats, common_kwargs = self.engine._construct_collection_plan(
            dataset_info
        )

        # Make sure parts and stats are aligned
        parts, stats = _align_statistics(parts, stats)

        # Use statistics to aggregate partitions
        parts, stats = _aggregate_row_groups(parts, stats, dataset_info)

        # Use statistics to calculate divisions
        divisions = _calculate_divisions(stats, dataset_info, len(parts))

        meta = dataset_info["meta"]
        if len(divisions) < 2:
            # empty dataframe - just use meta
            divisions = (None, None)
            io_func = lambda x: x
            parts = [meta]
        else:
            # Use IO function wrapper
            io_func = ParquetFunctionWrapper(
                self.engine,
                dataset_info["fs"],
                meta,
                dataset_info["columns"],
                dataset_info["index"],
                dataset_info["kwargs"]["dtype_backend"],
                {},  # All kwargs should now be in `common_kwargs`
                common_kwargs,
            )

        return {
            "func": io_func,
            "parts": parts,
            "statistics": stats,
            "divisions": divisions,
        }

    def _divisions(self):
        return self._plan["divisions"]

    def _filtered_task(self, index: int):
        tsk = (self._plan["func"], self._plan["parts"][index])
        if self._series:
            return (operator.getitem, tsk, self.columns[0])
        return tsk

    def _get_lengths(self) -> tuple | None:
        """Return known partition lengths using parquet statistics"""
        if not self.filters:
            self._update_length_statistics()
            return tuple(
                length
                for i, length in enumerate(self._pq_length_stats)
                if not self._filtered or i in self._partitions
            )

    def _update_length_statistics(self):
        """Ensure that partition-length statistics are up to date"""

        if not self._pq_length_stats:
            if self._plan["statistics"]:
                # Already have statistics from original API call
                self._pq_length_stats = tuple(
                    stat["num-rows"]
                    for i, stat in enumerate(self._plan["statistics"])
                    if not self._filtered or i in self._partitions
                )
            else:
                # Need to go back and collect statistics
                self._pq_length_stats = tuple(
                    stat["num-rows"] for stat in _collect_pq_statistics(self)
                )


#
# Helper functions
#


def _list_columns(columns):
    # Simple utility to convert columns to list
    if isinstance(columns, (str, int)):
        columns = [columns]
    elif isinstance(columns, tuple):
        columns = list(columns)
    return columns


def _align_statistics(parts, statistics):
    # Make sure parts and statistics are aligned
    # (if statistics is not empty)
    if statistics and len(parts) != len(statistics):
        statistics = []
    if statistics:
        result = list(
            zip(
                *[
                    (part, stats)
                    for part, stats in zip(parts, statistics)
                    if stats["num-rows"] > 0
                ]
            )
        )
        parts, statistics = result or [[], []]
    return parts, statistics


def _aggregate_row_groups(parts, statistics, dataset_info):
    # Aggregate parts/statistics if we are splitting by row-group
    blocksize = (
        dataset_info["blocksize"] if dataset_info["split_row_groups"] is True else None
    )
    split_row_groups = dataset_info["split_row_groups"]
    fs = dataset_info["fs"]
    aggregation_depth = dataset_info["aggregation_depth"]

    if statistics:
        if blocksize or (split_row_groups and int(split_row_groups) > 1):
            parts, statistics = aggregate_row_groups(
                parts, statistics, blocksize, split_row_groups, fs, aggregation_depth
            )
    return parts, statistics


def _calculate_divisions(statistics, dataset_info, npartitions):
    # Use statistics to define divisions
    divisions = None
    if statistics:
        calculate_divisions = dataset_info["kwargs"].get("calculate_divisions", None)
        index = dataset_info["index"]
        process_columns = index if index and len(index) == 1 else None
        if (calculate_divisions is not False) and process_columns:
            for sorted_column_info in sorted_columns(
                statistics, columns=process_columns
            ):
                if sorted_column_info["name"] in index:
                    divisions = sorted_column_info["divisions"]
                    break

    return divisions or (None,) * (npartitions + 1)


#
# Filtering logic
#


class _DNF:
    """Manage filters in Disjunctive Normal Form (DNF)"""

    class _Or(frozenset):
        """Fozen set of disjunctions"""

        def to_list_tuple(self) -> list:
            # DNF "or" is List[List[Tuple]]
            def _maybe_list(val):
                if isinstance(val, tuple) and val and isinstance(val[0], (tuple, list)):
                    return list(val)
                return [val]

            return [
                _maybe_list(val.to_list_tuple())
                if hasattr(val, "to_list_tuple")
                else _maybe_list(val)
                for val in self
            ]

    class _And(frozenset):
        """Frozen set of conjunctions"""

        def to_list_tuple(self) -> list:
            # DNF "and" is List[Tuple]
            return tuple(
                val.to_list_tuple() if hasattr(val, "to_list_tuple") else val
                for val in self
            )

    _filters: _And | _Or | None  # Underlying filter expression

    def __init__(self, filters: _And | _Or | list | tuple | None) -> _DNF:
        self._filters = self.normalize(filters)

    def to_list_tuple(self) -> list:
        return self._filters.to_list_tuple()

    def __bool__(self) -> bool:
        return bool(self._filters)

    @classmethod
    def normalize(cls, filters: _And | _Or | list | tuple | None):
        """Convert raw filters to the `_Or(_And)` DNF representation"""
        if not filters:
            result = None
        elif isinstance(filters, list):
            conjunctions = filters if isinstance(filters[0], list) else [filters]
            result = cls._Or([cls._And(conjunction) for conjunction in conjunctions])
        elif isinstance(filters, tuple):
            if isinstance(filters[0], tuple):
                raise TypeError("filters must be List[Tuple] or List[List[Tuple]]")
            result = cls._Or((cls._And((filters,)),))
        elif isinstance(filters, cls._Or):
            result = cls._Or(se for e in filters for se in cls.normalize(e))
        elif isinstance(filters, cls._And):
            total = []
            for c in itertools.product(*[cls.normalize(e) for e in filters]):
                total.append(cls._And(se for e in c for se in e))
            result = cls._Or(total)
        else:
            raise TypeError(f"{type(filters)} not a supported type for _DNF")
        return result

    def combine(self, other: _DNF | _And | _Or | list | tuple | None) -> _DNF:
        """Combine with another _DNF object"""
        if not isinstance(other, _DNF):
            other = _DNF(other)
        assert isinstance(other, _DNF)
        if self._filters is None:
            result = other._filters
        elif other._filters is None:
            result = self._filters
        else:
            result = self._And([self._filters, other._filters])
        return _DNF(result)

    @classmethod
    def extract_pq_filters(cls, pq_expr: ReadParquet, predicate_expr: Expr) -> _DNF:
        _filters = None
        if isinstance(predicate_expr, (LE, GE, LT, GT, EQ, NE)):
            if (
                isinstance(predicate_expr.left, ReadParquet)
                and predicate_expr.left.path == pq_expr.path
                and not isinstance(predicate_expr.right, Expr)
            ):
                op = predicate_expr._operator_repr
                column = predicate_expr.left.columns[0]
                value = predicate_expr.right
                _filters = (column, op, value)
            elif (
                isinstance(predicate_expr.right, ReadParquet)
                and predicate_expr.right.path == pq_expr.path
                and not isinstance(predicate_expr.left, Expr)
            ):
                # Simple dict to make sure field comes first in filter
                flip = {LE: GE, LT: GT, GE: LE, GT: LT}
                op = predicate_expr
                op = flip.get(op, op)._operator_repr
                column = predicate_expr.right.columns[0]
                value = predicate_expr.left
                _filters = (column, op, value)

        elif isinstance(predicate_expr, (And, Or)):
            left = cls.extract_pq_filters(pq_expr, predicate_expr.left)._filters
            right = cls.extract_pq_filters(pq_expr, predicate_expr.right)._filters
            if left and right:
                if isinstance(predicate_expr, And):
                    _filters = cls._And([left, right])
                else:
                    _filters = cls._Or([left, right])

        return _DNF(_filters)


#
# Parquet-statistics handling
#


def _collect_pq_statistics(
    expr: ReadParquet, columns: list | None = None
) -> list[dict] | None:
    """Collect Parquet statistic for dataset paths"""

    # Be strict about columns argument
    if columns:
        if not isinstance(columns, list):
            raise ValueError(f"Expected columns to be a list, got {type(columns)}.")
        allowed = {expr._meta.index.name} | set(expr.columns)
        if not set(columns).issubset(allowed):
            raise ValueError(f"columns={columns} must be a subset of {allowed}")

    # Collect statistics using layer information
    fs = expr._plan["func"].fs
    parts = [
        part
        for i, part in enumerate(expr._plan["parts"])
        if not expr._filtered or i in expr._partitions
    ]

    # Execute with delayed for large and remote datasets
    parallel = int(False if _is_local_fs(fs) else 16)
    if parallel:
        # Group parts corresponding to the same file.
        # A single task should always parse statistics
        # for all these parts at once (since they will
        # all be in the same footer)
        groups = defaultdict(list)
        for part in parts:
            for p in [part] if isinstance(part, dict) else part:
                path = p.get("piece")[0]
                groups[path].append(p)
        group_keys = list(groups.keys())

        # Compute and return flattened result
        func = delayed(_read_partition_stats_group)
        result = dask.compute(
            [
                func(
                    list(
                        itertools.chain(
                            *[groups[k] for k in group_keys[i : i + parallel]]
                        )
                    ),
                    fs,
                    columns=columns,
                )
                for i in range(0, len(group_keys), parallel)
            ]
        )[0]
        return list(itertools.chain(*result))
    else:
        # Serial computation on client
        return _read_partition_stats_group(parts, fs, columns=columns)


def _read_partition_stats_group(parts, fs, columns=None):
    """Parse the statistics for a group of files"""

    def _read_partition_stats(part, fs, columns=None):
        # Helper function to read Parquet-metadata
        # statistics for a single partition

        if not isinstance(part, list):
            part = [part]

        column_stats = {}
        num_rows = 0
        columns = columns or []
        for p in part:
            piece = p["piece"]
            path = piece[0]
            row_groups = None if piece[1] == [None] else piece[1]
            with fs.open(path, default_cache="none") as f:
                md = pq.ParquetFile(f).metadata
            if row_groups is None:
                row_groups = list(range(md.num_row_groups))
            for rg in row_groups:
                row_group = md.row_group(rg)
                num_rows += row_group.num_rows
                for i in range(row_group.num_columns):
                    col = row_group.column(i)
                    name = col.path_in_schema
                    if name in columns:
                        if col.statistics and col.statistics.has_min_max:
                            if name in column_stats:
                                column_stats[name]["min"] = min(
                                    column_stats[name]["min"], col.statistics.min
                                )
                                column_stats[name]["max"] = max(
                                    column_stats[name]["max"], col.statistics.max
                                )
                            else:
                                column_stats[name] = {
                                    "min": col.statistics.min,
                                    "max": col.statistics.max,
                                }

        # Convert dict-of-dict to list-of-dict to be consistent
        # with current `dd.read_parquet` convention (for now)
        column_stats_list = [
            {
                "name": name,
                "min": column_stats[name]["min"],
                "max": column_stats[name]["max"],
            }
            for name in column_stats.keys()
        ]
        return {"num-rows": num_rows, "columns": column_stats_list}

    # Helper function used by _extract_statistics
    return [_read_partition_stats(part, fs, columns=columns) for part in parts]
