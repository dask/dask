from __future__ import annotations

from functools import cached_property, lru_cache, partial

from dask.base import tokenize
from dask.dataframe.io.parquet.core import (
    ParquetFunctionWrapper,
    get_engine,
    process_statistics,
    set_index_columns,
)
from dask.dataframe.io.parquet.utils import _split_user_options
from dask.utils import natural_sort_key
from matchpy import CustomConstraint, Pattern, ReplacementRule, Wildcard

from dask_match.expr import EQ, GE, GT, LE, LT, NE, Filter
from dask_match.io import BlockwiseIO, PartitionsFiltered

NONE_LABEL = "__null_dask_index__"


def _list_columns(columns):
    # Simple utility to convert columns to list
    if isinstance(columns, (str, int)):
        columns = [columns]
    elif isinstance(columns, tuple):
        columns = list(columns)
    return columns


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
    }

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

    @classmethod
    def _replacement_rules(cls):
        # All wildcards defined here.
        # Note that "x" corresponds to a column selection, and
        # "y" corresponds to a literal filter-comparison value
        _ = Wildcard.dot()
        path, columns, filters, x, y = map(
            Wildcard.dot, ["path", "columns", "filters", "x", "y"]
        )
        other = {w.variable_name: w for w in map(Wildcard.dot, cls._parameters[3:])}

        # Column projection
        def project_columns(path, columns, filters, x, **kwargs):
            new = ReadParquet(path, columns=_list_columns(x), filters=filters, **kwargs)
            return new[x] if isinstance(x, (str, int)) else new

        pattern = Pattern(
            ReadParquet(path, columns=columns, filters=filters, **other)[x],
            CustomConstraint(
                # Avoid infinite loop if x is str or int
                lambda columns, x: (not isinstance(x, (str, int)) or columns is None)
            ),
        )
        yield ReplacementRule(pattern, project_columns)

        # Simple dict to make sure field comes first in filter
        flip_op = {LE: GE, LT: GT, GE: LE, GT: LT}

        # Predicate pushdown to parquet
        for op in [LE, LT, GE, GT, EQ, NE]:

            def predicate_pushdown(path, columns, filters, x, y, op=None, **kwargs):
                return ReadParquet(
                    path,
                    columns=_list_columns(columns),
                    filters=(filters or []) + [(x, op._operator_repr, y)],
                    **kwargs,
                )

            pattern = Pattern(
                Filter(
                    ReadParquet(path, columns=columns, filters=filters, **other),
                    op(ReadParquet(path, columns=_, filters=_, **other)[x], y),
                )
            )
            replace = partial(predicate_pushdown, op=op)
            yield ReplacementRule(pattern, replace)

            pattern = Pattern(
                Filter(
                    ReadParquet(path, columns=columns, filters=filters, **other),
                    op(y, ReadParquet(path, columns=_, filters=_, **other)[x]),
                )
            )
            replace = partial(predicate_pushdown, op=flip_op.get(op, op))
            yield ReplacementRule(pattern, replace)

            pattern = Pattern(
                Filter(
                    ReadParquet(path, columns=columns, filters=filters, **other),
                    op(ReadParquet(path, columns=x, filters=_, **other), y),
                ),
                CustomConstraint(lambda x: isinstance(x, str)),
            )
            replace = partial(predicate_pushdown, op=op)
            yield ReplacementRule(pattern, replace)

            pattern = Pattern(
                Filter(
                    ReadParquet(path, columns=columns, filters=filters, **other),
                    op(y, ReadParquet(path, columns=x, filters=_, **other)),
                ),
                CustomConstraint(lambda x: isinstance(x, str)),
            )
            replace = partial(predicate_pushdown, op=flip_op.get(op, op))
            yield ReplacementRule(pattern, replace)

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
        return self._dataset_info["meta"]

    @cached_property
    def _plan(self):
        dataset_info = self._dataset_info
        parts, stats, common_kwargs = self.engine._construct_collection_plan(
            dataset_info
        )

        # Parse dataset statistics from metadata (if available)
        parts, divisions, _ = process_statistics(
            parts,
            stats,
            dataset_info["filters"],
            dataset_info["index"],
            (
                dataset_info["blocksize"]
                if dataset_info["split_row_groups"] is True
                else None
            ),
            dataset_info["split_row_groups"],
            dataset_info["fs"],
            dataset_info["aggregation_depth"],
        )

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
            "divisions": divisions,
        }

    def _divisions(self):
        return self._plan["divisions"]

    def _filtered_task(self, index: int):
        return (self._plan["func"], self._plan["parts"][index])
