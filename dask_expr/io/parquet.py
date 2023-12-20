from __future__ import annotations

import contextlib
import functools
import itertools
import operator
import warnings
from collections import defaultdict
from functools import cached_property

import dask
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import tlz as toolz
from dask.base import normalize_token, tokenize
from dask.dataframe.io.parquet.core import (
    ParquetFunctionWrapper,
    ToParquetFunctionWrapper,
    aggregate_row_groups,
    get_engine,
    set_index_columns,
    sorted_columns,
)
from dask.dataframe.io.parquet.utils import _split_user_options
from dask.dataframe.io.utils import _is_local_fs
from dask.delayed import delayed
from dask.utils import apply, natural_sort_key, typename
from fsspec.utils import stringify_path

from dask_expr._expr import (
    EQ,
    GE,
    GT,
    LE,
    LT,
    NE,
    And,
    Blockwise,
    Expr,
    Index,
    Lengths,
    Literal,
    Or,
    Projection,
    determine_column_projection,
)
from dask_expr._reductions import Len
from dask_expr._util import _convert_to_list
from dask_expr.io import BlockwiseIO, PartitionsFiltered

NONE_LABEL = "__null_dask_index__"

_cached_dataset_info = {}
_CACHED_DATASET_SIZE = 10
_CACHED_PLAN_SIZE = 10
_cached_plan = {}


def _control_cached_dataset_info(key):
    if (
        len(_cached_dataset_info) > _CACHED_DATASET_SIZE
        and key not in _cached_dataset_info
    ):
        key_to_pop = list(_cached_dataset_info.keys())[0]
        _cached_dataset_info.pop(key_to_pop)


def _control_cached_plan(key):
    if len(_cached_plan) > _CACHED_PLAN_SIZE and key not in _cached_plan:
        key_to_pop = list(_cached_plan.keys())[0]
        _cached_plan.pop(key_to_pop)


@normalize_token.register(pa_ds.Dataset)
def normalize_pa_ds(ds):
    return (ds.files, ds.schema)


@normalize_token.register(pa_ds.FileFormat)
def normalize_pa_file_format(file_format):
    return str(file_format)


@normalize_token.register(pa.Schema)
def normalize_pa_schema(schema):
    return schema.to_string()


class ToParquet(Expr):
    _parameters = [
        "frame",
        "path",
        "fs",
        "fmd",
        "engine",
        "offset",
        "partition_on",
        "write_metadata_file",
        "name_function",
        "write_kwargs",
    ]

    @property
    def _meta(self):
        return None

    def _divisions(self):
        return (None, None)

    def _lower(self):
        return ToParquetBarrier(
            ToParquetData(
                *self.operands,
            ),
            *self.operands[1:],
        )


class ToParquetData(Blockwise):
    _parameters = ToParquet._parameters

    @cached_property
    def io_func(self):
        return ToParquetFunctionWrapper(
            self.engine,
            self.path,
            self.fs,
            self.partition_on,
            self.write_metadata_file,
            self.offset,
            self.name_function,
            self.write_kwargs,
        )

    def _divisions(self):
        return (None,) * (self.frame.npartitions + 1)

    def _task(self, index: int):
        return (self.io_func, (self.frame._name, index), (index,))


class ToParquetBarrier(Expr):
    _parameters = ToParquet._parameters

    @property
    def _meta(self):
        return None

    def _divisions(self):
        return (None, None)

    def _layer(self):
        if self.write_metadata_file:
            append = self.write_kwargs.get("append")
            compression = self.write_kwargs.get("compression")
            return {
                (self._name, 0): (
                    apply,
                    self.engine.write_metadata,
                    [
                        self.frame.__dask_keys__(),
                        self.fmd,
                        self.fs,
                        self.path,
                    ],
                    {"append": append, "compression": compression},
                )
            }
        else:
            return {(self._name, 0): (lambda x: None, self.frame.__dask_keys__())}


def to_parquet(
    df,
    path,
    compression="snappy",
    write_index=True,
    append=False,
    overwrite=False,
    ignore_divisions=False,
    partition_on=None,
    storage_options=None,
    custom_metadata=None,
    write_metadata_file=None,
    compute=True,
    compute_kwargs=None,
    schema="infer",
    name_function=None,
    filesystem=None,
    engine=None,
    **kwargs,
):
    from dask_expr._collection import new_collection
    from dask_expr.io.parquet import NONE_LABEL, ToParquet

    engine = _set_parquet_engine(engine=engine, meta=df._meta)
    compute_kwargs = compute_kwargs or {}

    partition_on = partition_on or []
    if isinstance(partition_on, str):
        partition_on = [partition_on]

    if set(partition_on) - set(df.columns):
        raise ValueError(
            "Partitioning on non-existent column. "
            "partition_on=%s ."
            "columns=%s" % (str(partition_on), str(list(df.columns)))
        )

    if df.columns.inferred_type not in {"string", "empty"}:
        raise ValueError("parquet doesn't support non-string column names")

    if isinstance(engine, str):
        engine = get_engine(engine)

    if hasattr(path, "name"):
        path = stringify_path(path)

    fs, _paths, _, _ = engine.extract_filesystem(
        path,
        filesystem=filesystem,
        dataset_options={},
        open_file_options={},
        storage_options=storage_options,
    )
    assert len(_paths) == 1, "only one path"
    path = _paths[0]

    if overwrite:
        if append:
            raise ValueError("Cannot use both `overwrite=True` and `append=True`!")

        if fs.exists(path) and fs.isdir(path):
            # Check for any previous parquet ops reading from a file in the
            # output directory, since deleting those files now would result in
            # errors or incorrect results.
            for read_op in df.expr.find_operations(ReadParquet):
                read_path_with_slash = str(read_op.path).rstrip("/") + "/"
                write_path_with_slash = path.rstrip("/") + "/"
                if read_path_with_slash.startswith(write_path_with_slash):
                    raise ValueError(
                        "Cannot overwrite a path that you are reading "
                        "from in the same task graph."
                    )

            # Don't remove the directory if it's the current working directory
            if _is_local_fs(fs):
                working_dir = fs.expand_path(".")[0]
                if path.rstrip("/") == working_dir.rstrip("/"):
                    raise ValueError(
                        "Cannot clear the contents of the current working directory!"
                    )

            # It's safe to clear the output directory
            fs.rm(path, recursive=True)

        # Clear read_parquet caches in case we are
        # also reading from the overwritten path
        _cached_dataset_info.clear()
        _cached_plan.clear()

    # Always skip divisions checks if divisions are unknown
    if not df.known_divisions:
        ignore_divisions = True

    # Save divisions and corresponding index name. This is necessary,
    # because we may be resetting the index to write the file
    division_info = {"divisions": df.divisions, "name": df.index.name}
    if division_info["name"] is None:
        # As of 0.24.2, pandas will rename an index with name=None
        # when df.reset_index() is called.  The default name is "index",
        # but dask will always change the name to the NONE_LABEL constant
        if NONE_LABEL not in df.columns:
            division_info["name"] = NONE_LABEL
        elif write_index:
            raise ValueError(
                "Index must have a name if __null_dask_index__ is a column."
            )
        else:
            warnings.warn(
                "If read back by Dask, column named __null_dask_index__ "
                "will be set to the index (and renamed to None)."
            )

    # There are some "reserved" names that may be used as the default column
    # name after resetting the index. However, we don't want to treat it as
    # a "special" name if the string is already used as a "real" column name.
    reserved_names = []
    for name in ["index", "level_0"]:
        if name not in df.columns:
            reserved_names.append(name)

    # If write_index==True (default), reset the index and record the
    # name of the original index in `index_cols` (we will set the name
    # to the NONE_LABEL constant if it is originally `None`).
    # `fastparquet` will use `index_cols` to specify the index column(s)
    # in the metadata.  `pyarrow` will revert the `reset_index` call
    # below if `index_cols` is populated (because pyarrow will want to handle
    # index preservation itself).  For both engines, the column index
    # will be written to "pandas metadata" if write_index=True
    index_cols = []
    if write_index:
        real_cols = set(df.columns)
        none_index = list(df._meta.index.names) == [None]
        df = df.reset_index()
        if none_index:
            rename_columns = {c: NONE_LABEL for c in df.columns if c in reserved_names}
            df = df.rename(rename_columns)
        index_cols = [c for c in set(df.columns) - real_cols]
    else:
        # Not writing index - might as well drop it
        df = df.reset_index(drop=True)

    if custom_metadata and b"pandas" in custom_metadata.keys():
        raise ValueError(
            "User-defined key/value metadata (custom_metadata) can not "
            "contain a b'pandas' key.  This key is reserved by Pandas, "
            "and overwriting the corresponding value can render the "
            "entire dataset unreadable."
        )

    # Engine-specific initialization steps to write the dataset.
    # Possibly create parquet metadata, and load existing stuff if appending
    i_offset, fmd, metadata_file_exists, extra_write_kwargs = engine.initialize_write(
        df.to_dask_dataframe(),
        fs,
        path,
        append=append,
        ignore_divisions=ignore_divisions,
        partition_on=partition_on,
        division_info=division_info,
        index_cols=index_cols,
        schema=schema,
        custom_metadata=custom_metadata,
        **kwargs,
    )

    # By default we only write a metadata file when appending if one already
    # exists
    if append and write_metadata_file is None:
        write_metadata_file = metadata_file_exists

    # Check that custom name_function is valid,
    # and that it will produce unique names
    if name_function is not None:
        if not callable(name_function):
            raise ValueError("``name_function`` must be a callable with one argument.")
        filenames = [name_function(i + i_offset) for i in range(df.npartitions)]
        if len(set(filenames)) < len(filenames):
            raise ValueError("``name_function`` must produce unique filenames.")

    # If we are using a remote filesystem and retries is not set, bump it
    # to be more fault tolerant, as transient transport errors can occur.
    # The specific number 5 isn't hugely motivated: it's less than ten and more
    # than two.
    annotations = dask.config.get("annotations", {})
    if "retries" not in annotations and not _is_local_fs(fs):
        ctx = dask.annotate(retries=5)
    else:
        ctx = contextlib.nullcontext()

    with ctx:
        out = new_collection(
            ToParquet(
                df,
                path,
                fs,
                fmd,
                engine,
                i_offset,
                partition_on,
                write_metadata_file,
                name_function,
                toolz.merge(
                    kwargs,
                    {"compression": compression, "custom_metadata": custom_metadata},
                    extra_write_kwargs,
                ),
            )
        )

    if compute:
        out = out.compute(**compute_kwargs)

    # Invalidate the filesystem listing cache for the output path after write.
    # We do this before returning, even if `compute=False`. This helps ensure
    # that reading files that were just written succeeds.
    fs.invalidate_cache(path)

    return out


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
        "engine",
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
        "engine": "pyarrow",
        "kwargs": None,
        "_partitions": None,
        "_series": False,
    }
    _pq_length_stats = None
    _absorb_projections = True

    @property
    def engine(self):
        _engine = self.operand("engine")
        if isinstance(_engine, str):
            return get_engine(_engine)
        return _engine

    @property
    def columns(self):
        columns_operand = self.operand("columns")
        if columns_operand is None:
            return list(self._meta.columns)
        else:
            return _convert_to_list(columns_operand)

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Index):
            # Column projection
            columns = determine_column_projection(self, parent, dependents)
            if set(columns) == set(self.columns):
                return
            columns = [col for col in self.columns if col in columns]
            return self.substitute_parameters({"columns": columns, "_series": False})

        if isinstance(parent, Projection):
            return super()._simplify_up(parent, dependents)

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

        # Collect general dataset info
        args = (
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
        dataset_token = tokenize(*args)
        if dataset_token not in _cached_dataset_info:
            _control_cached_dataset_info(dataset_token)
            _cached_dataset_info[dataset_token] = self.engine._collect_dataset_info(
                *args
            )
        dataset_info = _cached_dataset_info[dataset_token].copy()

        # Infer meta, accounting for index and columns arguments.
        meta = self.engine._create_dd_meta(dataset_info)
        index = dataset_info["index"]
        index = [index] if isinstance(index, str) else index
        meta, index, all_columns = set_index_columns(
            meta, index, None, auto_index_allowed
        )
        if meta.index.name == NONE_LABEL:
            meta.index.name = None
        dataset_info["base_meta"] = meta
        dataset_info["index"] = index
        dataset_info["all_columns"] = all_columns

        return dataset_info

    @property
    def _meta(self):
        meta = self._dataset_info["base_meta"]
        columns = _convert_to_list(self.operand("columns"))
        if self._series:
            assert len(columns) > 0
            return meta[columns[0]]
        elif columns is not None:
            return meta[columns]
        return meta

    @cached_property
    def _io_func(self):
        if self._plan["empty"]:
            return lambda x: x
        dataset_info = self._dataset_info
        return ParquetFunctionWrapper(
            self.engine,
            dataset_info["fs"],
            dataset_info["base_meta"],
            self.columns,
            dataset_info["index"],
            dataset_info["kwargs"]["dtype_backend"],
            {},  # All kwargs should now be in `common_kwargs`
            self._plan["common_kwargs"],
        )

    @cached_property
    def _plan(self):
        dataset_info = self._dataset_info
        dataset_token = tokenize(dataset_info)
        if dataset_token not in _cached_plan:
            parts, stats, common_kwargs = self.engine._construct_collection_plan(
                dataset_info
            )

            # Make sure parts and stats are aligned
            parts, stats = _align_statistics(parts, stats)

            # Use statistics to aggregate partitions
            parts, stats = _aggregate_row_groups(parts, stats, dataset_info)

            # Use statistics to calculate divisions
            divisions = _calculate_divisions(stats, dataset_info, len(parts))

            empty = False
            if len(divisions) < 2:
                # empty dataframe - just use meta
                divisions = (None, None)
                parts = [self._meta]
                empty = True

            _control_cached_plan(dataset_token)
            _cached_plan[dataset_token] = {
                "empty": empty,
                "parts": parts,
                "statistics": stats,
                "divisions": divisions,
                "common_kwargs": common_kwargs,
            }
        return _cached_plan[dataset_token]

    def _divisions(self):
        return self._plan["divisions"]

    def _filtered_task(self, index: int):
        tsk = (self._io_func, self._plan["parts"][index])
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

    @functools.cached_property
    def _fusion_compression_factor(self):
        if self.operand("columns") is None:
            return 1
        nr_original_columns = len(self._dataset_info["schema"].names) - 1
        return len(_convert_to_list(self.operand("columns"))) / nr_original_columns


#
# Helper functions
#


def _set_parquet_engine(engine=None, meta=None):
    # Use `engine` or `meta` input to set the parquet engine
    if engine == "fastparquet":
        raise NotImplementedError("Fastparquet engine is not supported")

    if engine is None:
        if (
            meta is not None and typename(meta).split(".")[0] == "cudf"
        ) or dask.config.get("dataframe.backend", "pandas") == "cudf":
            from dask_cudf.io.parquet import CudfEngine

            engine = CudfEngine
        else:
            engine = "pyarrow"
    return engine


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
    fs = expr._io_func.fs
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
