import warnings
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc

from ..parquet.core import apply_filters
from ..parquet.utils import _flatten_filters
from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes
from .utils import ORCEngine, collect_files, collect_partitions


class ArrowORCEngine(ORCEngine):
    @classmethod
    def get_dataset_info(
        cls,
        fs,
        paths,
        columns=None,
        index=None,
        filters=None,
        gather_statistics=True,
        dataset_kwargs=None,
    ):
        if dataset_kwargs:
            warnings.warn(
                "Pyarrow ORC engine does not currently support "
                "any 'dataset_kwargs' options."
            )

        # Generate a full list of files and
        # directory partitions
        directory_partitions = []
        directory_partition_keys = {}
        if len(paths) == 1 and not fs.isfile(paths[0]):
            root_dir = paths[0]
            paths = collect_files(root_dir, fs)
            (
                paths,
                directory_partitions,
                directory_partition_keys,
            ) = collect_partitions(paths, root_dir, fs)

        with fs.open(paths[0], "rb") as f:
            o = orc.ORCFile(f)
            schema = o.schema

        # Save list of directory-partition columns and
        # file columns that we will need statistics for
        dir_columns_need_stats = {
            col for col in _flatten_filters(filters) if col in directory_partition_keys
        } | ({index} if index in directory_partition_keys else set())
        file_columns_need_stats = {
            col for col in _flatten_filters(filters) if col in schema.names
        }
        # Before including the index column, raise an error
        # if the user is trying to filter with gather_statistics=False
        if file_columns_need_stats and gather_statistics is False:
            raise ValueError(
                "Cannot filter ORC stripes when `gather_statistics=False`."
            )
        file_columns_need_stats |= {index} if index in schema.names else set()

        # Convert schema and check columns
        pa_schema = _get_pyarrow_dtypes(schema, categories=None)
        if columns is not None:
            ex = set(columns) - (set(pa_schema) | set(directory_partition_keys))
            if ex:
                raise ValueError(
                    "Requested columns (%s) not in schema (%s)" % (ex, set(schema))
                )

        # Return the final `dataset_info` dictionary
        return {
            "fs": fs,
            "paths": paths,
            "schema": schema,
            "pa_schema": pa_schema,
            "dir_columns_need_stats": dir_columns_need_stats,
            "file_columns_need_stats": file_columns_need_stats,
            "directory_partitions": directory_partitions,
            "directory_partition_keys": directory_partition_keys,
        }

    @classmethod
    def construct_output_meta(
        cls,
        dataset_info,
        index=None,
        columns=None,
        read_kwargs=None,
    ):
        # Use dataset_info to define `columns`
        schema = dataset_info["pa_schema"]
        directory_partition_keys = dataset_info["directory_partition_keys"]
        columns = list(schema) if columns is None else columns

        # Construct initial meta
        index = [index] if isinstance(index, str) else index
        meta = _meta_from_dtypes(columns, schema, index, [])

        # Deal with hive-partitioned data
        for column, uniques in (directory_partition_keys or {}).items():
            if column not in meta.columns:
                meta[column] = pd.Series(
                    pd.Categorical(categories=uniques, values=[]),
                    index=meta.index,
                )

        return meta

    @classmethod
    def construct_partition_plan(
        cls,
        meta,
        dataset_info,
        filters=None,
        split_stripes=True,
        aggregate_files=False,
        gather_statistics=True,
    ):

        # Extract column and index from meta
        columns = list(meta.columns)
        index = meta.index.name

        # Extract necessary dataset_info values
        directory_partition_keys = dataset_info["directory_partition_keys"]

        # Set the file-aggregation depth if the data has
        # directory partitions, and one of these partition
        # columns was specified by `aggregate_files`
        directory_aggregation_depth = 0
        if isinstance(aggregate_files, str):
            try:
                directory_aggregation_depth = (
                    list(directory_partition_keys).index(aggregate_files) + 1
                )
            except ValueError:
                raise ValueError(
                    f"{aggregate_files} is not a recognized partition column. "
                    f"Please check the aggregate_files argument."
                )

        # Gather a list of partitions and corresponding
        # statistics.  Each element in this initial partition
        # list will only correspond to a single path. The
        # following `aggregate_files` method is required
        # to coalesce multiple paths into a single
        # `read_partition` task. Note that `gather_parts`
        # will use `cls._filter_stripes` to apply filters
        # on each path independently.
        parts, statistics = cls.gather_parts(
            dataset_info,
            index=index,
            columns=columns,
            filters=filters,
            split_stripes=split_stripes,
            aggregate_files=aggregate_files,
            gather_statistics=gather_statistics,
            directory_aggregation_depth=directory_aggregation_depth,
        )

        divisions = None
        if index and statistics:
            divisions = cls._calculate_divisions(index, statistics)

        # Aggregate adjacent partitions together
        # when possible/desired
        if aggregate_files:
            parts = cls._aggregate_files(
                parts,
                directory_aggregation_depth=directory_aggregation_depth,
                split_stripes=split_stripes,
                statistics=statistics,
                divisions=divisions,
            )

        # Define common kwargs
        common_kwargs = {
            "schema": dataset_info["pa_schema"],
            "partition_uniques": dataset_info["directory_partition_keys"],
            "filters": filters,
        }

        return parts, divisions, common_kwargs

    @classmethod
    def gather_parts(
        cls,
        dataset_info,
        index=None,
        columns=None,
        filters=None,
        split_stripes=True,
        aggregate_files=False,
        gather_statistics=True,
        directory_aggregation_depth=0,
    ):

        # Extract necessary info from dataset_info
        fs = dataset_info["fs"]
        paths = dataset_info["paths"]
        schema = dataset_info["schema"]
        directory_partitions = dataset_info["directory_partitions"]
        dir_columns_need_stats = dataset_info["dir_columns_need_stats"]
        file_columns_need_stats = dataset_info["file_columns_need_stats"]

        # Main loop(s) to gather stripes/statistics for
        # each file. After this, each element of `parts` will
        # correspond to a group of stripes for a single file/path.
        parts = []
        statistics = []
        if split_stripes:
            offset = 0
            for i, path in enumerate(paths):
                hive_part = directory_partitions[i] if directory_partitions else []
                hive_part_need_stats = [
                    (k, v) for k, v in hive_part if k in dir_columns_need_stats
                ]
                with fs.open(path, "rb") as f:
                    o = orc.ORCFile(f)
                    nstripes = o.nstripes
                    if schema != o.schema:
                        raise ValueError("Incompatible schemas while parsing ORC files")
                    stripes, stats = cls.filter_file_stripes(
                        fs=fs,
                        orc_file=o,
                        filters=filters,
                        stat_columns=file_columns_need_stats,
                        stat_hive_part=hive_part_need_stats,
                        file_handle=f,
                        file_path=path,
                        gather_statistics=gather_statistics,
                    )
                    if offset:
                        new_part_stripes = stripes[0:offset]
                        if new_part_stripes:
                            parts.append([(path, new_part_stripes, hive_part)])
                            if gather_statistics:
                                statistics.append(cls._aggregate_stats(stats[0:offset]))
                    while offset < nstripes:
                        new_part_stripes = stripes[offset : offset + int(split_stripes)]
                        if new_part_stripes:
                            parts.append([(path, new_part_stripes, hive_part)])
                            if gather_statistics:
                                statistics.append(
                                    cls._aggregate_stats(
                                        stats[offset : offset + int(split_stripes)]
                                    )
                                )
                        offset += int(split_stripes)
                    if (
                        aggregate_files
                        and int(split_stripes) > 1
                        and directory_aggregation_depth < 1
                    ):
                        offset -= nstripes
                    else:
                        offset = 0
        else:
            for i, path in enumerate(paths):
                hive_part = directory_partitions[i] if directory_partitions else []
                hive_part_need_stats = [
                    (k, v) for k, v in hive_part if k in dir_columns_need_stats
                ]
                stripes, stats = cls.filter_file_stripes(
                    fs=fs,
                    orc_file=None,
                    filters=filters,
                    stat_columns=file_columns_need_stats,
                    stat_hive_part=hive_part_need_stats,
                    file_path=path,
                    file_handle=None,
                    gather_statistics=gather_statistics,
                )
                parts.append([(path, stripes, hive_part)])
                if gather_statistics:
                    statistics.append(cls._aggregate_stats(stats))

        return parts, statistics

    @classmethod
    def filter_file_stripes(
        cls,
        fs=None,  # Not used (see note)
        orc_file=None,
        filters=None,
        stat_columns=None,  # Not used (see note)
        stat_hive_part=None,
        file_path=None,
        file_handle=None,  # Not used (see note)
        gather_statistics=True,  # Not used (see note)
    ):
        """Filter stripes in a single file and gather statistics"""

        # NOTE: ArrowORCEngine only supports filtering on
        # directory partition columns. However, derived
        # classes may want to implement custom filtering.
        # ArrowORCEngine will pass in `stat_columns`,
        # `path_or_buffer` and `gather_statistics` kwargs
        # for this purpose.

        statistics = []
        stripes = [None] if orc_file is None else list(range(orc_file.nstripes))
        if stat_hive_part:
            for stripe in stripes:
                statistics.append(
                    {
                        "num-rows": None,  # Not available with PyArrow
                        "file-path": file_path,
                        "columns": [
                            {"name": key, "min": val, "max": val}
                            for (key, val) in stat_hive_part
                            if key in stat_columns
                        ],
                    }
                )
            stripes, statistics = apply_filters(stripes, statistics, filters)
        return stripes, statistics

    @classmethod
    def _calculate_divisions(cls, index, statistics):
        if statistics:
            divisions = []
            for icol, column_stats in enumerate(statistics[0].get("columns", [])):
                if column_stats.get("name", None) == index:
                    divisions = [
                        column_stats.get("min", None),
                        column_stats.get("max", None),
                    ]
                    break
            if divisions and None not in divisions:
                for stat in statistics[1:]:
                    next_division = stat["columns"][icol].get("max", None)
                    if next_division is None or next_division < divisions[-1]:
                        return None
                    divisions.append(next_division)
            return divisions
        return None

    @classmethod
    def _aggregate_stats(cls, statistics):
        """Aggregate a list of statistics"""
        if statistics:

            # Check if we are already "aggregated"
            nstats = len(statistics)
            if nstats == 1:
                return statistics

            # Populate statistic lists
            counts = []
            column_counts = defaultdict(list)
            column_mins = defaultdict(list)
            column_maxs = defaultdict(list)
            use_count = statistics[0].get("num-rows", None) is not None
            for stat in statistics:
                if use_count:
                    counts.append(stat.get("num-rows"))
                for col_stats in stat["columns"]:
                    name = col_stats["name"]
                    if use_count:
                        column_counts[name].append(col_stats.get("count"))
                    column_mins[name].append(col_stats.get("min", None))
                    column_maxs[name].append(col_stats.get("max", None))

            # Perform aggregation
            output = {}
            output["file-path"] = statistics[0].get("file-path", None)
            if use_count:
                output["row-count"] = sum(counts)
            column_stats = []
            for k in column_counts.keys():
                column_stat = {"name": k}
                if use_count:
                    column_stat["count"] = sum(column_counts[k])
                try:
                    column_stat["min"] = min(column_mins[k])
                    column_stat["max"] = max(column_maxs[k])
                except TypeError:
                    column_stat["min"] = None
                    column_stat["max"] = None
                column_stats.append(column_stat)
            output["columns"] = column_stats
            return output
        else:
            return {}

    @classmethod
    def _aggregate_files(
        cls,
        parts,
        directory_aggregation_depth=0,
        split_stripes=1,
        statistics=None,  # Not used (yet)
        divisions=None,
    ):
        if int(split_stripes) > 1 and len(parts) > 1:
            new_parts = []
            new_part = parts[0]
            nstripes = len(new_part[0][1])
            hive_parts = new_part[0][2]
            for part in parts[1:]:
                next_nstripes = len(part[0][1])
                new_hive_parts = part[0][2]
                # For partitioned data, we do not allow file aggregation between
                # different hive partitions
                if (next_nstripes + nstripes <= split_stripes) and (
                    hive_parts[:directory_aggregation_depth]
                    == new_hive_parts[:directory_aggregation_depth]
                ):
                    new_part.append(part[0])
                    nstripes += next_nstripes
                else:
                    new_parts.append(new_part)
                    new_part = part
                    nstripes = next_nstripes
                    hive_parts = new_hive_parts
            new_parts.append(new_part)
            return new_parts
        else:
            return parts

    @classmethod
    def read_partition(
        cls,
        fs,
        parts,
        columns,
        filters=None,
        schema=None,
        partition_uniques=None,
    ):
        # Create a seperate table for each directory partition.
        # We are only creating a single pyarrow table if there
        # are no partition columns.
        tables = []
        partitions = []
        partition_uniques = partition_uniques or {}
        if columns:
            # Seperate file columns and partition columns
            file_columns = [c for c in columns if c in set(schema)]
            partition_columns = [c for c in columns if c not in set(schema)]
        else:
            file_columns, partition_columns = None, list(partition_uniques)
        path, stripes, hive_parts = parts[0]
        batches = _read_orc_stripes(fs, path, stripes, schema, file_columns)
        for path, stripes, next_hive_parts in parts[1:]:
            if hive_parts == next_hive_parts:
                batches += _read_orc_stripes(fs, path, stripes, schema, file_columns)
            else:
                tables.append(pa.Table.from_batches(batches))
                partitions.append(hive_parts)
                batches = _read_orc_stripes(fs, path, stripes, schema, file_columns)
                hive_parts = next_hive_parts
        tables.append(pa.Table.from_batches(batches))
        partitions.append(hive_parts)

        # Add partition columns to each pyarrow table
        for i, hive_parts in enumerate(partitions):
            for (part_name, cat) in hive_parts:
                if part_name in partition_columns:
                    # We read from file paths, so the partition
                    # columns are NOT in our table yet.
                    categories = partition_uniques[part_name]
                    cat_ind = np.full(len(tables[i]), categories.index(cat), dtype="i4")
                    arr = pa.DictionaryArray.from_arrays(cat_ind, pa.array(categories))
                    tables[i] = tables[i].append_column(part_name, arr)

        # Concatenate arrow tables and convert to pandas
        arrow_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        return arrow_table.to_pandas(date_as_object=False)

    @classmethod
    def write_partition(cls, df, path, fs, filename, partition_on, **kwargs):

        table = pa.Table.from_pandas(df)
        if partition_on:
            _write_partitioned(
                table,
                path,
                fs,
                filename,
                partition_on,
                **kwargs,
            )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as f:
                orc.write_table(table, f, **kwargs)


def _write_partitioned(table, root_path, fs, filename, partition_cols, **kwargs):
    """Write table to a partitioned dataset with pyarrow"""
    fs.mkdirs(root_path, exist_ok=True)

    df = table.to_pandas(ignore_metadata=True)

    partition_keys = [df[col] for col in partition_cols]
    data_df = df.drop(partition_cols, axis="columns")
    data_cols = df.columns.drop(partition_cols)
    if len(data_cols) == 0:
        raise ValueError("No data left to save outside partition columns")

    subschema = table.schema
    for col in table.schema.names:
        if col in partition_cols:
            subschema = subschema.remove(subschema.get_field_index(col))

    for keys, subgroup in data_df.groupby(partition_keys):
        if not isinstance(keys, tuple):
            keys = (keys,)
        subdir = fs.sep.join(
            [
                "{colname}={value}".format(colname=name, value=val)
                for name, val in zip(partition_cols, keys)
            ]
        )
        subtable = pa.Table.from_pandas(
            subgroup,
            nthreads=1,
            preserve_index=False,
            schema=subschema,
            safe=False,
        )
        prefix = fs.sep.join([root_path, subdir])
        fs.mkdirs(prefix, exist_ok=True)
        full_path = fs.sep.join([prefix, filename])
        with fs.open(full_path, "wb") as f:
            orc.write_table(subtable, f, **kwargs)


def _read_orc_stripes(fs, path, stripes, schema, columns):
    # Construct a list of RecordBatch objects.
    # Each ORC stripe will corresonpond to a single RecordBatch.
    if columns is None:
        columns = list(schema)

    batches = []
    with fs.open(path, "rb") as f:
        o = orc.ORCFile(f)
        _stripes = range(o.nstripes) if stripes in (None, [None]) else stripes
        for stripe in _stripes:
            batches.append(o.read_stripe(stripe, columns))
    return batches
