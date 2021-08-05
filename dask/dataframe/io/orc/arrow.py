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
    def read_metadata(
        cls,
        fs,
        paths,
        columns,
        index,
        filters,
        split_stripes,
        aggregate_files,
    ):

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
        # following `_aggregate_files` method is required
        # to coalesce multiple paths into a single
        # `read_partition` task
        parts, statistics, schema = cls._gather_parts(
            fs,
            paths,
            columns,
            filters,
            split_stripes,
            directory_partitions,
            directory_partition_keys,
            aggregate_files,
            directory_aggregation_depth,
        )

        # Apply filters if applicable
        if statistics and filters:
            parts, statistics = apply_filters(parts, statistics, filters)

        # Aggregate adjacent partitions together
        # when possible/desired
        parts = cls._aggregate_files(
            aggregate_files,
            directory_aggregation_depth,
            split_stripes,
            parts,
            statistics,
        )

        # Create metadata
        meta = cls._create_meta(columns, schema, index, directory_partition_keys)

        # Define common kwargs
        common_kwargs = {
            "schema": schema,
            "partition_uniques": directory_partition_keys,
            "filters": filters,
        }

        return parts, meta, common_kwargs

    @classmethod
    def _gather_parts(
        cls,
        fs,
        paths,
        columns,
        filters,
        split_stripes,
        directory_partitions,
        directory_partition_keys,
        aggregate_files,
        directory_aggregation_depth,
    ):
        def _new_stat(hive_part, stat_columns):
            return {
                "columns": [
                    {"name": key, "min": val, "max": val}
                    for (key, val) in hive_part
                    if key in stat_columns
                ]
            }

        # Check if filters contain
        need_stat_columns = [
            col for col in _flatten_filters(filters) if col in directory_partition_keys
        ]

        schema = None
        parts = []
        stats = []
        if split_stripes:
            offset = 0
            for i, path in enumerate(paths):
                hive_part = directory_partitions[i] if directory_partitions else []
                with fs.open(path, "rb") as f:
                    o = orc.ORCFile(f)
                    if schema is None:
                        schema = o.schema
                    elif schema != o.schema:
                        raise ValueError("Incompatible schemas while parsing ORC files")
                    _stripes = list(range(o.nstripes))
                    if offset:
                        parts.append([(path, _stripes[0:offset], hive_part)])
                        if need_stat_columns:
                            stats.append(_new_stat(hive_part, need_stat_columns))
                    while offset < o.nstripes:
                        parts.append(
                            [
                                (
                                    path,
                                    _stripes[offset : offset + int(split_stripes)],
                                    hive_part,
                                )
                            ]
                        )
                        if need_stat_columns:
                            stats.append(_new_stat(hive_part, need_stat_columns))
                        offset += int(split_stripes)
                    if (
                        aggregate_files
                        and int(split_stripes) > 1
                        and directory_aggregation_depth < 1
                    ):
                        offset -= o.nstripes
                    else:
                        offset = 0
        else:
            for i, path in enumerate(paths):
                hive_part = directory_partitions[i] if directory_partitions else []
                if schema is None:
                    with fs.open(paths[0], "rb") as f:
                        o = orc.ORCFile(f)
                        schema = o.schema
                parts.append([(path, None, hive_part)])
                if need_stat_columns:
                    stats.append(_new_stat(hive_part, need_stat_columns))

        schema = _get_pyarrow_dtypes(schema, categories=None)
        if columns is not None:
            ex = set(columns) - (set(schema) | set(directory_partition_keys))
            if ex:
                raise ValueError(
                    "Requested columns (%s) not in schema (%s)" % (ex, set(schema))
                )

        return parts, stats, schema

    @classmethod
    def _create_meta(cls, columns, schema, index, directory_partition_keys):
        columns = list(schema) if columns is None else columns
        index = [index] if isinstance(index, str) else index
        meta = _meta_from_dtypes(columns, schema, index, [])

        # Deal with hive-partitioned data
        for column, uniques in directory_partition_keys.items():
            if column not in meta.columns:
                meta[column] = pd.Series(
                    pd.Categorical(categories=uniques, values=[]),
                    index=meta.index,
                )

        return meta

    @classmethod
    def _aggregate_files(
        cls,
        aggregate_files,
        directory_aggregation_depth,
        split_stripes,
        parts,
        statistics,
    ):
        if aggregate_files and int(split_stripes) > 1 and len(parts) > 1:
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
        _stripes = range(o.nstripes) if stripes is None else stripes
        for stripe in _stripes:
            batches.append(o.read_stripe(stripe, columns))
    return batches
