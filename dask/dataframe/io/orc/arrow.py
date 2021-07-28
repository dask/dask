import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
from packaging.version import parse as parse_version

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
        split_stripes,
        aggregate_files,
        **kwargs,
    ):

        # Convert root directory to file list.
        # TODO: Handle hive-partitioned data
        hive_partitions = []
        unique_hive_partitions = {}
        if len(paths) == 1 and not fs.isfile(paths[0]):
            root_dir = paths[0]
            paths = collect_files(root_dir, fs)
            (
                paths,
                hive_partitions,
                unique_hive_partitions,
            ) = collect_partitions(paths, root_dir, fs)

        schema = None
        parts = []

        if split_stripes:
            offset = 0
            for i, path in enumerate(paths):
                hive_part = hive_partitions[i] if hive_partitions else []
                with fs.open(path, "rb") as f:
                    o = orc.ORCFile(f)
                    if schema is None:
                        schema = o.schema
                    elif schema != o.schema:
                        raise ValueError("Incompatible schemas while parsing ORC files")
                    _stripes = list(range(o.nstripes))
                    if offset:
                        parts.append([(path, _stripes[0:offset], hive_part)])
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
                        offset += int(split_stripes)
                    if aggregate_files and int(split_stripes) > 1:
                        offset -= o.nstripes
                    else:
                        offset = 0
        else:
            for i, path in enumerate(paths):
                hive_part = hive_partitions[i] if hive_partitions else []
                if schema is None:
                    with fs.open(paths[0], "rb") as f:
                        o = orc.ORCFile(f)
                        schema = o.schema
                parts.append([(path, None, hive_part)])

        schema = _get_pyarrow_dtypes(schema, categories=None)
        if columns is not None:
            ex = set(columns) - set(schema)
            if ex:
                raise ValueError(
                    "Requested columns (%s) not in schema (%s)" % (ex, set(schema))
                )

        # Check if we can aggregate adjacent parts together
        parts = cls._aggregate_files(aggregate_files, split_stripes, parts)

        columns = list(schema) if columns is None else columns
        index = [index] if isinstance(index, str) else index
        meta = _meta_from_dtypes(columns, schema, index, [])

        # Deal with hive-partitioned data
        for column, uniques in unique_hive_partitions.items():
            if column not in meta.columns:
                meta[column] = pd.Series(
                    pd.Categorical(categories=uniques, values=[]),
                    index=meta.index,
                )

        return parts, schema, meta, {"partition_uniques": unique_hive_partitions}

    @classmethod
    def _aggregate_files(cls, aggregate_files, split_stripes, parts):
        # TODO: Allow aggregate_files to specify a specific hive-partition column name
        if aggregate_files is True and int(split_stripes) > 1 and len(parts) > 1:
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
                    hive_parts == new_hive_parts
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
        cls, fs, parts, schema, columns, partition_uniques=None, **kwargs
    ):
        batches = []
        for path, stripes, hive_parts in parts:
            batches += _read_orc_stripes(fs, path, stripes, schema, columns)
        arrow_table = pa.Table.from_batches(batches)

        # Add partition columns
        partition_uniques = partition_uniques or {}
        for (part_name, cat) in hive_parts:
            if part_name not in arrow_table.schema.names:
                # We read from file paths, so the partition
                # columns are NOT in our table yet.
                categories = partition_uniques[part_name]
                cat_ind = np.full(len(arrow_table), categories.index(cat), dtype="i4")
                arr = pa.DictionaryArray.from_arrays(cat_ind, pa.array(categories))
                arrow_table = arrow_table.append_column(part_name, arr)

        if parse_version(pa.__version__) < parse_version("0.11.0"):
            return arrow_table.to_pandas()
        else:
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
