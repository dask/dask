import json
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from packaging.version import parse as parse_version

from dask.base import tokenize
from dask.core import flatten
from dask.dataframe.io.parquet.utils import (
    Engine,
    _get_aggregation_depth,
    _normalize_index_columns,
    _parse_pandas_metadata,
    _process_open_file_options,
    _row_groups_to_parts,
    _set_gather_statistics,
    _set_metadata_task_size,
    _sort_and_analyze_paths,
    _split_user_options,
)
from dask.dataframe.io.utils import (
    _get_pyarrow_dtypes,
    _is_local_fs,
    _meta_from_dtypes,
    _open_input_files,
)
from dask.dataframe.utils import clear_known_categories
from dask.delayed import Delayed, delayed
from dask.utils import getargspec, natural_sort_key

# Check PyArrow version for feature support
_pa_version = parse_version(pa.__version__)
from pyarrow import dataset as pa_ds

subset_stats_supported = _pa_version > parse_version("2.0.0")
pre_buffer_supported = _pa_version >= parse_version("5.0.0")
del _pa_version

#
#  Helper Utilities
#


def _append_row_groups(metadata, md):
    """Append row-group metadata and include a helpful
    error message if an inconsistent schema is detected.
    """
    try:
        metadata.append_row_groups(md)
    except RuntimeError as err:
        if "requires equal schemas" in str(err):
            raise RuntimeError(
                "Schemas are inconsistent, try using "
                '`to_parquet(..., schema="infer")`, or pass an explicit '
                "pyarrow schema. Such as "
                '`to_parquet(..., schema={"column1": pa.string()})`'
            ) from err
        else:
            raise err


def _write_partitioned(
    table,
    df,
    root_path,
    filename,
    partition_cols,
    fs,
    pandas_to_arrow_table,
    preserve_index,
    index_cols=(),
    return_metadata=True,
    **kwargs,
):
    """Write table to a partitioned dataset with pyarrow.

    Logic copied from pyarrow.parquet.
    (arrow/python/pyarrow/parquet.py::write_to_dataset)

    TODO: Remove this in favor of pyarrow's `write_to_dataset`
          once ARROW-8244 is addressed.
    """
    fs.mkdirs(root_path, exist_ok=True)

    if preserve_index:
        df.reset_index(inplace=True)
    df = df[table.schema.names]

    index_cols = list(index_cols) if index_cols else []
    preserve_index = False
    if index_cols:
        df.set_index(index_cols, inplace=True)
        preserve_index = True

    partition_keys = [df[col] for col in partition_cols]
    data_df = df.drop(partition_cols, axis="columns")
    data_cols = df.columns.drop(partition_cols)
    if len(data_cols) == 0 and not index_cols:
        raise ValueError("No data left to save outside partition columns")

    subschema = table.schema
    for col in table.schema.names:
        if col in partition_cols:
            subschema = subschema.remove(subschema.get_field_index(col))

    md_list = []
    for keys, subgroup in data_df.groupby(partition_keys):
        if not isinstance(keys, tuple):
            keys = (keys,)
        subdir = fs.sep.join(
            [f"{name}={val}" for name, val in zip(partition_cols, keys)]
        )
        subtable = pandas_to_arrow_table(
            subgroup, preserve_index=preserve_index, schema=subschema
        )
        prefix = fs.sep.join([root_path, subdir])
        fs.mkdirs(prefix, exist_ok=True)
        full_path = fs.sep.join([prefix, filename])
        with fs.open(full_path, "wb") as f:
            pq.write_table(
                subtable,
                f,
                metadata_collector=md_list if return_metadata else None,
                **kwargs,
            )
        if return_metadata:
            md_list[-1].set_file_path(fs.sep.join([subdir, filename]))

    return md_list


def _index_in_schema(index, schema):
    """Simple utility to check if all `index` columns are included
    in the known `schema`.
    """
    if index and schema is not None:
        # Make sure all index columns are in user-defined schema
        return len(set(index).intersection(schema.names)) == len(index)
    elif index:
        return True  # Schema is not user-specified, all good
    else:
        return False  # No index to check


class PartitionObj:
    """Simple class providing a `name` and `keys` attribute
    for a single partition column.

    This class was originally designed as a mechanism to build a
    duck-typed version of pyarrow's deprecated `ParquetPartitions`
    class. Now that `ArrowLegacyEngine` is deprecated, this class
    can be modified/removed, but it is still used as a convenience.
    """

    def __init__(self, name, keys):
        self.name = name
        self.keys = sorted(keys)


def _frag_subset(old_frag, row_groups):
    """Create new fragment with row-group subset."""
    return old_frag.format.make_fragment(
        old_frag.path,
        old_frag.filesystem,
        old_frag.partition_expression,
        row_groups=row_groups,
    )


def _get_pandas_metadata(schema):
    """Get pandas-specific metadata from schema."""

    has_pandas_metadata = schema.metadata is not None and b"pandas" in schema.metadata
    if has_pandas_metadata:
        return json.loads(schema.metadata[b"pandas"].decode("utf8"))
    else:
        return {}


def _read_table_from_path(
    path,
    fs,
    row_groups,
    columns,
    schema,
    filters,
    **kwargs,
):
    """Read arrow table from file path.

    Used by `ArrowDatasetEngine._read_table` when no filters
    are specified (otherwise fragments are converted directly
    into tables).
    """

    # Define file-opening options
    read_kwargs = kwargs.get("read", {}).copy()
    precache_options, open_file_options = _process_open_file_options(
        read_kwargs.pop("open_file_options", {}),
        **(
            {
                "allow_precache": False,
                "default_cache": "none",
            }
            if _is_local_fs(fs)
            else {
                "columns": columns,
                "row_groups": row_groups if row_groups == [None] else [row_groups],
                "default_engine": "pyarrow",
                "default_cache": "none",
            }
        ),
    )

    # Use `pre_buffer=True` if the option is supported and an optimized
    # "pre-caching" method isn't already specified in `precache_options`
    # (The distinct fsspec and pyarrow optimizations will conflict)
    pre_buffer_default = precache_options.get("method", None) is None
    pre_buffer = (
        {"pre_buffer": read_kwargs.pop("pre_buffer", pre_buffer_default)}
        if pre_buffer_supported
        else {}
    )

    with _open_input_files(
        [path],
        fs=fs,
        precache_options=precache_options,
        **open_file_options,
    )[0] as fil:
        if row_groups == [None]:
            return pq.ParquetFile(fil, **pre_buffer).read(
                columns=columns,
                use_threads=False,
                use_pandas_metadata=True,
                **read_kwargs,
            )
        else:
            return pq.ParquetFile(fil, **pre_buffer).read_row_groups(
                row_groups,
                columns=columns,
                use_threads=False,
                use_pandas_metadata=True,
                **read_kwargs,
            )


def _get_rg_statistics(row_group, col_indices):
    """Custom version of pyarrow's RowGroupInfo.statistics method
    (https://github.com/apache/arrow/blob/master/python/pyarrow/_dataset.pyx)

    We use col_indices to specify the specific subset of columns
    that we need statistics for.  This is more optimal than the
    upstream `RowGroupInfo.statistics` method, which will return
    statistics for all columns.
    """

    if subset_stats_supported:

        def name_stats(i):
            col = row_group.metadata.column(i)

            stats = col.statistics
            if stats is None or not stats.has_min_max:
                return None, None

            name = col.path_in_schema
            field_index = row_group.schema.get_field_index(name)
            if field_index < 0:
                return None, None

            return col.path_in_schema, {
                "min": stats.min,
                "max": stats.max,
            }

        return {
            name: stats
            for name, stats in map(name_stats, col_indices.values())
            if stats is not None
        }

    else:
        return row_group.statistics


def _need_fragments(filters, partition_keys):
    # Check if we need to generate a fragment for filtering.
    # We only need to do this if we are applying filters to
    # columns that were not already filtered by "partition".

    partition_cols = (
        {v[0] for v in flatten(partition_keys, container=list) if len(v)}
        if partition_keys
        else set()
    )
    filtered_cols = (
        {v[0] for v in flatten(filters, container=list) if len(v)} if filters else set()
    )

    return bool(filtered_cols - partition_cols)


#
#  ArrowDatasetEngine
#


class ArrowDatasetEngine(Engine):

    #
    # Public Class Methods
    #

    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        split_row_groups=False,
        chunksize=None,
        aggregate_files=None,
        ignore_metadata_file=False,
        metadata_task_size=0,
        parquet_file_extension=None,
        **kwargs,
    ):

        # Stage 1: Collect general dataset information
        dataset_info = cls._collect_dataset_info(
            paths,
            fs,
            categories,
            index,
            gather_statistics,
            filters,
            split_row_groups,
            chunksize,
            aggregate_files,
            ignore_metadata_file,
            metadata_task_size,
            parquet_file_extension,
            kwargs,
        )

        # Stage 2: Generate output `meta`
        meta = cls._create_dd_meta(dataset_info)

        # Stage 3: Generate parts and stats
        parts, stats, common_kwargs = cls._construct_collection_plan(dataset_info)

        # Add `common_kwargs` and `aggregation_depth` to the first
        # element of `parts`. We can return as a separate element
        # in the future, but should avoid breaking the API for now.
        if len(parts):
            parts[0]["common_kwargs"] = common_kwargs
            parts[0]["aggregation_depth"] = dataset_info["aggregation_depth"]

        return (meta, stats, parts, dataset_info["index"])

    @classmethod
    def multi_support(cls):
        return cls == ArrowDatasetEngine

    @classmethod
    def read_partition(
        cls,
        fs,
        pieces,
        columns,
        index,
        categories=(),
        partitions=(),
        filters=None,
        schema=None,
        **kwargs,
    ):
        """Read in a single output partition"""

        if isinstance(index, list):
            for level in index:
                # unclear if we can use set ops here. I think the order matters.
                # Need the membership test to avoid duplicating index when
                # we slice with `columns` later on.
                if level not in columns:
                    columns.append(level)

        # Ensure `columns` and `partitions` do not overlap
        columns_and_parts = columns.copy()
        if not isinstance(partitions, (list, tuple)):
            if columns_and_parts and partitions:
                for part_name in partitions.partition_names:
                    if part_name in columns:
                        columns.remove(part_name)
                    else:
                        columns_and_parts.append(part_name)
                columns = columns or None

        # Always convert pieces to list
        if not isinstance(pieces, list):
            pieces = [pieces]

        tables = []
        multi_read = len(pieces) > 1
        for piece in pieces:

            if isinstance(piece, str):
                # `piece` is a file-path string
                path_or_frag = piece
                row_group = None
                partition_keys = None
            else:
                # `piece` contains (path, row_group, partition_keys)
                (path_or_frag, row_group, partition_keys) = piece

            # Convert row_group to a list and be sure to
            # check if msgpack converted it to a tuple
            if isinstance(row_group, tuple):
                row_group = list(row_group)
            if not isinstance(row_group, list):
                row_group = [row_group]

            # Read in arrow table and convert to pandas
            arrow_table = cls._read_table(
                path_or_frag,
                fs,
                row_group,
                columns,
                schema,
                filters,
                partitions,
                partition_keys,
                **kwargs,
            )
            if multi_read:
                tables.append(arrow_table)

        if multi_read:
            arrow_table = pa.concat_tables(tables)

        # Convert to pandas
        df = cls._arrow_table_to_pandas(arrow_table, categories, **kwargs)

        # For pyarrow.dataset api, need to convert partition columns
        # to categorigal manually for integer types.
        if partitions and isinstance(partitions, list):
            for partition in partitions:
                if df[partition.name].dtype.name != "category":
                    # We read directly from fragments, so the partition
                    # columns are already in our dataframe.  We just
                    # need to convert non-categorical types.
                    df[partition.name] = pd.Series(
                        pd.Categorical(
                            categories=partition.keys,
                            values=df[partition.name].values,
                        ),
                        index=df.index,
                    )

        # Note that `to_pandas(ignore_metadata=False)` means
        # pyarrow will use the pandas metadata to set the index.
        index_in_columns_and_parts = set(df.index.names).issubset(
            set(columns_and_parts)
        )
        if not index:
            if index_in_columns_and_parts:
                # User does not want to set index and a desired
                # column/partition has been set to the index
                df.reset_index(drop=False, inplace=True)
            else:
                # User does not want to set index and an
                # "unwanted" column has been set to the index
                df.reset_index(drop=True, inplace=True)
        else:
            if set(df.index.names) != set(index) and index_in_columns_and_parts:
                # The wrong index has been set and it contains
                # one or more desired columns/partitions
                df.reset_index(drop=False, inplace=True)
            elif index_in_columns_and_parts:
                # The correct index has already been set
                index = False
                columns_and_parts = list(set(columns_and_parts) - set(df.index.names))
        df = df[list(columns_and_parts)]

        if index:
            df = df.set_index(index)
        return df

    @classmethod
    def initialize_write(
        cls,
        df,
        fs,
        path,
        append=False,
        partition_on=None,
        ignore_divisions=False,
        division_info=None,
        schema=None,
        index_cols=None,
        **kwargs,
    ):
        # Infer schema if "infer"
        # (also start with inferred schema if user passes a dict)
        if schema == "infer" or isinstance(schema, dict):

            # Start with schema from _meta_nonempty
            _schema = pa.Schema.from_pandas(
                df._meta_nonempty.set_index(index_cols)
                if index_cols
                else df._meta_nonempty
            )

            # Use dict to update our inferred schema
            if isinstance(schema, dict):
                schema = pa.schema(schema)
                for name in schema.names:
                    i = _schema.get_field_index(name)
                    j = schema.get_field_index(name)
                    _schema = _schema.set(i, schema.field(j))

            # If we have object columns, we need to sample partitions
            # until we find non-null data for each column in `sample`
            sample = [col for col in df.columns if df[col].dtype == "object"]
            if sample and schema == "infer":
                delayed_schema_from_pandas = delayed(pa.Schema.from_pandas)
                for i in range(df.npartitions):
                    # Keep data on worker
                    _s = delayed_schema_from_pandas(
                        df[sample].to_delayed()[i]
                    ).compute()
                    for name, typ in zip(_s.names, _s.types):
                        if typ != "null":
                            i = _schema.get_field_index(name)
                            j = _s.get_field_index(name)
                            _schema = _schema.set(i, _s.field(j))
                            sample.remove(name)
                    if not sample:
                        break

            # Final (inferred) schema
            schema = _schema

        # Check that target directory exists
        fs.mkdirs(path, exist_ok=True)
        if append and division_info is None:
            ignore_divisions = True

        full_metadata = None  # metadata for the full dataset, from _metadata
        tail_metadata = None  # metadata for at least the last file in the dataset
        i_offset = 0
        metadata_file_exists = False
        if append:
            # Extract metadata and get file offset if appending
            ds = pa_ds.dataset(path, filesystem=fs, format="parquet")
            i_offset = len(ds.files)
            if i_offset > 0:
                try:
                    with fs.open(fs.sep.join([path, "_metadata"]), mode="rb") as fil:
                        full_metadata = pq.read_metadata(fil)
                    tail_metadata = full_metadata
                    metadata_file_exists = True
                except OSError:
                    try:
                        with fs.open(
                            sorted(ds.files, key=natural_sort_key)[-1], mode="rb"
                        ) as fil:
                            tail_metadata = pq.read_metadata(fil)
                    except OSError:
                        pass
            else:
                append = False  # No existing files, can skip the append logic

        # If appending, validate against the initial metadata file (if present)
        if append and tail_metadata is not None:
            arrow_schema = tail_metadata.schema.to_arrow_schema()
            names = arrow_schema.names
            has_pandas_metadata = (
                arrow_schema.metadata is not None and b"pandas" in arrow_schema.metadata
            )
            if has_pandas_metadata:
                pandas_metadata = json.loads(
                    arrow_schema.metadata[b"pandas"].decode("utf8")
                )
                categories = [
                    c["name"]
                    for c in pandas_metadata["columns"]
                    if c["pandas_type"] == "categorical"
                ]
            else:
                categories = None
            dtypes = _get_pyarrow_dtypes(arrow_schema, categories)
            if set(names) != set(df.columns) - set(partition_on):
                raise ValueError(
                    "Appended columns not the same.\n"
                    "Previous: {} | New: {}".format(names, list(df.columns))
                )
            elif (pd.Series(dtypes).loc[names] != df[names].dtypes).any():
                # TODO Coerce values for compatible but different dtypes
                raise ValueError(
                    "Appended dtypes differ.\n{}".format(
                        set(dtypes.items()) ^ set(df.dtypes.items())
                    )
                )

            # Check divisions if necessary
            if division_info["name"] not in names:
                ignore_divisions = True
            if not ignore_divisions:
                old_end = None
                row_groups = (
                    tail_metadata.row_group(i)
                    for i in range(tail_metadata.num_row_groups)
                )
                index_col_i = names.index(division_info["name"])
                for row_group in row_groups:
                    column = row_group.column(index_col_i)
                    if column.statistics:
                        if old_end is None:
                            old_end = column.statistics.max
                        elif column.statistics.max > old_end:
                            old_end = column.statistics.max
                        else:
                            # Existing column on disk isn't sorted, set
                            # `old_end = None` to skip check below
                            old_end = None
                            break

                divisions = division_info["divisions"]
                if old_end is not None and divisions[0] <= old_end:
                    raise ValueError(
                        "The divisions of the appended dataframe overlap with "
                        "previously written divisions. If this is desired, set "
                        "``ignore_divisions=True`` to append anyway.\n"
                        "- End of last written partition: {old_end}\n"
                        "- Start of first new partition: {divisions[0]}"
                    )

        extra_write_kwargs = {"schema": schema, "index_cols": index_cols}
        return i_offset, full_metadata, metadata_file_exists, extra_write_kwargs

    @classmethod
    def _pandas_to_arrow_table(
        cls, df: pd.DataFrame, preserve_index=False, schema=None
    ) -> pa.Table:
        table = pa.Table.from_pandas(
            df, nthreads=1, preserve_index=preserve_index, schema=schema
        )
        return table

    @classmethod
    def write_partition(
        cls,
        df,
        path,
        fs,
        filename,
        partition_on,
        return_metadata,
        fmd=None,
        compression=None,
        index_cols=None,
        schema=None,
        head=False,
        custom_metadata=None,
        **kwargs,
    ):
        _meta = None
        preserve_index = False
        if _index_in_schema(index_cols, schema):
            df.set_index(index_cols, inplace=True)
            preserve_index = True
        else:
            index_cols = []

        t = cls._pandas_to_arrow_table(df, preserve_index=preserve_index, schema=schema)
        if custom_metadata:
            _md = t.schema.metadata
            _md.update(custom_metadata)
            t = t.replace_schema_metadata(metadata=_md)

        if partition_on:
            md_list = _write_partitioned(
                t,
                df,
                path,
                filename,
                partition_on,
                fs,
                cls._pandas_to_arrow_table,
                preserve_index,
                index_cols=index_cols,
                compression=compression,
                return_metadata=return_metadata,
                **kwargs,
            )
            if md_list:
                _meta = md_list[0]
                for i in range(1, len(md_list)):
                    _append_row_groups(_meta, md_list[i])
        else:
            md_list = []
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                pq.write_table(
                    t,
                    fil,
                    compression=compression,
                    metadata_collector=md_list if return_metadata else None,
                    **kwargs,
                )
            if md_list:
                _meta = md_list[0]
                _meta.set_file_path(filename)
        # Return the schema needed to write the metadata
        if return_metadata:
            d = {"meta": _meta}
            if head:
                # Only return schema if this is the "head" partition
                d["schema"] = t.schema
            return [d]
        else:
            return []

    @classmethod
    def write_metadata(cls, parts, meta, fs, path, append=False, **kwargs):
        schema = parts[0][0].get("schema", None)
        parts = [p for p in parts if p[0]["meta"] is not None]
        if parts:
            if not append:
                # Get only arguments specified in the function
                common_metadata_path = fs.sep.join([path, "_common_metadata"])
                keywords = getargspec(pq.write_metadata).args
                kwargs_meta = {k: v for k, v in kwargs.items() if k in keywords}
                with fs.open(common_metadata_path, "wb") as fil:
                    pq.write_metadata(schema, fil, **kwargs_meta)

            # Aggregate metadata and write to _metadata file
            metadata_path = fs.sep.join([path, "_metadata"])
            if append and meta is not None:
                _meta = meta
                i_start = 0
            else:
                _meta = parts[0][0]["meta"]
                i_start = 1
            for i in range(i_start, len(parts)):
                _append_row_groups(_meta, parts[i][0]["meta"])
            with fs.open(metadata_path, "wb") as fil:
                _meta.write_metadata_file(fil)

    #
    # Private Class Methods
    #

    @classmethod
    def _collect_dataset_info(
        cls,
        paths,
        fs,
        categories,
        index,
        gather_statistics,
        filters,
        split_row_groups,
        chunksize,
        aggregate_files,
        ignore_metadata_file,
        metadata_task_size,
        parquet_file_extension,
        kwargs,
    ):
        """pyarrow.dataset version of _collect_dataset_info
        Use pyarrow.dataset API to construct a dictionary of all
        general information needed to read the dataset.
        """

        # Use pyarrow.dataset API
        ds = None
        valid_paths = None  # Only used if `paths` is a list containing _metadata

        # Extract "supported" key-word arguments from `kwargs`
        (
            _dataset_kwargs,
            read_kwargs,
            user_kwargs,
        ) = _split_user_options(**kwargs)

        # Discover Partitioning - Note that we need to avoid creating
        # this factory until it is actually used.  The `partitioning`
        # object can be overridden if a "partitioning" kwarg is passed
        # in, containing a `dict` with a required "obj" argument and
        # optional "arg" and "kwarg" elements.  Note that the "obj"
        # value must support the "discover" attribute.
        partitioning = _dataset_kwargs.pop(
            "partitioning",
            {"obj": pa_ds.HivePartitioning},
        )

        # Case-dependent pyarrow.dataset creation
        has_metadata_file = False
        if len(paths) == 1 and fs.isdir(paths[0]):

            # Use _analyze_paths to avoid relative-path
            # problems (see GH#5608)
            paths, base, fns = _sort_and_analyze_paths(paths, fs)
            paths = fs.sep.join([base, fns[0]])

            meta_path = fs.sep.join([paths, "_metadata"])
            if not ignore_metadata_file and fs.exists(meta_path):
                # Use _metadata file
                ds = pa_ds.parquet_dataset(
                    meta_path,
                    filesystem=fs,
                    partitioning=partitioning["obj"].discover(
                        *partitioning.get("args", []),
                        **partitioning.get("kwargs", {}),
                    ),
                    **_dataset_kwargs,
                )
                has_metadata_file = True
            elif parquet_file_extension:
                # Need to materialize all paths if we are missing the _metadata file
                # Raise error if all files have been filtered by extension
                len0 = len(paths)
                paths = [
                    path
                    for path in fs.find(paths)
                    if path.endswith(parquet_file_extension)
                ]
                if len0 and paths == []:
                    raise ValueError(
                        "No files satisfy the `parquet_file_extension` criteria "
                        f"(files must end with {parquet_file_extension})."
                    )

        elif len(paths) > 1:
            paths, base, fns = _sort_and_analyze_paths(paths, fs)
            meta_path = fs.sep.join([base, "_metadata"])
            if "_metadata" in fns:
                # Pyarrow cannot handle "_metadata" when `paths` is a list
                # Use _metadata file
                if not ignore_metadata_file:
                    ds = pa_ds.parquet_dataset(
                        meta_path,
                        filesystem=fs,
                        partitioning=partitioning["obj"].discover(
                            *partitioning.get("args", []),
                            **partitioning.get("kwargs", {}),
                        ),
                        **_dataset_kwargs,
                    )
                    has_metadata_file = True

                # Populate valid_paths, since the original path list
                # must be used to filter the _metadata-based dataset
                fns.remove("_metadata")
                valid_paths = fns

        # Final "catch-all" pyarrow.dataset call
        if ds is None:
            ds = pa_ds.dataset(
                paths,
                filesystem=fs,
                format="parquet",
                partitioning=partitioning["obj"].discover(
                    *partitioning.get("args", []),
                    **partitioning.get("kwargs", {}),
                ),
                **_dataset_kwargs,
            )

        # Deal with directory partitioning
        # Get all partition keys (without filters) to populate partition_obj
        partition_obj = []  # See `partition_info` description below
        hive_categories = defaultdict(list)
        file_frag = None
        for file_frag in ds.get_fragments():
            keys = pa_ds._get_partition_keys(file_frag.partition_expression)
            if not (keys or hive_categories):
                break  # Bail - This is not a hive-partitioned dataset
            for k, v in keys.items():
                if v not in hive_categories[k]:
                    hive_categories[k].append(v)

        physical_schema = ds.schema
        if file_frag is not None:
            # Check/correct order of `categories` using last file_frag
            #
            # Note that `_get_partition_keys` does NOT preserve the
            # partition-hierarchy order of the keys. Therefore, we
            # use custom logic to determine the "correct" oredering
            # of the `categories` output.
            #
            # Example (why we need to "reorder" `categories`):
            #
            #    # Fragment path has "hive" structure
            #    file_frag.path
            #
            #        '/data/path/b=x/c=x/part.1.parquet'
            #
            #    # `categories` may NOT preserve the hierachy order
            #    categories.keys()
            #
            #        dict_keys(['c', 'b'])
            #
            cat_keys = [
                part.split("=")[0]
                for part in file_frag.path.split(fs.sep)
                if "=" in part
            ]
            if set(hive_categories) == set(cat_keys):
                hive_categories = {
                    k: hive_categories[k] for k in cat_keys if k in hive_categories
                }

            physical_schema = file_frag.physical_schema

        partition_names = list(hive_categories)
        for name in partition_names:
            partition_obj.append(PartitionObj(name, hive_categories[name]))

        # Check the `aggregate_files` setting
        aggregation_depth = _get_aggregation_depth(aggregate_files, partition_names)

        # Note on (hive) partitioning information:
        #
        #    - "partitions" : (list of PartitionObj) This is a list of
        #          simple objects providing `name` and `keys` attributes
        #          for each partition column.
        #    - "partition_names" : (list)  This is a list containing the
        #          names of partitioned columns.
        #    - "partitioning" : (dict) The `partitioning` options
        #          used for file discovery by pyarrow.
        #
        return {
            "ds": ds,
            "physical_schema": physical_schema,
            "has_metadata_file": has_metadata_file,
            "schema": ds.schema,
            "fs": fs,
            "valid_paths": valid_paths,
            "gather_statistics": gather_statistics,
            "categories": categories,
            "index": index,
            "filters": filters,
            "split_row_groups": split_row_groups,
            "chunksize": chunksize,
            "aggregate_files": aggregate_files,
            "aggregation_depth": aggregation_depth,
            "partitions": partition_obj,
            "partition_names": partition_names,
            "partitioning": partitioning,
            "metadata_task_size": metadata_task_size,
            "kwargs": {
                "dataset": _dataset_kwargs,
                "read": read_kwargs,
                **user_kwargs,
            },
        }

    @classmethod
    def _create_dd_meta(cls, dataset_info):
        """Use parquet schema and hive-partition information
        (stored in dataset_info) to construct DataFrame metadata.
        """

        # Collect necessary information from dataset_info
        schema = dataset_info["schema"]
        index = dataset_info["index"]
        categories = dataset_info["categories"]
        partition_obj = dataset_info["partitions"]
        partitions = dataset_info["partition_names"]
        physical_column_names = dataset_info.get("physical_schema", schema).names
        columns = None

        # Set index and column names using
        # pandas metadata (when available)
        pandas_metadata = _get_pandas_metadata(schema)
        if pandas_metadata:
            (
                index_names,
                column_names,
                storage_name_mapping,
                column_index_names,
            ) = _parse_pandas_metadata(pandas_metadata)
            if categories is None:
                categories = []
                for col in pandas_metadata["columns"]:
                    if (col["pandas_type"] == "categorical") and (
                        col["name"] not in categories
                    ):
                        categories.append(col["name"])
        else:
            # No pandas metadata implies no index, unless selected by the user
            index_names = []
            column_names = physical_column_names
            storage_name_mapping = {k: k for k in column_names}
            column_index_names = [None]
        if index is None and index_names:
            # Pandas metadata has provided the index name for us
            index = index_names

        # Ensure that there is no overlap between partition columns
        # and explicit column storage
        if partitions:
            _partitions = [p for p in partitions if p not in physical_column_names]
            if not _partitions:
                partitions = []
                dataset_info["partitions"] = None
                dataset_info["partition_keys"] = {}
                dataset_info["partition_names"] = partitions
            elif len(_partitions) != len(partitions):
                raise ValueError(
                    "No partition-columns should be written in the \n"
                    "file unless they are ALL written in the file.\n"
                    "physical columns: {} | partitions: {}".format(
                        physical_column_names, partitions
                    )
                )

        column_names, index_names = _normalize_index_columns(
            columns, column_names + partitions, index, index_names
        )

        all_columns = index_names + column_names

        # Check that categories are included in columns
        if categories and not set(categories).intersection(all_columns):
            raise ValueError(
                "categories not in available columns.\n"
                "categories: {} | columns: {}".format(categories, list(all_columns))
            )

        dtypes = _get_pyarrow_dtypes(schema, categories)
        dtypes = {storage_name_mapping.get(k, k): v for k, v in dtypes.items()}

        index_cols = index or ()
        meta = _meta_from_dtypes(all_columns, dtypes, index_cols, column_index_names)
        if categories:
            # Make sure all categories are set to "unknown".
            # Cannot include index names in the `cols` argument.
            meta = clear_known_categories(
                meta, cols=[c for c in categories if c not in meta.index.names]
            )

        if partition_obj:

            for partition in partition_obj:
                if isinstance(index, list) and partition.name == index[0]:
                    # Index from directory structure
                    meta.index = pd.CategoricalIndex(
                        [], categories=partition.keys, name=index[0]
                    )
                elif partition.name == meta.index.name:
                    # Index created from a categorical column
                    meta.index = pd.CategoricalIndex(
                        [], categories=partition.keys, name=meta.index.name
                    )
                elif partition.name in meta.columns:
                    meta[partition.name] = pd.Series(
                        pd.Categorical(categories=partition.keys, values=[]),
                        index=meta.index,
                    )

        # Update `dataset_info` and return `meta`
        dataset_info["index"] = index
        dataset_info["index_cols"] = index_cols
        dataset_info["categories"] = categories

        return meta

    @classmethod
    def _construct_collection_plan(cls, dataset_info):
        """pyarrow.dataset version of _construct_collection_plan
        Use dataset_info to construct the general plan for
        generating the output DataFrame collection.

        The "plan" is essentially a list (called `parts`) of
        information that is needed to produce each output partition.
        After this function is returned, the information in each
        element of `parts` will be used to produce a single Dask-
        DataFrame partition (unless some elements of `parts`
        are aggregated together in a follow-up step).

        This method also returns ``stats`` (which is a list of
        parquet-metadata statistics for each element of parts),
        and ``common_metadata`` (which is a dictionary of kwargs
        that should be passed to the ``read_partition`` call for
        every output partition).
        """

        # Collect necessary dataset information from dataset_info
        ds = dataset_info["ds"]
        fs = dataset_info["fs"]
        filters = dataset_info["filters"]
        split_row_groups = dataset_info["split_row_groups"]
        gather_statistics = dataset_info["gather_statistics"]
        chunksize = dataset_info["chunksize"]
        aggregation_depth = dataset_info["aggregation_depth"]
        index_cols = dataset_info["index_cols"]
        schema = dataset_info["schema"]
        partition_names = dataset_info["partition_names"]
        partitioning = dataset_info["partitioning"]
        partitions = dataset_info["partitions"]
        categories = dataset_info["categories"]
        has_metadata_file = dataset_info["has_metadata_file"]
        valid_paths = dataset_info["valid_paths"]
        kwargs = dataset_info["kwargs"]

        # Ensure metadata_task_size is set
        # (Using config file or defaults)
        metadata_task_size = _set_metadata_task_size(
            dataset_info["metadata_task_size"], fs
        )

        # Make sure that any `in`-predicate filters have iterable values
        filter_columns = set()
        if filters is not None:
            for filter in flatten(filters, container=list):
                col, op, val = filter
                if op == "in" and not isinstance(val, (set, list, tuple)):
                    raise TypeError(
                        "Value of 'in' filter must be a list, set or tuple."
                    )
                filter_columns.add(col)

        # Determine which columns need statistics.
        # At this point, gather_statistics is only True if
        # the user specified calculate_divisions=True
        stat_col_indices = {}
        _index_cols = index_cols if (gather_statistics and len(index_cols) == 1) else []
        for i, name in enumerate(schema.names):
            if name in _index_cols or name in filter_columns:
                if name in partition_names:
                    # Partition columns wont have statistics
                    continue
                stat_col_indices[name] = i

        # Decide final `gather_statistics` setting
        gather_statistics = _set_gather_statistics(
            gather_statistics,
            chunksize,
            split_row_groups,
            aggregation_depth,
            filter_columns,
            set(stat_col_indices),
        )

        # Add common kwargs
        common_kwargs = {
            "partitioning": partitioning,
            "partitions": partitions,
            "categories": categories,
            "filters": filters,
            "schema": schema,
            **kwargs,
        }

        # Check if this is a very simple case where we can just return
        # the path names
        if gather_statistics is False and not split_row_groups:
            return (
                [
                    {"piece": (full_path, None, None)}
                    for full_path in sorted(ds.files, key=natural_sort_key)
                ],
                [],
                common_kwargs,
            )

        # Get/transate filters
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)

        # Define subset of `dataset_info` required by _collect_file_parts
        dataset_info_kwargs = {
            "fs": fs,
            "split_row_groups": split_row_groups,
            "gather_statistics": gather_statistics,
            "partitioning": partitioning,
            "filters": filters,
            "ds_filters": ds_filters,
            "schema": schema,
            "stat_col_indices": stat_col_indices,
            "aggregation_depth": aggregation_depth,
            "chunksize": chunksize,
            "partitions": partitions,
        }

        # Main parts/stats-construction
        if (
            has_metadata_file
            or metadata_task_size == 0
            or metadata_task_size > len(ds.files)
        ):
            # We have a global _metadata file to work with.
            # Therefore, we can just loop over fragments on the client.

            # Start with sorted (by path) list of file-based fragments
            file_frags = sorted(
                (frag for frag in ds.get_fragments(ds_filters)),
                key=lambda x: natural_sort_key(x.path),
            )
            parts, stats = cls._collect_file_parts(file_frags, dataset_info_kwargs)
        else:
            # We DON'T have a global _metadata file to work with.
            # We should loop over files in parallel

            # Collect list of file paths.
            # If valid_paths is not None, the user passed in a list
            # of files containing a _metadata file.  Since we used
            # the _metadata file to generate our dataset object , we need
            # to ignore any file fragments that are not in the list.
            all_files = sorted(ds.files, key=natural_sort_key)
            if valid_paths:
                all_files = [
                    filef
                    for filef in all_files
                    if filef.split(fs.sep)[-1] in valid_paths
                ]

            parts, stats = [], []
            if all_files:
                # Build and compute a task graph to construct stats/parts
                gather_parts_dsk = {}
                name = "gather-pq-parts-" + tokenize(all_files, dataset_info_kwargs)
                finalize_list = []
                for task_i, file_i in enumerate(
                    range(0, len(all_files), metadata_task_size)
                ):
                    finalize_list.append((name, task_i))
                    gather_parts_dsk[finalize_list[-1]] = (
                        cls._collect_file_parts,
                        all_files[file_i : file_i + metadata_task_size],
                        dataset_info_kwargs,
                    )

                def _combine_parts(parts_and_stats):
                    parts, stats = [], []
                    for part, stat in parts_and_stats:
                        parts += part
                        if stat:
                            stats += stat
                    return parts, stats

                gather_parts_dsk["final-" + name] = (_combine_parts, finalize_list)
                parts, stats = Delayed("final-" + name, gather_parts_dsk).compute()

        return parts, stats, common_kwargs

    @classmethod
    def _collect_file_parts(
        cls,
        files_or_frags,
        dataset_info_kwargs,
    ):

        # Collect necessary information from dataset_info
        fs = dataset_info_kwargs["fs"]
        split_row_groups = dataset_info_kwargs["split_row_groups"]
        gather_statistics = dataset_info_kwargs["gather_statistics"]
        partitions = dataset_info_kwargs["partitions"]

        # Make sure we are processing a non-empty list
        if not isinstance(files_or_frags, list):
            files_or_frags = [files_or_frags]
        elif not files_or_frags:
            return [], []

        # Make sure we are starting with file fragments
        if isinstance(files_or_frags[0], str):

            # Check if we are using a simple file-partition map
            # without requiring any file or row-group statistics
            if not (split_row_groups or partitions) and gather_statistics is False:
                # Cool - We can return immediately
                return [
                    {"piece": (file_or_frag, None, None)}
                    for file_or_frag in files_or_frags
                ], None

            # Need more information - convert the path to a fragment
            partitioning = dataset_info_kwargs["partitioning"]
            file_frags = list(
                pa_ds.dataset(
                    files_or_frags,
                    filesystem=fs,
                    format="parquet",
                    partitioning=partitioning["obj"].discover(
                        *partitioning.get("args", []),
                        **partitioning.get("kwargs", {}),
                    ),
                ).get_fragments()
            )
        else:
            file_frags = files_or_frags

        # Collect settings from dataset_info
        filters = dataset_info_kwargs["filters"]
        ds_filters = dataset_info_kwargs["ds_filters"]
        schema = dataset_info_kwargs["schema"]
        stat_col_indices = dataset_info_kwargs["stat_col_indices"]
        aggregation_depth = dataset_info_kwargs["aggregation_depth"]
        chunksize = dataset_info_kwargs["chunksize"]

        # Intialize row-group and statistiscs data structures
        file_row_groups = defaultdict(list)
        file_row_group_stats = defaultdict(list)
        file_row_group_column_stats = defaultdict(list)
        single_rg_parts = int(split_row_groups) == 1
        hive_partition_keys = {}
        cmax_last = {}
        for file_frag in file_frags:
            fpath = file_frag.path

            # Extract hive-partition keys, and make sure they
            # are orederd the same as they are in `partitions`
            raw_keys = pa_ds._get_partition_keys(file_frag.partition_expression)
            hive_partition_keys[fpath] = [
                (hive_part.name, raw_keys[hive_part.name]) for hive_part in partitions
            ]

            for frag in file_frag.split_by_row_group(ds_filters, schema=schema):
                row_group_info = frag.row_groups
                if gather_statistics or split_row_groups:
                    # If we are gathering statistics or splitting by
                    # row-group, we may need to ensure our fragment
                    # metadata is complete.
                    if row_group_info is None:
                        frag.ensure_complete_metadata()
                        row_group_info = frag.row_groups
                    if not len(row_group_info):
                        continue
                else:
                    file_row_groups[fpath] = [None]
                    continue
                for row_group in row_group_info:
                    file_row_groups[fpath].append(row_group.id)
                    if gather_statistics:
                        statistics = _get_rg_statistics(row_group, stat_col_indices)
                        if single_rg_parts:
                            s = {
                                "file_path_0": fpath,
                                "num-rows": row_group.num_rows,
                                "total_byte_size": row_group.total_byte_size,
                                "columns": [],
                            }
                        else:
                            s = {
                                "num-rows": row_group.num_rows,
                                "total_byte_size": row_group.total_byte_size,
                            }
                        cstats = []
                        for name, i in stat_col_indices.items():
                            if name in statistics:
                                cmin = statistics[name]["min"]
                                cmax = statistics[name]["max"]
                                cmin = (
                                    pd.Timestamp(cmin)
                                    if isinstance(cmin, datetime)
                                    else cmin
                                )
                                cmax = (
                                    pd.Timestamp(cmax)
                                    if isinstance(cmax, datetime)
                                    else cmax
                                )
                                last = cmax_last.get(name, None)
                                if not (filters or chunksize or aggregation_depth):
                                    # Only think about bailing if we don't need
                                    # stats for filtering
                                    if cmin is None or (last and cmin < last):
                                        # We are collecting statistics for divisions
                                        # only (no filters) - Column isn't sorted, or
                                        # we have an all-null partition, so lets bail.
                                        #
                                        # Note: This assumes ascending order.
                                        #
                                        gather_statistics = False
                                        file_row_group_stats = {}
                                        file_row_group_column_stats = {}
                                        break

                                if single_rg_parts:
                                    s["columns"].append(
                                        {
                                            "name": name,
                                            "min": cmin,
                                            "max": cmax,
                                        }
                                    )
                                else:
                                    cstats += [cmin, cmax]
                                cmax_last[name] = cmax
                            else:
                                if single_rg_parts:
                                    s["columns"].append({"name": name})
                                else:
                                    cstats += [None, None, None]
                        if gather_statistics:
                            file_row_group_stats[fpath].append(s)
                            if not single_rg_parts:
                                file_row_group_column_stats[fpath].append(tuple(cstats))

        # Check if we have empty parts to return
        if not file_row_groups:
            return [], []

        # Convert organized row-groups to parts
        return _row_groups_to_parts(
            gather_statistics,
            split_row_groups,
            aggregation_depth,
            file_row_groups,
            file_row_group_stats,
            file_row_group_column_stats,
            stat_col_indices,
            cls._make_part,
            make_part_kwargs={
                "fs": fs,
                "partition_keys": hive_partition_keys,
                "partition_obj": partitions,
                "data_path": "",
            },
        )

    @classmethod
    def _make_part(
        cls,
        filename,
        rg_list,
        fs=None,
        partition_keys=None,
        partition_obj=None,
        data_path=None,
    ):
        """Generate a partition-specific element of `parts`."""

        # Get full path (empty strings should be ignored)
        full_path = fs.sep.join([p for p in [data_path, filename] if p != ""])

        pkeys = partition_keys.get(full_path, None)
        if partition_obj and pkeys is None:
            return None  # This partition was filtered
        return {"piece": (full_path, rg_list, pkeys)}

    @classmethod
    def _read_table(
        cls,
        path_or_frag,
        fs,
        row_groups,
        columns,
        schema,
        filters,
        partitions,
        partition_keys,
        **kwargs,
    ):
        """Read in a pyarrow table"""

        if isinstance(path_or_frag, pa_ds.ParquetFileFragment):
            frag = path_or_frag

        else:
            frag = None

            # Check if we have partitioning information.
            # Will only have this if the engine="pyarrow-dataset"
            partitioning = kwargs.pop("partitioning", None)

            # Check if we need to generate a fragment for filtering.
            # We only need to do this if we are applying filters to
            # columns that were not already filtered by "partition".
            if (partitions and partition_keys is None) or (
                partitioning and _need_fragments(filters, partition_keys)
            ):

                # We are filtering with "pyarrow-dataset".
                # Need to convert the path and row-group IDs
                # to a single "fragment" to read
                ds = pa_ds.dataset(
                    path_or_frag,
                    filesystem=fs,
                    format="parquet",
                    partitioning=partitioning["obj"].discover(
                        *partitioning.get("args", []),
                        **partitioning.get("kwargs", {}),
                    ),
                    **kwargs.get("dataset", {}),
                )
                frags = list(ds.get_fragments())
                assert len(frags) == 1
                frag = (
                    _frag_subset(frags[0], row_groups)
                    if row_groups != [None]
                    else frags[0]
                )

                # Extract hive-partition keys, and make sure they
                # are orderd the same as they are in `partitions`
                raw_keys = pa_ds._get_partition_keys(frag.partition_expression)
                partition_keys = [
                    (hive_part.name, raw_keys[hive_part.name])
                    for hive_part in partitions
                ]

        if frag:
            cols = []
            for name in columns:
                if name is None:
                    if "__index_level_0__" in schema.names:
                        columns.append("__index_level_0__")
                else:
                    cols.append(name)

            arrow_table = frag.to_table(
                use_threads=False,
                schema=schema,
                columns=cols,
                filter=pq._filters_to_expression(filters) if filters else None,
            )
        else:
            arrow_table = _read_table_from_path(
                path_or_frag,
                fs,
                row_groups,
                columns,
                schema,
                filters,
                **kwargs,
            )

        # For pyarrow.dataset api, if we did not read directly from
        # fragments, we need to add the partitioned columns here.
        if partitions and isinstance(partitions, list):
            keys_dict = {k: v for (k, v) in partition_keys}
            for partition in partitions:
                if partition.name not in arrow_table.schema.names:
                    # We read from file paths, so the partition
                    # columns are NOT in our table yet.
                    cat = keys_dict.get(partition.name, None)
                    cat_ind = np.full(
                        len(arrow_table), partition.keys.index(cat), dtype="i4"
                    )
                    arr = pa.DictionaryArray.from_arrays(
                        cat_ind, pa.array(partition.keys)
                    )
                    arrow_table = arrow_table.append_column(partition.name, arr)

        return arrow_table

    @classmethod
    def _arrow_table_to_pandas(
        cls, arrow_table: pa.Table, categories, **kwargs
    ) -> pd.DataFrame:
        _kwargs = kwargs.get("arrow_to_pandas", {})
        _kwargs.update({"use_threads": False, "ignore_metadata": False})

        return arrow_table.to_pandas(categories=categories, **_kwargs)

    @classmethod
    def collect_file_metadata(cls, path, fs, file_path):
        with fs.open(path, "rb") as f:
            meta = pq.ParquetFile(f).metadata
        if file_path:
            meta.set_file_path(file_path)
        return meta

    @classmethod
    def aggregate_metadata(cls, meta_list, fs, out_path):
        meta = None
        for _meta in meta_list:
            if meta:
                _append_row_groups(meta, _meta)
            else:
                meta = _meta
        if out_path:
            metadata_path = fs.sep.join([out_path, "_metadata"])
            with fs.open(metadata_path, "wb") as fil:
                if not meta:
                    raise ValueError("Cannot write empty metdata!")
                meta.write_metadata_file(fil)
            return None
        else:
            return meta
