from functools import partial
from collections import defaultdict
import json
import warnings
from distutils.version import LooseVersion

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ....utils import getargspec
from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes
from ...utils import clear_known_categories
from ....core import flatten
from dask import delayed

from .utils import (
    _parse_pandas_metadata,
    _normalize_index_columns,
    Engine,
    _analyze_paths,
)

preserve_ind_supported = pa.__version__ >= LooseVersion("0.15.0")
schema_field_supported = pa.__version__ >= LooseVersion("0.15.0")


#
#  Private Helper Functions
#


def _append_row_groups(metadata, md):
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
    table, root_path, filename, partition_cols, fs, index_cols=(), **kwargs
):
    """Write table to a partitioned dataset with pyarrow.

    Logic copied from pyarrow.parquet.
    (arrow/python/pyarrow/parquet.py::write_to_dataset)

    TODO: Remove this in favor of pyarrow's `write_to_dataset`
          once ARROW-8244 is addressed.
    """
    fs.mkdirs(root_path, exist_ok=True)

    df = table.to_pandas(ignore_metadata=True)
    index_cols = list(index_cols) if index_cols else []
    preserve_index = False
    if index_cols and preserve_ind_supported:
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
            [
                "{colname}={value}".format(colname=name, value=val)
                for name, val in zip(partition_cols, keys)
            ]
        )
        subtable = pa.Table.from_pandas(
            subgroup, preserve_index=preserve_index, schema=subschema, safe=False
        )
        prefix = fs.sep.join([root_path, subdir])
        fs.mkdirs(prefix, exist_ok=True)
        full_path = fs.sep.join([prefix, filename])
        with fs.open(full_path, "wb") as f:
            pq.write_table(subtable, f, metadata_collector=md_list, **kwargs)
        md_list[-1].set_file_path(fs.sep.join([subdir, filename]))

    return md_list


def _index_in_schema(index, schema):
    if index and schema is not None:
        # Make sure all index columns are in user-defined schema
        return len(set(index).intersection(schema.names)) == len(index)
    elif index:
        return True  # Schema is not user-specified, all good
    else:
        return False  # No index to check


def _get_dataset_object(paths, fs, filters, dataset_kwargs):
    """Generate a ParquetDataset object"""
    kwargs = dataset_kwargs.copy()
    if "validate_schema" not in kwargs:
        kwargs["validate_schema"] = False
    if len(paths) > 1:
        # This is a list of files
        base, fns = _analyze_paths(paths, fs)
        proxy_metadata = None
        if "_metadata" in fns:
            # We have a _metadata file. PyArrow cannot handle
            #  "_metadata" when `paths` is a list. So, we shuld
            # open "_metadata" separately.
            paths.remove(fs.sep.join([base, "_metadata"]))
            fns.remove("_metadata")
            with fs.open(fs.sep.join([base, "_metadata"]), mode="rb") as fil:
                proxy_metadata = pq.ParquetFile(fil).metadata
        # Create our dataset from the list of data files.
        # Note #1: that this will not parse all the files (yet)
        # Note #2: Cannot pass filters for legacy pyarrow API (see issue#6512).
        #          We can handle partitions + filtering for list input after
        #          adopting new pyarrow.dataset API.
        dataset = pq.ParquetDataset(paths, filesystem=fs, **kwargs)
        if proxy_metadata:
            dataset.metadata = proxy_metadata
    elif fs.isdir(paths[0]):
        # This is a directory.  We can let pyarrow do its thing.
        # Note: In the future, it may be best to avoid listing the
        #       directory if we can get away with checking for the
        #       existence of _metadata.  Listing may be much more
        #       expensive in storage systems like S3.
        allpaths = fs.glob(paths[0] + fs.sep + "*")
        base, fns = _analyze_paths(allpaths, fs)
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, filters=filters, **kwargs)
    else:
        # This is a single file.  No danger in gathering statistics
        # and/or splitting row-groups without a "_metadata" file
        base = paths[0]
        fns = [None]
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, **kwargs)

    return dataset, base, fns


def _gather_metadata(
    paths, fs, split_row_groups, gather_statistics, filters, dataset_kwargs
):
    """Gather parquet metadata into a single data structure.

    Use _metadata or aggregate footer metadata into a single
    object.  Also, collect other information necessary for
    parquet-to-ddf mapping (e.g. schema, partition_info).
    """

    # Step 1: Create a ParquetDataset object
    dataset, base, fns = _get_dataset_object(paths, fs, filters, dataset_kwargs)
    if fns == [None]:
        # This is a single file. No danger in gathering statistics
        # and/or splitting row-groups without a "_metadata" file
        if gather_statistics is None:
            gather_statistics = True
        if split_row_groups is None:
            split_row_groups = True

    # Step 2: Construct necessary (parquet) partitioning information
    partition_info = {"partitions": None, "partition_keys": {}, "partition_names": []}
    fn_partitioned = False
    if dataset.partitions is not None:
        fn_partitioned = True
        partition_info["partition_names"] = [
            n for n in dataset.partitions.partition_names if n is not None
        ]
        partition_info["partitions"] = dataset.partitions
        for piece in dataset.pieces:
            partition_info["partition_keys"][piece.path] = piece.partition_keys

    # Step 3: Construct a single `metadata` object. We can
    #         directly use dataset.metadata if it is available.
    #         Otherwise, if `gather_statistics` or `split_row_groups`,
    #         we need to gether the footer metadata manually
    metadata = None
    if dataset.metadata:
        # We have a _metadata file.
        # PyArrow already did the work for us
        schema = dataset.metadata.schema.to_arrow_schema()
        if gather_statistics is None:
            gather_statistics = True
        if split_row_groups is None:
            split_row_groups = True
        return (
            schema,
            dataset.metadata,
            base,
            partition_info,
            split_row_groups,
            gather_statistics,
        )
    else:
        # No _metadata file.
        # May need to collect footer metadata manually
        if dataset.schema is not None:
            schema = dataset.schema.to_arrow_schema()
        else:
            schema = None
        if gather_statistics is None:
            gather_statistics = False
        if split_row_groups is None:
            split_row_groups = False
        metadata = None
        if not (split_row_groups or gather_statistics):
            # Don't need to construct real metadata if
            # we are not gathering statistics or splitting
            # by row-group
            metadata = [p.path for p in dataset.pieces]
            if schema is None:
                schema = dataset.pieces[0].get_metadata().schema.to_arrow_schema()
            return (
                schema,
                metadata,
                base,
                partition_info,
                split_row_groups,
                gather_statistics,
            )
        # We have not detected a _metadata file, and the user has specified
        # that they want to split by row-group and/or gather statistics.
        # This is the only case where we MUST scan all files to collect
        # metadata.
        for piece, fn in zip(dataset.pieces, fns):
            md = piece.get_metadata()
            if schema is None:
                schema = md.schema.to_arrow_schema()
            if fn_partitioned:
                md.set_file_path(piece.path.replace(base + fs.sep, ""))
            elif fn:
                md.set_file_path(fn)
            if metadata:
                _append_row_groups(metadata, md)
            else:
                metadata = md
        return (
            schema,
            metadata,
            base,
            partition_info,
            split_row_groups,
            gather_statistics,
        )


def _generate_dd_meta(schema, index, categories, partition_info):
    partition_obj = partition_info["partitions"]
    partitions = partition_info["partition_names"]
    columns = None

    has_pandas_metadata = schema.metadata is not None and b"pandas" in schema.metadata

    if has_pandas_metadata:
        pandas_metadata = json.loads(schema.metadata[b"pandas"].decode("utf8"))
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
        column_names = schema.names
        storage_name_mapping = {k: k for k in column_names}
        column_index_names = [None]

    if index is None and index_names:
        index = index_names

    if set(column_names).intersection(partitions):
        raise ValueError(
            "partition(s) should not exist in columns.\n"
            "categories: {} | partitions: {}".format(column_names, partitions)
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
    meta = clear_known_categories(meta, cols=categories)

    if partition_obj:
        for partition in partition_obj:
            if isinstance(index, list) and partition.name == index[0]:
                # Index from directory structure
                meta.index = pd.CategoricalIndex(
                    categories=partition.keys, name=index[0]
                )
            elif partition.name == meta.index.name:
                # Index created from a categorical column
                meta.index = pd.CategoricalIndex(
                    categories=partition.keys, name=meta.index.name
                )
            elif partition.name in meta.columns:
                meta[partition.name] = pd.Series(
                    pd.Categorical(categories=partition.keys, values=[]),
                    index=meta.index,
                )

    return meta, index_cols, categories, index


def _aggregate_stats(
    file_path, file_row_group_stats, file_row_group_column_stats, stat_col_indices
):
    """Utility to aggregate the statistics for N row-groups
    into a single dictionary.
    """
    if len(file_row_group_stats) < 1:
        # Empty statistics
        return {}
    elif len(file_row_group_column_stats) == 0:
        assert len(file_row_group_stats) == 1
        return file_row_group_stats[0]
    else:
        # Note: It would be better to avoid df_rgs and df_cols
        #       construction altogether. It makes it fast to aggregate
        #       the statistics for many row groups, but isn't
        #       worthwhile for a small number of row groups.
        if len(file_row_group_stats) > 1:
            df_rgs = pd.DataFrame(file_row_group_stats)
            s = {
                "file_path_0": file_path,
                "num-rows": df_rgs["num-rows"].sum(),
                "total_byte_size": df_rgs["total_byte_size"].sum(),
                "columns": [],
            }
        else:
            s = {
                "file_path_0": file_path,
                "num-rows": file_row_group_stats[0]["num-rows"],
                "total_byte_size": file_row_group_stats[0]["total_byte_size"],
                "columns": [],
            }

        df_cols = None
        if len(file_row_group_column_stats) > 1:
            df_cols = pd.DataFrame(file_row_group_column_stats)
        for ind, name in enumerate(stat_col_indices):
            i = ind * 3
            if df_cols is None:
                s["columns"].append(
                    {
                        "name": name,
                        "min": file_row_group_column_stats[0][i],
                        "max": file_row_group_column_stats[0][i + 1],
                        "null_count": file_row_group_column_stats[0][i + 2],
                    }
                )
            else:
                s["columns"].append(
                    {
                        "name": name,
                        "min": df_cols.iloc[:, i].min(),
                        "max": df_cols.iloc[:, i + 1].max(),
                        "null_count": df_cols.iloc[:, i + 2].sum(),
                    }
                )
        return s


def _process_metadata(
    metadata, single_rg_parts, gather_statistics, stat_col_indices, no_filters
):
    # Get the number of row groups per file
    file_row_groups = defaultdict(list)
    file_row_group_stats = defaultdict(list)
    file_row_group_column_stats = defaultdict(list)
    cmax_last = {}
    for rg in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg)
        fpath = row_group.column(0).file_path
        if fpath is None:
            raise ValueError(
                "Global metadata structure is missing a file_path string. "
                "If the dataset includes a _metadata file, that file may "
                "have one or more missing file_path fields."
            )
        if file_row_groups[fpath]:
            file_row_groups[fpath].append(file_row_groups[fpath][-1] + 1)
        else:
            file_row_groups[fpath].append(0)
        if gather_statistics:
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
                column = row_group.column(i)
                if column.statistics:
                    cmin = column.statistics.min
                    cmax = column.statistics.max
                    cnull = column.statistics.null_count
                    last = cmax_last.get(name, None)
                    if no_filters:
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
                        to_ts = column.statistics.logical_type.type == "TIMESTAMP"
                        s["columns"].append(
                            {
                                "name": name,
                                "min": cmin if not to_ts else pd.Timestamp(cmin),
                                "max": cmax if not to_ts else pd.Timestamp(cmax),
                                "null_count": cnull,
                            }
                        )
                    else:
                        cstats += [cmin, cmax, cnull]
                    cmax_last[name] = cmax
                else:

                    if no_filters and column.num_values > 0:
                        # We are collecting statistics for divisions
                        # only (no filters) - Lets bail.
                        gather_statistics = False
                        file_row_group_stats = {}
                        file_row_group_column_stats = {}
                        break

                    if single_rg_parts:
                        s["columns"].append({"name": name})
                    else:
                        cstats += [None, None, None]
            if gather_statistics:
                file_row_group_stats[fpath].append(s)
                if not single_rg_parts:
                    file_row_group_column_stats[fpath].append(tuple(cstats))

    return (
        file_row_groups,
        file_row_group_stats,
        file_row_group_column_stats,
        gather_statistics,
    )


def _construct_parts(
    fs,
    metadata,
    schema,
    filters,
    index_cols,
    data_path,
    partition_info,
    categories,
    split_row_groups,
    gather_statistics,
):
    """Construct ``parts`` for ddf construction

    Use metadata (along with other data) to define a tuple
    for each ddf partition.  Also gather statistics if
    ``gather_statistics=True``, and other criteria is met.
    """

    parts = []
    stats = []

    partition_keys = partition_info["partition_keys"]
    partition_obj = partition_info["partitions"]

    # Check if `metadata` is just a list of paths
    # (not splitting by row-group or collecting statistics)
    if isinstance(metadata, list) and isinstance(metadata[0], str):
        for full_path in metadata:
            part = {
                "piece": (full_path, None, partition_keys.get(full_path, None)),
                "kwargs": {"partitions": partition_obj, "categories": categories},
            }
            parts.append(part)
        return parts, stats

    # Determine which columns need statistics
    flat_filters = (
        set(flatten(tuple(flatten(filters, container=list)), container=tuple))
        if filters
        else []
    )
    stat_col_indices = {}
    for i, name in enumerate(schema.names):
        if name in index_cols or name in flat_filters:
            stat_col_indices[name] = i
    stat_cols = list(stat_col_indices.keys())
    gather_statistics = gather_statistics and len(stat_cols) > 0

    # Convert metadata into simple dictionary structures
    (
        file_row_groups,
        file_row_group_stats,
        file_row_group_column_stats,
        gather_statistics,
    ) = _process_metadata(
        metadata,
        int(split_row_groups) == 1,
        gather_statistics,
        stat_col_indices,
        flat_filters == [],
    )

    if split_row_groups:
        # Create parts from each file,
        # limiting the number of row_groups in each piece
        split_row_groups = int(split_row_groups)
        for filename, row_groups in file_row_groups.items():
            row_group_count = len(row_groups)
            for i in range(0, row_group_count, split_row_groups):
                i_end = i + split_row_groups
                rg_list = row_groups[i:i_end]
                full_path = (
                    fs.sep.join([data_path, filename])
                    if filename != ""
                    else data_path  # This is a single file
                )
                pkeys = partition_keys.get(full_path, None)
                if partition_obj and pkeys is None:
                    continue  # This partition was filtered
                part = {
                    "piece": (full_path, rg_list, pkeys),
                    "kwargs": {
                        "partitions": partition_obj,
                        "categories": categories,
                        "filters": filters,
                        "schema": schema,
                    },
                }
                parts.append(part)
                if gather_statistics:
                    stat = _aggregate_stats(
                        filename,
                        file_row_group_stats[filename][i:i_end],
                        file_row_group_column_stats[filename][i:i_end],
                        stat_col_indices,
                    )
                    stats.append(stat)
    else:
        for filename, row_groups in file_row_groups.items():
            full_path = (
                fs.sep.join([data_path, filename])
                if filename != ""
                else data_path  # This is a single file
            )
            pkeys = partition_keys.get(full_path, None)
            if partition_obj and pkeys is None:
                continue  # This partition was filtered
            rgs = None
            part = {
                "piece": (full_path, rgs, pkeys),
                "kwargs": {
                    "partitions": partition_obj,
                    "categories": categories,
                    "filters": filters,
                    "schema": schema,
                },
            }
            parts.append(part)
            if gather_statistics:
                stat = _aggregate_stats(
                    filename,
                    file_row_group_stats[filename],
                    file_row_group_column_stats[filename],
                    stat_col_indices,
                )
                stats.append(stat)

    return parts, stats


class ArrowEngine(Engine):
    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        split_row_groups=None,
        **kwargs,
    ):

        # Check if we are using pyarrow.dataset API
        dataset_kwargs = kwargs.get("dataset", {})

        # Gather necessary metadata information. This includes
        # the schema and (parquet) partitioning information.
        # This may also set split_row_groups and gather_statistics,
        # depending on _metadata availability.
        (
            schema,
            metadata,
            base_path,
            partition_info,
            split_row_groups,
            gather_statistics,
        ) = _gather_metadata(
            paths, fs, split_row_groups, gather_statistics, filters, dataset_kwargs
        )

        # Process metadata to define `meta` and `index_cols`
        meta, index_cols, categories, index = _generate_dd_meta(
            schema, index, categories, partition_info
        )

        # Cannot gather_statistics if our `metadata` is a list
        # of paths, or if we are building a multiindex (for now).
        # We also don't "need" to gather statistics if we don't
        # want to apply any filters or calculate divisions
        if (isinstance(metadata, list) and isinstance(metadata[0], str)) or len(
            index_cols
        ) > 1:
            gather_statistics = False
        elif filters is None and len(index_cols) == 0:
            gather_statistics = False

        # Make sure gather_statistics allows filtering
        # (if filters are desired)
        if filters:
            # Filters may require us to gather statistics
            if gather_statistics is False and partition_info["partition_names"]:
                warnings.warn(
                    "Filtering with gather_statistics=False. "
                    "Only partition columns will be filtered correctly."
                )
            elif gather_statistics is False:
                raise ValueError("Cannot apply filters with gather_statistics=False")
            elif not gather_statistics:
                gather_statistics = True

        # Finally, construct our list of `parts`
        # (and a corresponding list of statistics)
        parts, stats = _construct_parts(
            fs,
            metadata,
            schema,
            filters,
            index_cols,
            base_path,
            partition_info,
            categories,
            split_row_groups,
            gather_statistics,
        )

        return (meta, stats, parts, index)

    @classmethod
    def read_partition(
        cls,
        fs,
        piece,
        columns,
        index,
        categories=(),
        partitions=(),
        filters=None,
        schema=None,
        **kwargs,
    ):
        if isinstance(index, list):
            for level in index:
                # unclear if we can use set ops here. I think the order matters.
                # Need the membership test to avoid duplicating index when
                # we slice with `columns` later on.
                if level not in columns:
                    columns.append(level)

        # Ensure `columns` and `partitions` do not overlap
        columns_and_parts = columns.copy()
        if columns_and_parts and partitions:
            for part_name in partitions.partition_names:
                if part_name in columns:
                    columns.remove(part_name)
                else:
                    columns_and_parts.append(part_name)
            columns = columns or None

        if isinstance(piece, str):
            # `piece` is a file-path string
            path = piece
            row_group = None
            partition_keys = None
        else:
            # `piece` contains (path, row_group, partition_keys)
            (path, row_group, partition_keys) = piece

        if not isinstance(row_group, list):
            row_group = [row_group]

        dfs = []
        for rg in row_group:
            piece = pq.ParquetDatasetPiece(
                path,
                row_group=rg,
                partition_keys=partition_keys,
                open_file_func=partial(fs.open, mode="rb"),
            )
            arrow_table = cls._parquet_piece_as_arrow(
                piece, columns, partitions, **kwargs
            )
            df = cls._arrow_table_to_pandas(arrow_table, categories, **kwargs)

            if len(row_group) > 1:
                dfs.append(df)

        if len(row_group) > 1:
            df = pd.concat(dfs)

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
                columns_and_parts = list(
                    set(columns_and_parts).difference(set(df.index.names))
                )
        df = df[list(columns_and_parts)]

        if index:
            df = df.set_index(index)
        return df

    @classmethod
    def _arrow_table_to_pandas(
        cls, arrow_table: pa.Table, categories, **kwargs
    ) -> pd.DataFrame:
        _kwargs = kwargs.get("arrow_to_pandas", {})
        _kwargs.update({"use_threads": False, "ignore_metadata": False})

        return arrow_table.to_pandas(categories=categories, **_kwargs)

    @classmethod
    def _parquet_piece_as_arrow(
        cls, piece: pq.ParquetDatasetPiece, columns, partitions, **kwargs
    ) -> pa.Table:
        arrow_table = piece.read(
            columns=columns,
            partitions=partitions,
            use_pandas_metadata=True,
            use_threads=False,
            **kwargs.get("read", {}),
        )
        return arrow_table

    @staticmethod
    def initialize_write(
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
            if schema_field_supported and sample and schema == "infer":
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

        dataset = fmd = None
        i_offset = 0
        if append and division_info is None:
            ignore_divisions = True
        fs.mkdirs(path, exist_ok=True)

        if append:
            try:
                # Allow append if the dataset exists.
                # Also need dataset.metadata object if
                # ignore_divisions is False (to check divisions)
                dataset = pq.ParquetDataset(path, filesystem=fs)
                if not dataset.metadata and not ignore_divisions:
                    # TODO: Be more flexible about existing metadata.
                    raise NotImplementedError(
                        "_metadata file needed to `append` "
                        "with `engine='pyarrow'` "
                        "unless `ignore_divisions` is `True`"
                    )
                fmd = dataset.metadata
            except (IOError, ValueError, IndexError):
                # Original dataset does not exist - cannot append
                append = False
        if append:
            names = dataset.metadata.schema.names
            has_pandas_metadata = (
                dataset.schema.to_arrow_schema().metadata is not None
                and b"pandas" in dataset.schema.to_arrow_schema().metadata
            )
            if has_pandas_metadata:
                pandas_metadata = json.loads(
                    dataset.schema.to_arrow_schema().metadata[b"pandas"].decode("utf8")
                )
                categories = [
                    c["name"]
                    for c in pandas_metadata["columns"]
                    if c["pandas_type"] == "categorical"
                ]
            else:
                categories = None
            dtypes = _get_pyarrow_dtypes(dataset.schema.to_arrow_schema(), categories)
            if set(names) != set(df.columns) - set(partition_on):
                raise ValueError(
                    "Appended columns not the same.\n"
                    "Previous: {} | New: {}".format(names, list(df.columns))
                )
            elif (pd.Series(dtypes).loc[names] != df[names].dtypes).any():
                # TODO Coerce values for compatible but different dtypes
                raise ValueError(
                    "Appended dtypes differ.\n{}".format(
                        set(dtypes.items()) ^ set(df.dtypes.iteritems())
                    )
                )
            i_offset = len(dataset.pieces)

            if division_info["name"] not in names:
                ignore_divisions = True
            if not ignore_divisions:
                old_end = None
                row_groups = [
                    dataset.metadata.row_group(i)
                    for i in range(dataset.metadata.num_row_groups)
                ]
                for row_group in row_groups:
                    for i, name in enumerate(names):
                        if name != division_info["name"]:
                            continue
                        column = row_group.column(i)
                        if column.statistics:
                            if not old_end:
                                old_end = column.statistics.max
                            else:
                                old_end = max(old_end, column.statistics.max)
                            break

                divisions = division_info["divisions"]
                if divisions[0] < old_end:
                    raise ValueError(
                        "Appended divisions overlapping with the previous ones"
                        " (set ignore_divisions=True to append anyway).\n"
                        "Previous: {} | New: {}".format(old_end, divisions[0])
                    )

        return fmd, schema, i_offset

    @classmethod
    def _pandas_to_arrow_table(
        cls, df: pd.DataFrame, preserve_index=False, schema=None
    ) -> pa.Table:
        table = pa.Table.from_pandas(df, preserve_index=preserve_index, schema=schema)
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

        if partition_on:
            md_list = _write_partitioned(
                t,
                path,
                filename,
                partition_on,
                fs,
                index_cols=index_cols,
                compression=compression,
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
                    metadata_collector=md_list,
                    **kwargs,
                )
            if md_list:
                _meta = md_list[0]
                _meta.set_file_path(filename)
        # Return the schema needed to write the metadata
        if return_metadata:
            return [{"schema": t.schema, "meta": _meta}]
        else:
            return []

    @staticmethod
    def write_metadata(parts, fmd, fs, path, append=False, **kwargs):
        parts = [p for p in parts if p[0]["meta"] is not None]
        if parts:
            if not append:
                # Get only arguments specified in the function
                common_metadata_path = fs.sep.join([path, "_common_metadata"])
                keywords = getargspec(pq.write_metadata).args
                kwargs_meta = {k: v for k, v in kwargs.items() if k in keywords}
                with fs.open(common_metadata_path, "wb") as fil:
                    pq.write_metadata(parts[0][0]["schema"], fil, **kwargs_meta)

            # Aggregate metadata and write to _metadata file
            metadata_path = fs.sep.join([path, "_metadata"])
            if append and fmd is not None:
                _meta = fmd
                i_start = 0
            else:
                _meta = parts[0][0]["meta"]
                i_start = 1
            for i in range(i_start, len(parts)):
                _append_row_groups(_meta, parts[i][0]["meta"])
            with fs.open(metadata_path, "wb") as fil:
                _meta.write_metadata_file(fil)
