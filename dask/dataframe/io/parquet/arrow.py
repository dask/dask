import json
import warnings
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from packaging.version import parse as parse_version

from dask import delayed

from ....base import tokenize
from ....core import flatten
from ....delayed import Delayed
from ....utils import getargspec, natural_sort_key
from ...utils import clear_known_categories
from ..utils import (
    _get_pyarrow_dtypes,
    _is_local_fs,
    _meta_from_dtypes,
    _open_input_files,
)
from .core import create_metadata_file
from .utils import (
    Engine,
    _flatten_filters,
    _get_aggregation_depth,
    _normalize_index_columns,
    _parse_pandas_metadata,
    _process_open_file_options,
    _row_groups_to_parts,
    _set_metadata_task_size,
    _sort_and_analyze_paths,
    _split_user_options,
)

# Check PyArrow version for feature support
_pa_version = parse_version(pa.__version__)
from pyarrow import dataset as pa_ds

subset_stats_supported = _pa_version > parse_version("2.0.0")
del _pa_version

#
#  Helper Utilities
#


def _append_row_groups(metadata, md):
    """Append row-group metadata and include a helpful
    error message if an inconsistent schema is detected.

    Used by `ArrowDatasetEngine` and `ArrowLegacyEngine`.
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
    preserve_index,
    index_cols=(),
    **kwargs,
):
    """Write table to a partitioned dataset with pyarrow.

    Logic copied from pyarrow.parquet.
    (arrow/python/pyarrow/parquet.py::write_to_dataset)

    Used by `ArrowDatasetEngine` (and by `ArrowLegacyEngine`,
    through inherited `write_partition` method).

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
        subtable = pa.Table.from_pandas(
            subgroup,
            nthreads=1,
            preserve_index=preserve_index,
            schema=subschema,
            safe=False,
        )
        prefix = fs.sep.join([root_path, subdir])
        fs.mkdirs(prefix, exist_ok=True)
        full_path = fs.sep.join([prefix, filename])
        with fs.open(full_path, "wb") as f:
            pq.write_table(subtable, f, metadata_collector=md_list, **kwargs)
        md_list[-1].set_file_path(fs.sep.join([subdir, filename]))

    return md_list


def _index_in_schema(index, schema):
    """Simple utility to check if all `index` columns are included
    in the known `schema`.

    Used by `ArrowDatasetEngine` (and by `ArrowLegacyEngine`,
    through inherited `write_partition` method).
    """
    if index and schema is not None:
        # Make sure all index columns are in user-defined schema
        return len(set(index).intersection(schema.names)) == len(index)
    elif index:
        return True  # Schema is not user-specified, all good
    else:
        return False  # No index to check


class PartitionObj:
    """Simple object to provide a `name` and `keys` attribute
    for a single partition column. `ArrowDatasetEngine` will use
    a list of these objects to "duck type" a `ParquetPartitions`
    object (used in `ArrowLegacyEngine`). The larger purpose of this
    class is to allow the same `read_partition` definition to handle
    both Engine instances.

    Used by `ArrowDatasetEngine` only.
    """

    def __init__(self, name, keys):
        self.name = name
        self.keys = sorted(keys)


def _frag_subset(old_frag, row_groups):
    """Create new fragment with row-group subset.

    Used by `ArrowDatasetEngine` only.
    """
    return old_frag.format.make_fragment(
        old_frag.path,
        old_frag.filesystem,
        old_frag.partition_expression,
        row_groups=row_groups,
    )


def _get_pandas_metadata(schema):
    """Get pandas-specific metadata from schema.

    Used by `ArrowDatasetEngine` and `ArrowLegacyEngine`.
    """

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
    partitions,
    partition_keys,
    piece_to_arrow_func,
    **kwargs,
):
    """Read arrow table from file path.

    Used in all cases by `ArrowLegacyEngine._read_table`.
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

    if partition_keys:
        tables = []
        with _open_input_files(
            [path],
            fs=fs,
            precache_options=precache_options,
            **open_file_options,
        )[0] as fil:
            for rg in row_groups:
                piece = pq.ParquetDatasetPiece(
                    path,
                    row_group=rg,
                    partition_keys=partition_keys,
                    open_file_func=lambda _path, **_kwargs: fil,
                )
                arrow_table = piece_to_arrow_func(
                    piece, columns, partitions, **read_kwargs
                )
                tables.append(arrow_table)

        if len(row_groups) > 1:
            # NOTE: Not covered by pytest
            return pa.concat_tables(tables)
        else:
            return tables[0]
    else:
        with _open_input_files(
            [path],
            fs=fs,
            precache_options=precache_options,
            **open_file_options,
        )[0] as fil:
            if row_groups == [None]:
                return pq.ParquetFile(fil).read(
                    columns=columns,
                    use_threads=False,
                    use_pandas_metadata=True,
                    **read_kwargs,
                )
            else:
                return pq.ParquetFile(fil).read_row_groups(
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
        split_row_groups=None,
        chunksize=None,
        aggregate_files=None,
        ignore_metadata_file=False,
        metadata_task_size=0,
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
        """Read in a single output partition.

        This method is also used by `ArrowLegacyEngine`.
        """
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
    def _get_dataset_offset(cls, path, fs, append, ignore_divisions):
        fmd = None
        i_offset = 0
        if append:
            # Make sure there are existing file fragments.
            # Otherwise there is no need to set `append=True`
            i_offset = len(
                list(
                    pa_ds.dataset(path, filesystem=fs, format="parquet").get_fragments()
                )
            )
            if i_offset == 0:
                # No dataset to append to
                return fmd, i_offset, False
            try:
                with fs.open(fs.sep.join([path, "_metadata"]), mode="rb") as fil:
                    fmd = pq.read_metadata(fil)
            except OSError:
                # No _metadata file present - No appending allowed (for now)
                if not ignore_divisions:
                    # TODO: Be more flexible about existing metadata.
                    raise NotImplementedError(
                        "_metadata file needed to `append` "
                        "with `engine='pyarrow-dataset'` "
                        "unless `ignore_divisions` is `True`"
                    )
        return fmd, i_offset, append

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

        # Extract metadata and get file offset if appending
        fmd, i_offset, append = cls._get_dataset_offset(
            path, fs, append, ignore_divisions
        )

        # Inspect the intial metadata if appending
        if append:
            arrow_schema = fmd.schema.to_arrow_schema()
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
                        set(dtypes.items()) ^ set(df.dtypes.iteritems())
                    )
                )

            # Check divisions if necessary
            if division_info["name"] not in names:
                ignore_divisions = True
            if not ignore_divisions:
                old_end = None
                row_groups = [fmd.row_group(i) for i in range(fmd.num_row_groups)]
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
                preserve_index,
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
            d = {"meta": _meta}
            if head:
                # Only return schema if this is the "head" partition
                d["schema"] = t.schema
            return [d]
        else:
            return []

    @staticmethod
    def write_metadata(parts, fmd, fs, path, append=False, **kwargs):
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
        kwargs,
    ):
        """pyarrow.dataset version of _collect_dataset_info
        Use pyarrow.dataset API to construct a dictionary of all
        general information needed to read the dataset.

        This method is overriden by `ArrowLegacyEngine`.
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

        # Set require_extension option
        require_extension = _dataset_kwargs.pop(
            "require_extension", (".parq", ".parquet")
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
                if gather_statistics is None:
                    gather_statistics = True
            elif require_extension:
                # Need to materialize all paths if we are missing the _metadata file
                # Raise error if all files have been filtered by extension
                len0 = len(paths)
                paths = [
                    path for path in fs.find(paths) if path.endswith(require_extension)
                ]
                if len0 and paths == []:
                    raise ValueError(
                        "No files satisfy the `require_extension` criteria "
                        f"(files must end with {require_extension})."
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
                    if gather_statistics is None:
                        gather_statistics = True

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

        # At this point, we know if `split_row_groups` should be
        # set to `True` by default.  If the user has not specified
        # this option, we will only collect statistics if there is
        # a global "_metadata" file available, otherwise we will
        # opt for `gather_statistics=False`. For `ArrowDatasetEngine`,
        # statistics are only required to calculate divisions
        # and/or aggregate row-groups using `chunksize` (not for
        # filtering).
        #
        # By default, we will create an output partition for each
        # row group in the dataset (`split_row_groups=True`).
        # However, we will NOT split by row-group if
        # `gather_statistics=False`, because this can be
        # interpreted as an indication that metadata overhead should
        # be avoided at all costs.
        if gather_statistics is None:
            gather_statistics = False
        if split_row_groups is None:
            if gather_statistics:
                split_row_groups = True
            else:
                split_row_groups = False

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

        # Construct and return `datset_info`
        #
        # Note on (hive) partitioning information:
        #
        #    - "partitions" : (list of PartitionObj) This is a list of
        #          simple objects providing `name` and `keys` attributes
        #          for each partition column. The list is designed to
        #          "duck type" a `ParquetPartitions` object, so that the
        #          same code path can be used for both legacy and
        #          pyarrow.dataset-based logic.
        #    - "partition_names" : (list)  This is a list containing the
        #          names of partitioned columns.
        #    - "partitioning" : (dict) The `partitioning` options
        #          used for file discovory by pyarrow.
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

        This method is used by both arrow engines.
        """

        # Collect necessary information from dataset_info
        schema = dataset_info["schema"]
        index = dataset_info["index"]
        categories = dataset_info["categories"]
        partition_obj = dataset_info["partitions"]
        partitions = dataset_info["partition_names"]
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
            column_names = dataset_info.get("physical_schema", schema).names
            storage_name_mapping = {k: k for k in column_names}
            column_index_names = [None]
        if index is None and index_names:
            # Pandas metadata has provided the index name for us
            index = index_names

        # Ensure that there is no overlap between partition columns
        # and explicit column storage
        if partitions:
            _partitions = [p for p in partitions if p not in column_names]
            if not _partitions:
                partitions = []
                dataset_info["partitions"] = None
                dataset_info["partition_keys"] = {}
                dataset_info["partition_names"] = partitions
            elif len(_partitions) != len(partitions):
                raise ValueError(
                    "No partition-columns should be written in the \n"
                    "file unless they are ALL written in the file.\n"
                    "columns: {} | partitions: {}".format(column_names, partitions)
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

        This method is overridden in `ArrowLegacyEngine`.
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

        # Cannot gather_statistics if our `metadata` is a list
        # of paths, or if we are building a multiindex (for now).
        # We also don't "need" to gather statistics if we don't
        # want to apply any filters or calculate divisions. Note
        # that the `ArrowDatasetEngine` doesn't even require
        # `gather_statistics=True` for filtering.
        if split_row_groups is None:
            split_row_groups = False
        _need_aggregation_stats = chunksize or (
            int(split_row_groups) > 1 and aggregation_depth
        )
        if len(index_cols) > 1:
            gather_statistics = False
        elif not _need_aggregation_stats and filters is None and len(index_cols) == 0:
            gather_statistics = False

        # Determine which columns need statistics.
        flat_filters = _flatten_filters(filters)
        stat_col_indices = {}
        for i, name in enumerate(schema.names):
            if name in index_cols or name in flat_filters:
                if name in partition_names:
                    # Partition columns wont have statistics
                    continue
                stat_col_indices[name] = i

        # If the user has not specified `gather_statistics`,
        # we will only do so if there are specific columns in
        # need of statistics.
        # NOTE: We cannot change `gather_statistics` from True
        # to False (even if `stat_col_indices` is empty), in
        # case a `chunksize` was specified, and the row-group
        # statistics are needed for part aggregation.
        if gather_statistics is None:
            gather_statistics = bool(stat_col_indices)

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
                                            "min": pd.Timestamp(cmin)
                                            if isinstance(cmin, datetime)
                                            else cmin,
                                            "max": pd.Timestamp(cmax)
                                            if isinstance(cmax, datetime)
                                            else cmax,
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
        """Generate a partition-specific element of `parts`.

        This method is used by both `ArrowDatasetEngine`
        and `ArrowLegacyEngine`.
        """

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
        """Read in a pyarrow table.

        This method is overridden in `ArrowLegacyEngine`.
        """

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
                None,  # partitions,
                [],  # partition_keys,
                cls._parquet_piece_as_arrow,
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


#
#  PyArrow Legacy API [PyArrow<1.0.0]
#


def _get_dataset_object(paths, fs, filters, dataset_kwargs):
    """Generate a ParquetDataset object"""
    kwargs = dataset_kwargs.copy()
    ignore_metadata_file = kwargs.pop("ignore_metadata_file", False)
    if ignore_metadata_file:
        raise ValueError("ignore_metadata_file not supported for ArrowLegacyEngine.")

    if "validate_schema" not in kwargs:
        kwargs["validate_schema"] = False
    if len(paths) > 1:
        # This is a list of files
        paths, base, fns = _sort_and_analyze_paths(paths, fs)
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
        allpaths, base, fns = _sort_and_analyze_paths(allpaths, fs)
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, filters=filters, **kwargs)
    else:
        # This is a single file.  No danger in gathering statistics
        # and/or splitting row-groups without a "_metadata" file
        base = paths[0]
        fns = [None]
        dataset = pq.ParquetDataset(paths[0], filesystem=fs, **kwargs)

    return dataset, base, fns


class ArrowLegacyEngine(ArrowDatasetEngine):

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
        kwargs,
    ):
        """pyarrow-legacy version of _collect_dataset_info
        Use the ParquetDataset API to construct a dictionary of all
        general information needed to read the dataset.

        This method overrides `ArrowDatasetEngine._collect_dataset_info`.
        """

        if ignore_metadata_file:
            raise ValueError("ignore_metadata_file not supported in ArrowLegacyEngine")

        if metadata_task_size:
            raise ValueError("metadata_task_size not supported in ArrowLegacyEngine")

        # Extract "supported" key-word arguments from `kwargs`
        (
            dataset_kwargs,
            read_kwargs,
            user_kwargs,
        ) = _split_user_options(**kwargs)

        (
            schema,
            metadata,
            base,
            partition_info,
            split_row_groups,
            gather_statistics,
        ) = cls._gather_metadata(
            paths,
            fs,
            split_row_groups,
            gather_statistics,
            filters,
            index,
            dataset_kwargs,
        )

        # Check the `aggregate_files` setting
        aggregation_depth = _get_aggregation_depth(
            aggregate_files,
            partition_info["partition_names"],
        )

        return {
            "schema": schema,
            "metadata": metadata,
            "fs": fs,
            "base_path": base,
            "gather_statistics": gather_statistics,
            "categories": categories,
            "index": index,
            "filters": filters,
            "split_row_groups": split_row_groups,
            "chunksize": chunksize,
            "aggregate_files": aggregate_files,
            "aggregation_depth": aggregation_depth,
            "partition_keys": partition_info["partition_keys"],
            "partition_names": partition_info["partition_names"],
            "partitions": partition_info["partitions"],
            "kwargs": {
                "dataset": dataset_kwargs,
                "read": read_kwargs,
                **user_kwargs,
            },
        }

    @classmethod
    def _construct_collection_plan(cls, dataset_info):
        """pyarrow-legacy version of _construct_collection_plan

        This method overrides the `ArrowDatasetEngine` implementation.
        """

        # Wrap legacy `_construct_parts` implementation
        return cls._construct_parts(
            dataset_info["fs"],
            dataset_info["metadata"],
            dataset_info["schema"],
            dataset_info["filters"],
            dataset_info["index_cols"],
            dataset_info["base_path"],
            {
                "partition_keys": dataset_info["partition_keys"],
                "partition_names": dataset_info["partition_names"],
                "partitions": dataset_info["partitions"],
            },
            dataset_info["categories"],
            dataset_info["split_row_groups"],
            dataset_info["gather_statistics"],
            dataset_info["chunksize"],
            dataset_info["aggregation_depth"],
            dataset_info["kwargs"],
        )

    @classmethod
    def _gather_metadata(
        cls,
        paths,
        fs,
        split_row_groups,
        gather_statistics,
        filters,
        index,
        dataset_kwargs,
    ):
        """Gather parquet metadata into a single data structure.

        Use _metadata or aggregate footer metadata into a single
        object.  Also, collect other information necessary for
        parquet-to-ddf mapping (e.g. schema, partition_info).

        This method overrides `ArrowDatasetEngine._gather_metadata`.
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
        partition_info = {
            "partitions": None,
            "partition_keys": {},
            "partition_names": [],
        }
        # The `partition_info` dict summarizes information needed to handle
        # nested-directory (hive) partitioning.
        #
        #    - "partitions" : (ParquetPartitions) PyArrow-specific  object
        #          needed to read in each partition correctly
        #    - "partition_keys" : (dict) The keys and values correspond to
        #          file paths and partition values, respectively. The partition
        #          values (or partition "keys") will be represented as a list
        #          of tuples. E.g. `[("year", 2020), ("state", "CA")]`
        #    - "partition_names" : (list)  This is a list containing the names
        #          of partitioned columns.  This list must be ordered correctly
        #          by partition level.
        fn_partitioned = False
        if dataset.partitions is not None:
            fn_partitioned = True
            partition_info["partition_names"] = [
                n.name for n in list(dataset.partitions) if n.name is not None
            ]
            partition_info["partitions"] = dataset.partitions
            for piece in dataset.pieces:
                partition_info["partition_keys"][piece.path] = piece.partition_keys

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
            if len(dataset.pieces) > 1:
                # Perform metadata collection in parallel.
                metadata = create_metadata_file(
                    [p.path for p in dataset.pieces],
                    root_dir=base,
                    engine=cls,
                    out_dir=False,
                    fs=fs,
                )
                if schema is None:
                    schema = metadata.schema.to_arrow_schema()
            else:
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

    @classmethod
    def _construct_parts(
        cls,
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
        chunksize,
        aggregation_depth,
        kwargs,
    ):
        """Construct ``parts`` for ddf construction

        Use metadata (along with other data) to define a tuple
        for each ddf partition.  Also gather statistics if
        ``gather_statistics=True``, and other criteria is met.

        This method is only used by `ArrowLegacyEngine`.
        """

        partition_keys = partition_info["partition_keys"]
        partition_obj = partition_info["partitions"]

        # Check if `metadata` is just a list of paths
        # (not splitting by row-group or collecting statistics)
        if (
            isinstance(metadata, list)
            and len(metadata)
            and isinstance(metadata[0], str)
        ):
            parts = []
            stats = []
            for full_path in metadata:
                part = {"piece": (full_path, None, partition_keys.get(full_path, None))}
                parts.append(part)
            common_kwargs = {
                "partitions": partition_obj,
                "categories": categories,
                **kwargs,
            }
            return parts, stats, common_kwargs

        # Use final metadata info to update our options for
        # `parts`/`stats` construnction
        (
            gather_statistics,
            split_row_groups,
            stat_col_indices,
        ) = cls._update_metadata_options(
            gather_statistics,
            split_row_groups,
            metadata,
            schema,
            index_cols,
            filters,
            partition_info,
            chunksize,
            aggregation_depth,
        )

        # Convert metadata into `parts` and `stats`
        return cls._process_metadata(
            metadata,
            schema,
            split_row_groups,
            gather_statistics,
            stat_col_indices,
            filters,
            categories,
            partition_info,
            data_path,
            fs,
            chunksize,
            aggregation_depth,
            kwargs,
        )

    @classmethod
    def _update_metadata_options(
        cls,
        gather_statistics,
        split_row_groups,
        metadata,
        schema,
        index_cols,
        filters,
        partition_info,
        chunksize,
        aggregation_depth,
    ):
        """Update read_parquet options given up-to-data metadata.

        The primary focus here is `gather_statistics`. We want to
        avoid setting this option to `True` if it is unnecessary.

        This method is only used by `ArrowLegacyEngine`.
        """

        # Cannot gather_statistics if our `metadata` is a list
        # of paths, or if we are building a multiindex (for now).
        # We also don't "need" to gather statistics if we don't
        # want to apply any filters or calculate divisions. Note
        # that the `ArrowDatasetEngine` doesn't even require
        # `gather_statistics=True` for filtering.
        if split_row_groups is None:
            split_row_groups = False
        _need_aggregation_stats = chunksize or (
            int(split_row_groups) > 1 and aggregation_depth
        )
        if (
            isinstance(metadata, list)
            and len(metadata)
            and isinstance(metadata[0], str)
        ) or len(index_cols) > 1:
            gather_statistics = False
        elif not _need_aggregation_stats and filters is None and len(index_cols) == 0:
            gather_statistics = False

        # Determine which columns need statistics.
        flat_filters = _flatten_filters(filters)
        stat_col_indices = {}
        for i, name in enumerate(schema.names):
            if name in index_cols or name in flat_filters:
                if name in partition_info["partition_names"]:
                    # Partition columns wont have statistics
                    continue
                stat_col_indices[name] = i

        # If the user has not specified `gather_statistics`,
        # we will only do so if there are specific columns in
        # need of statistics.
        # NOTE: We cannot change `gather_statistics` from True
        # to False (even if `stat_col_indices` is empty), in
        # case a `chunksize` was specified, and the row-group
        # statistics are needed for part aggregation.
        if gather_statistics is None:
            gather_statistics = bool(stat_col_indices)

        return (
            gather_statistics,
            split_row_groups,
            stat_col_indices,
        )

    @classmethod
    def _organize_row_groups(
        cls,
        metadata,
        split_row_groups,
        gather_statistics,
        stat_col_indices,
        filters,
        chunksize,
        aggregation_depth,
    ):
        """Organize row-groups by file.

        This method is used by ArrowLegacyEngine._process_metadata
        """

        sorted_row_group_indices = range(metadata.num_row_groups)
        if aggregation_depth:
            sorted_row_group_indices = sorted(
                range(metadata.num_row_groups),
                key=lambda x: metadata.row_group(x).column(0).file_path,
            )

        # Get the number of row groups per file
        single_rg_parts = int(split_row_groups) == 1
        file_row_groups = defaultdict(list)
        file_row_group_stats = defaultdict(list)
        file_row_group_column_stats = defaultdict(list)
        cmax_last = {}
        for rg in sorted_row_group_indices:
            row_group = metadata.row_group(rg)

            # NOTE: Here we assume that all column chunks are stored
            # in the same file. This is not strictly required by the
            # parquet spec.
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
                            to_ts = column.statistics.logical_type.type == "TIMESTAMP"
                            s["columns"].append(
                                {
                                    "name": name,
                                    "min": cmin if not to_ts else pd.Timestamp(cmin),
                                    "max": cmax if not to_ts else pd.Timestamp(cmax),
                                }
                            )
                        else:
                            cstats += [cmin, cmax]
                        cmax_last[name] = cmax
                    else:

                        if (
                            not (filters or chunksize or aggregation_depth)
                            and column.num_values > 0
                        ):
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

    @classmethod
    def _process_metadata(
        cls,
        metadata,
        schema,
        split_row_groups,
        gather_statistics,
        stat_col_indices,
        filters,
        categories,
        partition_info,
        data_path,
        fs,
        chunksize,
        aggregation_depth,
        kwargs,
    ):
        """Process row-groups and statistics.

        This method is only used by `ArrowLegacyEngine`.
        """

        # Organize row-groups by file
        (
            file_row_groups,
            file_row_group_stats,
            file_row_group_column_stats,
            gather_statistics,
        ) = cls._organize_row_groups(
            metadata,
            split_row_groups,
            gather_statistics,
            stat_col_indices,
            filters,
            chunksize,
            aggregation_depth,
        )

        # Convert organized row-groups to parts
        parts, stats = _row_groups_to_parts(
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
                "partition_keys": partition_info.get("partition_keys", None),
                "partition_obj": partition_info.get("partitions", None),
                "data_path": data_path,
            },
        )

        # Add common kwargs
        common_kwargs = {
            "partitions": partition_info["partitions"],
            "categories": categories,
            "filters": filters,
            **kwargs,
        }

        return parts, stats, common_kwargs

    @classmethod
    def _read_table(
        cls,
        path,
        fs,
        row_groups,
        columns,
        schema,
        filters,
        partitions,
        partition_keys,
        **kwargs,
    ):
        """Read in a pyarrow table.

        This method is overrides the `ArrowDatasetEngine` implementation.
        """

        return _read_table_from_path(
            path,
            fs,
            row_groups,
            columns,
            schema,
            filters,
            partitions,
            partition_keys,
            cls._parquet_piece_as_arrow,
            **kwargs,
        )

    @classmethod
    def multi_support(cls):
        return cls == ArrowLegacyEngine

    @classmethod
    def _get_dataset_offset(cls, path, fs, append, ignore_divisions):
        dataset = fmd = None
        i_offset = 0
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
                        "with `engine='pyarrow-legacy'` "
                        "unless `ignore_divisions` is `True`"
                    )
                fmd = dataset.metadata
                i_offset = len(dataset.pieces)
            except (OSError, ValueError, IndexError):
                # Original dataset does not exist - cannot append
                append = False
        return fmd, i_offset, append


# Compatibility access to legacy ArrowEngine
# (now called `ArrowLegacyEngine`)
ArrowEngine = ArrowLegacyEngine
