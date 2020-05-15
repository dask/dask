from functools import partial
from collections import OrderedDict
import json
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.compat import guid
from ....utils import natural_sort_key, getargspec
from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes
from ...utils import clear_known_categories

from .utils import (
    _parse_pandas_metadata,
    _normalize_index_columns,
    Engine,
    _analyze_paths,
)


def _get_md_row_groups(pieces):
    """ Read file-footer metadata from each individual piece.

    Since this operation can be painfully slow in some cases, abort
    if any metadata or statistics are missing
    """
    row_groups = []
    row_groups_per_piece = []
    for piece in pieces:
        num_row_groups = piece.get_metadata().num_row_groups
        for rg in range(num_row_groups):
            row_group = piece.get_metadata().row_group(rg)
            for c in range(row_group.num_columns):
                if not row_group.column(c).statistics:
                    return (None, None)
            row_groups.append(row_group)
        row_groups_per_piece.append(num_row_groups)
    if len(row_groups) == len(pieces):
        row_groups_per_piece = None
    # TODO: Skip row_groups_per_piece after ARROW-2801
    return row_groups, row_groups_per_piece


def _get_row_groups_per_piece(pieces, metadata, path, fs):
    """ Determine number of row groups in each dataset piece.

    This function requires access to ParquetDataset.metadata
    """
    # TODO: Remove this function after ARROW-2801
    if metadata.num_row_groups == len(pieces):
        return None  # pieces already map to row-groups

    result = OrderedDict()
    for piece in pieces:
        result[piece.path] = 0
    for rg in range(metadata.num_row_groups):
        filename = metadata.row_group(rg).column(0).file_path
        if filename:
            result[fs.sep.join([path, filename])] += 1
        else:
            return None  # File path is missing, abort
    return tuple(result.values())


def _merge_statistics(stats, s):
    """ Update `stats` with vaules in `s`
    """
    stats[-1]["total_byte_size"] += s["total_byte_size"]
    stats[-1]["num-rows"] += s["num-rows"]
    ncols = len(stats[-1]["columns"])
    ncols_n = len(s["columns"])
    if ncols != ncols_n:
        raise ValueError(f"Column count not equal ({ncols} vs {ncols_n})")
    for i in range(ncols):
        name = stats[-1]["columns"][i]["name"]
        j = i
        for ii in range(ncols):
            if name == s["columns"][j]["name"]:
                break
            if ii == ncols - 1:
                raise KeyError(f"Column statistics missing for {name}")
            j = (j + 1) % ncols

        min_n = s["columns"][j]["min"]
        max_n = s["columns"][j]["max"]
        null_count_n = s["columns"][j]["null_count"]

        min_i = stats[-1]["columns"][i]["min"]
        max_i = stats[-1]["columns"][i]["max"]
        stats[-1]["columns"][i]["min"] = min(min_i, min_n)
        stats[-1]["columns"][i]["max"] = max(max_i, max_n)
        stats[-1]["columns"][i]["null_count"] += null_count_n
    return True


class SimplePiece:
    """ SimplePiece
        Surrogate class for PyArrow ParquetDatasetPiece.
        Only used for flat datasets (not partitioned) where
        a "_metadata" file is available.
    """

    def __init__(self, path):
        self.path = path
        self.partition_keys = None
        self.row_group = None


def _determine_dataset_parts(fs, paths, gather_statistics, filters, dataset_kwargs):
    """ Determine how to access metadata and break read into ``parts``

    This logic is mostly to handle `gather_statistics=False` cases,
    because this also means we should avoid scanning every file in the
    dataset.
    """
    parts = []
    if len(paths) > 1:
        base, fns = _analyze_paths(paths, fs)
        if "_metadata" in fns:
            # We have a _metadata file
            # PyArrow cannot handle "_metadata"
            # when `paths` is a list.
            paths.remove(base + fs.sep + "_metadata")
            fns.remove("_metadata")
            if gather_statistics is not False:
                # If we are allowed to gather statistics,
                # lets use "_metadata" instead of opening
                # every file. Note that we don't need to check if
                # the dataset is flat here, because PyArrow cannot
                # properly handle partitioning in this case anyway.
                dataset = pq.ParquetDataset(
                    base + fs.sep + "_metadata",
                    filesystem=fs,
                    filters=filters,
                    **dataset_kwargs,
                )
                dataset.metadata = dataset.pieces[0].get_metadata()
                dataset.pieces = [SimplePiece(path) for path in paths]
                dataset.partitions = None
                return parts, dataset
        if gather_statistics is not False:
            # This scans all the files
            dataset = pq.ParquetDataset(
                paths, filesystem=fs, filters=filters, **dataset_kwargs
            )
            if dataset.schema is None:
                # The dataset may have inconsistent schemas between files.
                # If so, we should try to use a "_common_metadata" file
                proxy_path = (
                    base + fs.sep + "_common_metadata"
                    if "_common_metadata" in fns
                    else paths[0]
                )
                dataset.schema = pq.ParquetDataset(proxy_path, filesystem=fs).schema
        else:
            # Rely on schema for 0th file.
            # Will need to pass a list of paths to read_partition
            dataset = pq.ParquetDataset(paths[0], filesystem=fs, **dataset_kwargs)
            parts = [base + fs.sep + fn for fn in fns]
    elif fs.isdir(paths[0]):
        # This is a directory, check for _metadata, then _common_metadata
        allpaths = fs.glob(paths[0] + fs.sep + "*")
        base, fns = _analyze_paths(allpaths, fs)
        # Check if dataset is "not flat" (partitioned into directories).
        # If so, we will need to let pyarrow generate the `dataset` object.
        not_flat = any([fs.isdir(p) for p in fs.glob(fs.sep.join([base, "*"]))])
        if "_metadata" in fns and "validate_schema" not in dataset_kwargs:
            dataset_kwargs["validate_schema"] = False
        if not_flat or "_metadata" in fns or gather_statistics is not False:
            # Let arrow do its thing (use _metadata or scan files)
            dataset = pq.ParquetDataset(
                paths, filesystem=fs, filters=filters, **dataset_kwargs
            )
            if dataset.schema is None:
                # The dataset may have inconsistent schemas between files.
                # If so, we should try to use a "_common_metadata" file
                proxy_path = (
                    base + fs.sep + "_common_metadata"
                    if "_common_metadata" in fns
                    else allpaths[0]
                )
                dataset.schema = pq.ParquetDataset(proxy_path, filesystem=fs).schema
        else:
            # Use _common_metadata file if it is available.
            # Otherwise, just use 0th file
            if "_common_metadata" in fns:
                dataset = pq.ParquetDataset(
                    base + fs.sep + "_common_metadata", filesystem=fs, **dataset_kwargs
                )
            else:
                dataset = pq.ParquetDataset(
                    allpaths[0], filesystem=fs, **dataset_kwargs
                )
            parts = [base + fs.sep + fn for fn in fns if fn != "_common_metadata"]
    else:
        # There is only one file to read
        dataset = pq.ParquetDataset(paths, filesystem=fs, **dataset_kwargs)
    return parts, dataset


def _write_partitioned(
    table, root_path, partition_cols, fs, preserve_index=True, **kwargs
):
    """ Write table to a partitioned dataset with pyarrow.

        Logic copied from pyarrow.parquet.
        (arrow/python/pyarrow/parquet.py::write_to_dataset)

        TODO: Remove this in favor of pyarrow's `write_to_dataset`
              once ARROW-8244 is addressed.
    """
    fs.mkdirs(root_path, exist_ok=True)

    df = table.to_pandas(ignore_metadata=True)
    partition_keys = [df[col] for col in partition_cols]
    data_df = df.drop(partition_cols, axis="columns")
    data_cols = df.columns.drop(partition_cols)
    if len(data_cols) == 0 and not preserve_index:
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
            subgroup, preserve_index=False, schema=subschema, safe=False
        )
        prefix = fs.sep.join([root_path, subdir])
        fs.mkdir(prefix, exists_ok=True)
        outfile = guid() + ".parquet"
        full_path = fs.sep.join([prefix, outfile])
        with fs.open(full_path, "wb") as f:
            pq.write_table(subtable, f, metadata_collector=md_list, **kwargs)
        md_list[-1].set_file_path(fs.sep.join([subdir, outfile]))

    return md_list


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
        split_row_groups=True,
        **kwargs,
    ):
        # Define the dataset object to use for metadata,
        # Also, initialize `parts`.  If `parts` is populated here,
        # then each part will correspond to a file.  Otherwise, each part will
        # correspond to a row group (populated below)
        parts, dataset = _determine_dataset_parts(
            fs, paths, gather_statistics, filters, kwargs.get("dataset", {})
        )
        # Check if the column-chunk file_path's are set in "_metadata".
        # If available, we can use the path to sort the row-groups
        col_chunk_paths = False
        if dataset.metadata:
            col_chunk_paths = all(
                dataset.metadata.row_group(i).column(0).file_path is not None
                for i in range(dataset.metadata.num_row_groups)
            )

        # TODO: Call to `_determine_dataset_parts` uses `pq.ParquetDataset`
        # to define the `dataset` object. `split_row_groups` should be passed
        # to that constructor once it is supported (see ARROW-2801).
        if dataset.partitions is not None:
            partitions = [
                n for n in dataset.partitions.partition_names if n is not None
            ]
            if partitions and dataset.metadata:
                # Dont use dataset.metadata for partitioned datasets, unless
                # the column-chunk metadata includes the `"file_path"`.
                # The order of dataset.metadata.row_group items is often
                # different than the order of `dataset.pieces`.
                if not col_chunk_paths or (
                    len(dataset.pieces) != dataset.metadata.num_row_groups
                ):
                    dataset.schema = dataset.metadata.schema
                    dataset.metadata = None
        else:
            partitions = []

        # Statistics are currently collected at the row-group level only.
        # Therefore, we cannot perform filtering with split_row_groups=False.
        # For "partitioned" datasets, each file (usually) corresponds to a
        # row-group anyway.
        # TODO: Map row-group statistics onto file pieces for filtering.
        #       This shouldn't be difficult if `col_chunk_paths==True`
        if not split_row_groups and not col_chunk_paths:
            if gather_statistics is None and not partitions:
                gather_statistics = False
                if filters:
                    raise ValueError(
                        "Filters not supported with split_row_groups=False "
                        "(unless proper _metadata is available)."
                    )
            if gather_statistics and not partitions:
                raise ValueError(
                    "Statistics not supported with split_row_groups=False."
                    "(unless proper _metadata is available)."
                )

        if dataset.metadata:
            schema = dataset.metadata.schema.to_arrow_schema()
        else:
            schema = dataset.schema.to_arrow_schema()
        columns = None

        has_pandas_metadata = (
            schema.metadata is not None and b"pandas" in schema.metadata
        )

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

        pieces = sorted(dataset.pieces, key=lambda piece: natural_sort_key(piece.path))

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
        if (
            gather_statistics is None
            and dataset.metadata
            and dataset.metadata.num_row_groups >= len(pieces)
        ):
            gather_statistics = True
        if not pieces:
            gather_statistics = False

        if filters:
            # Filters may require us to gather statistics
            if gather_statistics is False and partitions:
                warnings.warn(
                    "Filtering with gather_statistics=False. "
                    "Only partition columns will be filtered correctly."
                )
            elif gather_statistics is False:
                raise ValueError("Cannot apply filters with gather_statistics=False")
            elif not gather_statistics:
                gather_statistics = True

        row_groups_per_piece = None
        if gather_statistics:
            # Read from _metadata file
            if dataset.metadata and dataset.metadata.num_row_groups >= len(pieces):
                row_groups = [
                    dataset.metadata.row_group(i)
                    for i in range(dataset.metadata.num_row_groups)
                ]

                # Re-order row-groups by path name if known
                if col_chunk_paths:
                    row_groups = sorted(
                        row_groups,
                        key=lambda row_group: natural_sort_key(
                            row_group.column(0).file_path
                        ),
                    )

                if split_row_groups and len(dataset.paths) == 1:
                    row_groups_per_piece = _get_row_groups_per_piece(
                        pieces, dataset.metadata, dataset.paths[0], fs
                    )
                names = dataset.metadata.schema.names
            else:
                # Read from each individual piece (quite possibly slow).
                row_groups, row_groups_per_piece = _get_md_row_groups(pieces)
                if row_groups:
                    piece = pieces[0]
                    md = piece.get_metadata()
                    names = md.schema.names
                else:
                    gather_statistics = False

        if gather_statistics:
            stats = []
            skip_cols = set()  # Columns with min/max = None detected
            path_last = None
            for ri, row_group in enumerate(row_groups):
                s = {"num-rows": row_group.num_rows, "columns": []}
                for i, name in enumerate(names):
                    if name not in skip_cols:
                        column = row_group.column(i)
                        d = {"name": name}
                        if column.statistics:
                            cs_min = column.statistics.min
                            cs_max = column.statistics.max
                            if not column.statistics.has_min_max:
                                cs_min, cs_max = None, None
                            if None in [cs_min, cs_max] and ri == 0:
                                skip_cols.add(name)
                                continue
                            cs_vals = pd.Series([cs_min, cs_max])
                            d.update(
                                {
                                    "min": cs_vals[0],
                                    "max": cs_vals[1],
                                    "null_count": column.statistics.null_count,
                                }
                            )
                        s["columns"].append(d)
                s["total_byte_size"] = row_group.total_byte_size
                if col_chunk_paths:
                    s["file_path_0"] = row_group.column(0).file_path
                    if not split_row_groups and (s["file_path_0"] == path_last):
                        # Rather than appending a new "row-group", just merge
                        # new `s` statistics into last element of `stats`.
                        # Note that each stats element will now correspond to an
                        # entire file (rather than actual "row-groups")
                        _merge_statistics(stats, s)
                        continue
                    else:
                        path_last = s["file_path_0"]
                stats.append(s)
        else:
            stats = None

        if dataset.partitions:
            for partition in dataset.partitions:
                if isinstance(index, list) and partition.name == index[0]:
                    meta.index = pd.CategoricalIndex(
                        categories=partition.keys, name=index[0]
                    )
                elif partition.name == meta.index.name:
                    meta.index = pd.CategoricalIndex(
                        categories=partition.keys, name=meta.index.name
                    )
                elif partition.name in meta.columns:
                    meta[partition.name] = pd.Categorical(
                        categories=partition.keys, values=[]
                    )

        # Create `parts`
        # This is a list of row-group-descriptor dicts, or file-paths
        # if we have a list of files and gather_statistics=False
        if not parts:
            if split_row_groups and row_groups_per_piece:
                # TODO: This block can be removed after ARROW-2801
                parts = []
                rg_tot = 0
                for i, piece in enumerate(pieces):
                    num_row_groups = row_groups_per_piece[i]
                    for rg in range(num_row_groups):
                        parts.append((piece.path, rg, piece.partition_keys))
                        # Setting file_path here, because it may be
                        # missing from the row-group/column-chunk stats
                        if "file_path_0" not in stats[rg_tot]:
                            stats[rg_tot]["file_path_0"] = piece.path
                        rg_tot += 1
            else:
                parts = [
                    (piece.path, piece.row_group, piece.partition_keys)
                    for piece in pieces
                ]
        parts = [
            {
                "piece": piece,
                "kwargs": {"partitions": dataset.partitions, "categories": categories},
            }
            for piece in parts
        ]

        return (meta, stats, parts)

    @classmethod
    def read_partition(
        cls, fs, piece, columns, index, categories=(), partitions=(), **kwargs
    ):
        if isinstance(index, list):
            for level in index:
                # unclear if we can use set ops here. I think the order matters.
                # Need the membership test to avoid duplicating index when
                # we slice with `columns` later on.
                if level not in columns:
                    columns.append(level)
        if isinstance(piece, str):
            # `piece` is a file-path string
            piece = pq.ParquetDatasetPiece(
                piece, open_file_func=partial(fs.open, mode="rb")
            )
        else:
            # `piece` contains (path, row_group, partition_keys)
            (path, row_group, partition_keys) = piece
            piece = pq.ParquetDatasetPiece(
                path,
                row_group=row_group,
                partition_keys=partition_keys,
                open_file_func=partial(fs.open, mode="rb"),
            )

        # Ensure `columns` and `partitions` do not overlap
        columns_and_parts = columns.copy()
        if columns_and_parts and partitions:
            for part_name in partitions.partition_names:
                if part_name in columns:
                    columns.remove(part_name)
                else:
                    columns_and_parts.append(part_name)
            columns = columns or None

        arrow_table = cls._parquet_piece_as_arrow(piece, columns, partitions, **kwargs)
        df = cls._arrow_table_to_pandas(arrow_table, categories, **kwargs)

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
        **kwargs,
    ):
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

        return fmd, i_offset

    @staticmethod
    def write_partition(
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
        if index_cols:
            df = df.set_index(index_cols)
            preserve_index = True
        t = pa.Table.from_pandas(df, preserve_index=preserve_index, schema=schema)
        if partition_on:
            md_list = _write_partitioned(
                t, path, partition_on, fs, preserve_index=preserve_index, **kwargs
            )
            if md_list:
                _meta = md_list[0]
                for i in range(1, len(md_list)):
                    _meta.append_row_groups(md_list[i])
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
                _meta.append_row_groups(parts[i][0]["meta"])
            with fs.open(metadata_path, "wb") as fil:
                _meta.write_metadata_file(fil)
