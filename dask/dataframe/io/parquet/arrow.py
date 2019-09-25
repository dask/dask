from functools import partial
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
    for piece in pieces:
        for rg in range(piece.get_metadata().num_row_groups):
            row_group = piece.get_metadata().row_group(rg)
            for c in range(row_group.num_columns):
                if not row_group.column(c).statistics:
                    return []
            row_groups.append(row_group)
    return row_groups


def _determine_dataset_parts(fs, paths, gather_statistics, filters, dataset_kwargs):
    """ Determine how to access metadata and break read into ``parts``

    This logic is mostly to handle `gather_statistics=False` cases,
    because this also means we should avoid scanning every file in the
    dataset.
    """
    parts = []
    if len(paths) > 1:
        if gather_statistics is not False:
            # This scans all the files
            dataset = pq.ParquetDataset(
                paths, filesystem=fs, filters=filters, **dataset_kwargs
            )
        else:
            base, fns = _analyze_paths(paths, fs)
            relpaths = [path.replace(base, "").lstrip("/") for path in paths]
            if "_metadata" in relpaths:
                # We have a _metadata file, lets use it
                dataset = pq.ParquetDataset(
                    base + fs.sep + "_metadata",
                    filesystem=fs,
                    filters=filters,
                    **dataset_kwargs,
                )
            else:
                # Rely on metadata for 0th file.
                # Will need to pass a list of paths to read_partition
                dataset = pq.ParquetDataset(paths[0], filesystem=fs, **dataset_kwargs)
                parts = [base + fs.sep + fn for fn in fns]
    else:
        if fs.isdir(paths[0]):
            # This is a directory, check for _metadata, then _common_metadata
            allpaths = fs.glob(paths[0] + fs.sep + "*")
            base, fns = _analyze_paths(allpaths, fs)
            relpaths = [path.replace(base, "").lstrip("/") for path in allpaths]
            if "_metadata" in relpaths and "validate_schema" not in dataset_kwargs:
                dataset_kwargs["validate_schema"] = False
            if "_metadata" in relpaths or gather_statistics is not False:
                # Let arrow do its thing (use _metadata or scan files)
                dataset = pq.ParquetDataset(
                    paths, filesystem=fs, filters=filters, **dataset_kwargs
                )
            else:
                # Use _common_metadata file if it is available.
                # Otherwise, just use 0th file
                if "_common_metadata" in relpaths:
                    dataset = pq.ParquetDataset(
                        base + fs.sep + "_common_metadata",
                        filesystem=fs,
                        **dataset_kwargs,
                    )
                else:
                    dataset = pq.ParquetDataset(
                        allpaths[0], filesystem=fs, **dataset_kwargs
                    )
                parts = [base + fs.sep + fn for fn in fns]
        else:
            # There is only one file to read
            dataset = pq.ParquetDataset(paths, filesystem=fs, **dataset_kwargs)
    return parts, dataset


class ArrowEngine(Engine):
    @staticmethod
    def read_metadata(
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        **kwargs,
    ):
        # Define the dataset object to use for metadata,
        # Also, initialize `parts`.  If `parts` is populated here,
        # then each part will correspond to a file.  Otherwise, each part will
        # correspond to a row group (populated below)
        parts, dataset = _determine_dataset_parts(
            fs, paths, gather_statistics, filters, kwargs.get("dataset", {})
        )
        if dataset.partitions is not None:
            partitions = [
                n for n in dataset.partitions.partition_names if n is not None
            ]
        else:
            partitions = []

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
            and dataset.metadata.num_row_groups == len(pieces)
        ):
            gather_statistics = True
        if not pieces:
            gather_statistics = False

        if gather_statistics:
            # Read from _metadata file
            if dataset.metadata and dataset.metadata.num_row_groups == len(pieces):
                row_groups = [
                    dataset.metadata.row_group(i)
                    for i in range(dataset.metadata.num_row_groups)
                ]
                names = dataset.metadata.schema.names
            else:
                # Read from each individual piece (quite possibly slow).
                row_groups = _get_md_row_groups(pieces)
                if row_groups:
                    piece = pieces[0]
                    md = piece.get_metadata()
                    names = md.schema.names
                else:
                    gather_statistics = False

        if gather_statistics:
            stats = []
            skip_cols = set()  # Columns with min/max = None detected
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
            parts = [
                (piece.path, piece.row_group, piece.partition_keys) for piece in pieces
            ]
        parts = [
            {
                "piece": piece,
                "kwargs": {"partitions": dataset.partitions, "categories": categories},
            }
            for piece in parts
        ]

        return (meta, stats, parts)

    @staticmethod
    def read_partition(
        fs, piece, columns, index, categories=(), partitions=(), **kwargs
    ):
        if isinstance(index, list):
            columns += index
        if isinstance(piece, str):
            # `piece` is a file-path string
            piece = pq.ParquetDatasetPiece(
                piece, open_file_func=partial(fs.open, mode="rb")
            )
        else:
            # `piece` contains (path, row_group, partition_keys)
            piece = pq.ParquetDatasetPiece(
                piece[0],
                row_group=piece[1],
                partition_keys=piece[2],
                open_file_func=partial(fs.open, mode="rb"),
            )
        df = piece.read(
            columns=columns,
            partitions=partitions,
            use_pandas_metadata=True,
            use_threads=False,
            **kwargs.get("read", {}),
        ).to_pandas(categories=categories, use_threads=False, ignore_metadata=True)[
            list(columns)
        ]

        if index:
            df = df.set_index(index)
        return df

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
        md_list = []
        preserve_index = False
        if index_cols:
            df = df.set_index(index_cols)
            preserve_index = True
        t = pa.Table.from_pandas(df, preserve_index=preserve_index, schema=schema)
        if partition_on:
            pq.write_to_dataset(
                t,
                path,
                partition_cols=partition_on,
                filesystem=fs,
                metadata_collector=md_list,
                **kwargs,
            )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                pq.write_table(
                    t,
                    fil,
                    compression=compression,
                    metadata_collector=md_list,
                    **kwargs,
                )
        # Return the schema needed to write the metadata
        if return_metadata:
            return [{"schema": t.schema, "meta": md_list[0]}]
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
