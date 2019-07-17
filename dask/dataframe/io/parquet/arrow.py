import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ....bytes.core import get_pyarrow_filesystem
from ....utils import natural_sort_key, getargspec
from ..utils import _get_pyarrow_dtypes, _meta_from_dtypes
from ...utils import clear_known_categories

from .utils import _parse_pandas_metadata, _normalize_index_columns, Engine


def _get_md_row_groups(pieces):
    """ Read file-footer metadata from each individual piece.

    Since this operation can be p[ainfully slow in some cases, abort
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


class ArrowEngine(Engine):
    @staticmethod
    def read_partition(fs, piece, columns, index, **kwargs):
        """ Read a single piece of a Parquet dataset into a Pandas DataFrame

        This function is called many times in individual tasks

        Parameters
        ----------
        fs: FileSystem
        piece: object
            This is some token that is returned by Engine.read_metadata.
            Typically it represents a row group in a Parquet dataset
        columns: List[str]
            List of column names to pull out of that row group
        index: str, List[str], or False
            The index name(s).
        **kwargs:
            Includes `"kwargs"` values stored within the `parts` output
            of `pyarrow.read_metadata`: `partitions` and `categories`.

        Returns
        -------
        A Pandas DataFrame
        """
        partitions = kwargs["partitions"]
        categories = kwargs["categories"]
        if isinstance(index, list):
            columns += index
        with fs.open(piece.path, mode="rb") as f:
            table = piece.read(
                columns=columns,
                partitions=partitions,
                use_pandas_metadata=True,
                file=f,
                use_threads=False,
            )
        df = table.to_pandas(
            categories=categories, use_threads=False, ignore_metadata=True
        )[list(columns)]

        if index:
            df = df.set_index(index)
        return df

    @staticmethod
    def read_metadata(
        fs, paths, categories=None, index=None, gather_statistics=None, **kwargs
    ):
        """ Gather metadata about a Parquet Dataset to prepare for a read

        Parameters
        ----------
        fs: FileSystem
        paths: List[str]
            A list of paths to files (or their equivalents)
        categories: list, dict or None
            Column(s) containing categorical data.
        index: str, List[str], or False
            The column name(s) to be used as the index.
            If set to ``None``, pandas metadata (if available) can be used
            to reset the value in this function
        gather_statistics: bool
            Whether or not to gather statistics data.  If ``None``, we only
            gather statistics data if there is a _metadata file available to
            query (cheaply)

        Returns
        -------
        meta: pandas.DataFrame
            An empty DataFrame object to use for metadata.
            Should have appropriate column names and dtypes but need not have
            any actual data
        statistics: Optional[List[Dict]]
            Either none, if no statistics were found, or a list of dictionaries
            of statistics data, one dict for every partition (see the next
            return value).  The statistics should look like the following:

            [
                {'num-rows': 1000, 'columns': [
                    {'name': 'id', 'min': 0, 'max': 100, 'null-count': 0},
                    {'name': 'x', 'min': 0.0, 'max': 1.0, 'null-count': 5},
                    ]},
                ...
            ]
        parts: List[object]
            A list of row-group-describing dictionary objects to be passed to
            ``pyarrow.read_partition``.
        """
        dataset = pq.ParquetDataset(paths, filesystem=get_pyarrow_filesystem(fs))

        if dataset.partitions is not None:
            partitions = [
                n for n in dataset.partitions.partition_names if n is not None
            ]
        else:
            partitions = []

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
        meta = _meta_from_dtypes(
            all_columns,
            dtypes,
            index_cols=index_cols,
            column_index_names=column_index_names,
        )

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
            for row_group in row_groups:
                s = {"num-rows": row_group.num_rows, "columns": []}
                for i, name in enumerate(names):
                    column = row_group.column(i)
                    d = {"name": name}
                    if column.statistics:
                        cs_min = column.statistics.min
                        cs_max = column.statistics.max
                        d.update(
                            {
                                "min": cs_min,
                                "max": cs_max,
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

        # Create `parts` (list of row-group-descriptor dicts)
        parts = [
            {
                "piece": piece,
                "kwargs": {"partitions": dataset.partitions, "categories": categories},
            }
            for piece in pieces
        ]

        return (meta, stats, parts)

    @staticmethod
    def initialize_write(
        df,
        fs,
        path,
        append=False,
        partition_on=None,
        ignore_divisions=False,
        division_info=None,
        **kwargs
    ):
        """Perform engine-specific initialization steps for this dataset

        Parameters
        ----------
        df: dask.dataframe.DataFrame
        fs: FileSystem
        path : string
            Destination directory for data.  Prepend with protocol like ``s3://``
            or ``hdfs://`` for remote data.
        append: bool
            If True, may use existing metadata (if any) and perform checks
            against the new data being stored.
        partition_on: List(str)
            Column(s) to use for dataset partitioning in parquet.
        ignore_divisions: bool
            Whether or not to ignore old divisions when appending.  Otherwise,
            overlapping divisions will lead to an error being raised.
        division_info: dict
            Dictionary containing the divisions and corresponding column name.
        **kwargs: Ignored

        Returns
        -------
        fmd: Parquet-file metadata object
        i_offset: int
            Initial offset to user for new-file labeling
        """
        dataset = fmd = None
        i_offset = 0
        if append and division_info is None:
            ignore_divisions = True
        fs.mkdirs(path)

        if append:
            try:
                # Allow append if the dataset exists.
                # Also need dataset.metadata object if
                # ignore_divisions is False (to check divisions)
                dataset = pq.ParquetDataset(path, filesystem=get_pyarrow_filesystem(fs))
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

    @staticmethod
    def write_partition(
        df,
        path,
        fs,
        filename,
        partition_on,
        fmd=None,
        compression=None,
        return_metadata=True,
        with_metadata=True,
        index_cols=[],
        **kwargs
    ):
        md_list = []
        preserve_index = False
        if index_cols:
            df = df.set_index(index_cols)
            preserve_index = True
        t = pa.Table.from_pandas(df, preserve_index=preserve_index)
        if partition_on:
            pq.write_to_dataset(
                t,
                path,
                partition_cols=partition_on,
                filesystem=fs,
                metadata_collector=md_list,
                **kwargs
            )
        else:
            with fs.open(fs.sep.join([path, filename]), "wb") as fil:
                pq.write_table(
                    t,
                    fil,
                    compression=compression,
                    metadata_collector=md_list,
                    **kwargs
                )
        # Return the schema needed to write the metadata
        if return_metadata:
            return [{"schema": t.schema, "meta": md_list[0]}]
        else:
            return []
