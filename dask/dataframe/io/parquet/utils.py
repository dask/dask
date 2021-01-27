import re

import pandas as pd

from ....core import flatten


class Engine:
    """ The API necessary to provide a new Parquet reader/writer """

    @classmethod
    def read_metadata(
        cls,
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        **kwargs
    ):
        """Gather metadata about a Parquet Dataset to prepare for a read

        This function is called once in the user's Python session to gather
        important metadata about the parquet dataset.

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
        filters: list
            List of filters to apply, like ``[('x', '>', 0), ...]``.
        **kwargs: dict (of dicts)
            User-specified arguments to pass on to backend.
            Top level key can be used by engine to select appropriate dict.

        Returns
        -------
        meta: pandas.DataFrame
            An empty DataFrame object to use for metadata.
            Should have appropriate column names and dtypes but need not have
            any actual data
        statistics: Optional[List[Dict]]
            Either None, if no statistics were found, or a list of dictionaries
            of statistics data, one dict for every partition (see the next
            return value).  The statistics should look like the following:

            [
                {'num-rows': 1000, 'columns': [
                    {'name': 'id', 'min': 0, 'max': 100},
                    {'name': 'x', 'min': 0.0, 'max': 1.0},
                    ]},
                ...
            ]
        parts: List[object]
            A list of objects to be passed to ``Engine.read_partition``.
            Each object should represent a piece of data (usually a row-group).
            The type of each object can be anything, as long as the
            engine's read_partition function knows how to interpret it.
        """
        raise NotImplementedError()

    @classmethod
    def read_partition(cls, fs, piece, columns, index, **kwargs):
        """Read a single piece of a Parquet dataset into a Pandas DataFrame

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
            of `engine.read_metadata`. May also include arguments to be
            passed to the backend (if stored under a top-level `"read"` key).

        Returns
        -------
        A Pandas DataFrame
        """
        raise NotImplementedError()

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
        **kwargs
    ):
        """Perform engine-specific initialization steps for this dataset

        Parameters
        ----------
        df: dask.dataframe.DataFrame
        fs: FileSystem
        path: str
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
        **kwargs: dict
            Other keyword arguments (including `index_cols`)

        Returns
        -------
        tuple:
            engine-specific instance
            list of filenames, one per partition
        """
        raise NotImplementedError

    @classmethod
    def write_partition(
        cls, df, path, fs, filename, partition_on, return_metadata, **kwargs
    ):
        """
        Output a partition of a dask.DataFrame. This will correspond to
        one output file, unless partition_on is set, in which case, it will
        correspond to up to one file in each sub-directory.

        Parameters
        ----------
        df: dask.dataframe.DataFrame
        path: str
            Destination directory for data.  Prepend with protocol like ``s3://``
            or ``hdfs://`` for remote data.
        fs: FileSystem
        filename: str
        partition_on: List(str)
            Column(s) to use for dataset partitioning in parquet.
        return_metadata : bool
            Whether to return list of instances from this write, one for each
            output file. These will be passed to write_metadata if an output
            metadata file is requested.
        **kwargs: dict
            Other keyword arguments (including `fmd` and `index_cols`)

        Returns
        -------
        List of metadata-containing instances (if `return_metadata` is `True`)
        or empty list
        """
        raise NotImplementedError

    @classmethod
    def write_metadata(cls, parts, meta, fs, path, append=False, **kwargs):
        """
        Write the shared metadata file for a parquet dataset.

        Parameters
        ----------
        parts: List
            Contains metadata objects to write, of the type undrestood by the
            specific implementation
        meta: non-chunk metadata
            Details that do not depend on the specifics of each chunk write,
            typically the schema and pandas metadata, in a format the writer
            can use.
        fs: FileSystem
        path: str
            Output file to write to, usually ``"_metadata"`` in the root of
            the output dataset
        append: boolean
            Whether or not to consolidate new metadata with existing (True)
            or start from scratch (False)
        **kwargs: dict
            Other keyword arguments (including `compression`)
        """
        raise NotImplementedError()

    @classmethod
    def collect_file_metadata(cls, path, fs, file_path):
        """
        Collect parquet metadata from a file and set the file_path.

        Parameters
        ----------
        path: str
            Parquet-file path to extract metadata from.
        fs: FileSystem
        file_path: str
            Relative path to set as `file_path` in the metadata.

        Returns
        -------
        A metadata object.  The specific type should be recognized
        by the aggregate_metadata method.
        """
        raise NotImplementedError()

    @classmethod
    def aggregate_metadata(cls, meta_list, fs, out_path):
        """
        Aggregate a list of metadata objects and optionally
        write out the final result as a _metadata file.

        Parameters
        ----------
        meta_list: list
            List of metadata objects to be aggregated into a single
            metadata object, and optionally written to disk. The
            specific element type can be engine specific.
        fs: FileSystem
        out_path: str or None
            Directory to write the final _metadata file. If None
            is specified, the aggregated metadata will be returned,
            and nothing will be written to disk.

        Returns
        -------
        If out_path is None, an aggregate metadata object is returned.
        Otherwise, None is returned.
        """
        raise NotImplementedError()


def _parse_pandas_metadata(pandas_metadata):
    """Get the set of names from the pandas metadata section

    Parameters
    ----------
    pandas_metadata : dict
        Should conform to the pandas parquet metadata spec

    Returns
    -------
    index_names : list
        List of strings indicating the actual index names
    column_names : list
        List of strings indicating the actual column names
    storage_name_mapping : dict
        Pairs of storage names (e.g. the field names for
        PyArrow) and actual names. The storage and field names will
        differ for index names for certain writers (pyarrow > 0.8).
    column_indexes_names : list
        The names for ``df.columns.name`` or ``df.columns.names`` for
        a MultiIndex in the columns

    Notes
    -----
    This should support metadata written by at least

    * fastparquet>=0.1.3
    * pyarrow>=0.7.0
    """
    index_storage_names = [
        n["name"] if isinstance(n, dict) else n
        for n in pandas_metadata["index_columns"]
    ]
    index_name_xpr = re.compile(r"__index_level_\d+__")

    # older metadatas will not have a 'field_name' field so we fall back
    # to the 'name' field
    pairs = [
        (x.get("field_name", x["name"]), x["name"]) for x in pandas_metadata["columns"]
    ]

    # Need to reconcile storage and real names. These will differ for
    # pyarrow, which uses __index_leveL_d__ for the storage name of indexes.
    # The real name may be None (e.g. `df.index.name` is None).
    pairs2 = []
    for storage_name, real_name in pairs:
        if real_name and index_name_xpr.match(real_name):
            real_name = None
        pairs2.append((storage_name, real_name))
    index_names = [name for (storage_name, name) in pairs2 if name != storage_name]

    # column_indexes represents df.columns.name
    # It was added to the spec after pandas 0.21.0+, and implemented
    # in PyArrow 0.8. It was added to fastparquet in 0.3.1.
    column_index_names = pandas_metadata.get("column_indexes", [{"name": None}])
    column_index_names = [x["name"] for x in column_index_names]

    # Now we need to disambiguate between columns and index names. PyArrow
    # 0.8.0+ allows for duplicates between df.index.names and df.columns
    if not index_names:
        # For PyArrow < 0.8, Any fastparquet. This relies on the facts that
        # 1. Those versions used the real index name as the index storage name
        # 2. Those versions did not allow for duplicate index / column names
        # So we know that if a name is in index_storage_names, it must be an
        # index name
        if index_storage_names and isinstance(index_storage_names[0], dict):
            # Cannot handle dictionary case
            index_storage_names = []
        index_names = list(index_storage_names)  # make a copy
        index_storage_names2 = set(index_storage_names)
        column_names = [
            name for (storage_name, name) in pairs if name not in index_storage_names2
        ]
    else:
        # For newer PyArrows the storage names differ from the index names
        # iff it's an index level. Though this is a fragile assumption for
        # other systems...
        column_names = [name for (storage_name, name) in pairs2 if name == storage_name]

    storage_name_mapping = dict(pairs2)  # TODO: handle duplicates gracefully

    return index_names, column_names, storage_name_mapping, column_index_names


def _normalize_index_columns(user_columns, data_columns, user_index, data_index):
    """Normalize user and file-provided column and index names

    Parameters
    ----------
    user_columns : None, str or list of str
    data_columns : list of str
    user_index : None, str, or list of str
    data_index : list of str

    Returns
    -------
    column_names : list of str
    index_names : list of str
    """
    specified_columns = user_columns is not None
    specified_index = user_index is not None

    if user_columns is None:
        user_columns = list(data_columns)
    elif isinstance(user_columns, str):
        user_columns = [user_columns]
    else:
        user_columns = list(user_columns)

    if user_index is None:
        user_index = data_index
    elif user_index is False:
        # When index is False, use no index and all fields should be treated as
        # columns (unless `columns` provided).
        user_index = []
        data_columns = data_index + data_columns
    elif isinstance(user_index, str):
        user_index = [user_index]
    else:
        user_index = list(user_index)

    if specified_index and not specified_columns:
        # Only `index` provided. Use specified index, and all column fields
        # that weren't specified as indices
        index_names = user_index
        column_names = [x for x in data_columns if x not in index_names]
    elif specified_columns and not specified_index:
        # Only `columns` provided. Use specified columns, and all index fields
        # that weren't specified as columns
        column_names = user_columns
        index_names = [x for x in data_index if x not in column_names]
    elif specified_index and specified_columns:
        # Both `index` and `columns` provided. Use as specified, but error if
        # they intersect.
        column_names = user_columns
        index_names = user_index
        if set(column_names).intersection(index_names):
            raise ValueError("Specified index and column names must not intersect")
    else:
        # Use default columns and index from the metadata
        column_names = data_columns
        index_names = data_index

    return column_names, index_names


def _analyze_paths(file_list, fs, root=False):
    """Consolidate list of file-paths into parquet relative paths

    Note: This function was mostly copied from dask/fastparquet to
    use in both `FastParquetEngine` and `ArrowEngine`."""

    def _join_path(*path):
        def _scrub(i, p):
            # Convert path to standard form
            # this means windows path separators are converted to linux
            p = p.replace(fs.sep, "/")
            if p == "":  # empty path is assumed to be a relative path
                return "."
            if p[-1] == "/":  # trailing slashes are not allowed
                p = p[:-1]
            if i > 0 and p[0] == "/":  # only the first path can start with /
                p = p[1:]
            return p

        abs_prefix = ""
        if path and path[0]:
            if path[0][0] == "/":
                abs_prefix = "/"
                path = list(path)
                path[0] = path[0][1:]
            elif fs.sep == "\\" and path[0][1:].startswith(":/"):
                # If windows, then look for the "c:/" prefix
                abs_prefix = path[0][0:3]
                path = list(path)
                path[0] = path[0][3:]

        _scrubbed = []
        for i, p in enumerate(path):
            _scrubbed.extend(_scrub(i, p).split("/"))
        simpler = []
        for s in _scrubbed:
            if s == ".":
                pass
            elif s == "..":
                if simpler:
                    if simpler[-1] == "..":
                        simpler.append(s)
                    else:
                        simpler.pop()
                elif abs_prefix:
                    raise Exception("can not get parent of root")
                else:
                    simpler.append(s)
            else:
                simpler.append(s)

        if not simpler:
            if abs_prefix:
                joined = abs_prefix
            else:
                joined = "."
        else:
            joined = abs_prefix + ("/".join(simpler))
        return joined

    path_parts_list = [_join_path(fn).split("/") for fn in file_list]
    if root is False:
        basepath = path_parts_list[0][:-1]
        for i, path_parts in enumerate(path_parts_list):
            j = len(path_parts) - 1
            for k, (base_part, path_part) in enumerate(zip(basepath, path_parts)):
                if base_part != path_part:
                    j = k
                    break
            basepath = basepath[:j]
        l = len(basepath)

    else:
        basepath = _join_path(root).split("/")
        l = len(basepath)
        assert all(
            p[:l] == basepath for p in path_parts_list
        ), "All paths must begin with the given root"
    l = len(basepath)
    out_list = []
    for path_parts in path_parts_list:
        out_list.append(
            "/".join(path_parts[l:])
        )  # use '/'.join() instead of _join_path to be consistent with split('/')

    return (
        "/".join(basepath),
        out_list,
    )  # use '/'.join() instead of _join_path to be consistent with split('/')


def _flatten_filters(filters):
    """Flatten DNF-formatted filters (list of tuples)"""
    return (
        set(flatten(tuple(flatten(filters, container=list)), container=tuple))
        if filters
        else []
    )


def _aggregate_stats(
    file_path,
    file_row_group_stats,
    file_row_group_column_stats,
    stat_col_indices,
):
    """Utility to aggregate the statistics for N row-groups
    into a single dictionary.

    Used by `Engine._construct_parts`
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
            i = ind * 2
            if df_cols is None:
                s["columns"].append(
                    {
                        "name": name,
                        "min": file_row_group_column_stats[0][i],
                        "max": file_row_group_column_stats[0][i + 1],
                    }
                )
            else:
                s["columns"].append(
                    {
                        "name": name,
                        "min": df_cols.iloc[:, i].min(),
                        "max": df_cols.iloc[:, i + 1].max(),
                    }
                )
        return s


def _row_groups_to_parts(
    gather_statistics,
    split_row_groups,
    file_row_groups,
    file_row_group_stats,
    file_row_group_column_stats,
    stat_col_indices,
    make_part_func,
    make_part_kwargs,
):

    # Construct `parts` and `stats`
    parts = []
    stats = []
    if split_row_groups:
        # Create parts from each file,
        # limiting the number of row_groups in each piece
        split_row_groups = int(split_row_groups)
        for filename, row_groups in file_row_groups.items():
            row_group_count = len(row_groups)
            for i in range(0, row_group_count, split_row_groups):
                i_end = i + split_row_groups
                rg_list = row_groups[i:i_end]

                part = make_part_func(
                    filename,
                    rg_list,
                    **make_part_kwargs,
                )
                if part is None:
                    continue

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

            part = make_part_func(
                filename,
                row_groups,
                **make_part_kwargs,
            )
            if part is None:
                continue

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
