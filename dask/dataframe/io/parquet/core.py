from distutils.version import LooseVersion
import math

import tlz as toolz
import warnings
from ....bytes import core  # noqa
from fsspec.core import get_fs_token_paths
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import stringify_path

from .utils import _analyze_paths
from ...core import DataFrame, new_dd_object
from ....base import tokenize
from ....delayed import Delayed
from ....utils import import_required, natural_sort_key, parse_bytes, apply
from ...methods import concat
from ....highlevelgraph import Layer, HighLevelGraph


try:
    import snappy

    snappy.compress
except (ImportError, AttributeError):
    snappy = None


__all__ = ("read_parquet", "to_parquet")

NONE_LABEL = "__null_dask_index__"

# ----------------------------------------------------------------------
# User API


class ParquetSubgraph(Layer):
    """
    Subgraph for reading Parquet files.

    Enables optimizations (see optimize_read_parquet_getitem).
    """

    def __init__(
        self,
        name,
        engine,
        fs,
        meta,
        columns,
        index,
        parts,
        kwargs,
        part_ids=None,
        common_kwargs=None,
        annotations=None,
    ):
        super().__init__(annotations=annotations)
        self.name = name
        self.engine = engine
        self.fs = fs
        self.meta = meta
        self.columns = columns
        self.index = index
        self.parts = parts
        self.kwargs = kwargs
        self.part_ids = list(range(len(parts))) if part_ids is None else part_ids

        # `kwargs` = user-defined kwargs to be passed for all parts
        self.kwargs = kwargs

        # `common_kwargs` = engine-gathered kwargs to be passed for all parts
        self.common_kwargs = common_kwargs if common_kwargs else {}

    def __repr__(self):
        return "ParquetSubgraph<name='{}', n_parts={}, columns={}>".format(
            self.name, len(self.part_ids), list(self.columns)
        )

    def __getitem__(self, key):
        try:
            name, i = key
        except ValueError:
            # too many / few values to unpack
            raise KeyError(key) from None

        if name != self.name:
            raise KeyError(key)

        if i not in self.part_ids:
            raise KeyError(key)

        part = self.parts[i]
        if not isinstance(part, list):
            part = [part]

        return (
            read_parquet_part,
            self.fs,
            self.engine.read_partition,
            self.meta,
            [(p["piece"], p.get("kwargs", {})) for p in part],
            self.columns,
            self.index,
            toolz.merge(self.common_kwargs, self.kwargs or {}),
        )

    def __len__(self):
        return len(self.part_ids)

    def __iter__(self):
        for i in self.part_ids:
            yield (self.name, i)

    def is_materialized(self):
        return False  # Never materialized

    def get_dependencies(self, all_hlg_keys):
        return {k: set() for k in self}

    def cull(self, keys, all_hlg_keys):
        ret = ParquetSubgraph(
            name=self.name,
            engine=self.engine,
            fs=self.fs,
            meta=self.meta,
            columns=self.columns,
            index=self.index,
            parts=self.parts,
            kwargs=self.kwargs,
            part_ids={i for i in self.part_ids if (self.name, i) in keys},
            common_kwargs=self.common_kwargs,
            annotations=self.annotations,
        )
        return ret, ret.get_dependencies(all_hlg_keys)


def read_parquet(
    path,
    columns=None,
    filters=None,
    categories=None,
    index=None,
    storage_options=None,
    engine="auto",
    gather_statistics=None,
    split_row_groups=None,
    read_from_paths=None,
    chunksize=None,
    **kwargs,
):
    """
    Read a Parquet file into a Dask DataFrame

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

    Parameters
    ----------
    path : string or list
        Source directory for data, or path(s) to individual parquet files.
        Prefix with a protocol like ``s3://`` to read from alternative
        filesystems. To read from multiple files you can pass a globstring or a
        list of paths, with the caveat that they must all have the same
        protocol.
    columns : string, list or None (default)
        Field name(s) to read in as columns in the output. By default all
        non-index fields will be read (as determined by the pandas parquet
        metadata, if present). Provide a single field name instead of a list to
        read in the data as a Series.
    filters : Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]]
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``. Using this
        argument will NOT result in row-wise filtering of the final partitions
        unless ``engine="pyarrow-dataset"`` is also specified.  For other engines,
        filtering is only performed at the partition level, i.e., to prevent the
        loading of some row-groups and/or files.

        For the "pyarrow" engines, predicates can be expressed in disjunctive
        normal form (DNF). This means that the innermost tuple describes a single
        column predicate. These inner predicates are combined with an AND
        conjunction into a larger predicate. The outer-most list then combines all
        of the combined filters with an OR disjunction.

        Predicates can also be expressed as a List[Tuple]. These are evaluated
        as an AND conjunction. To express OR in predictates, one must use the
        (preferred for "pyarrow") List[List[Tuple]] notation.

        Note that the "fastparquet" engine does not currently support DNF for
        the filtering of partitioned columns (List[Tuple] is required).
    index : string, list, False or None (default)
        Field name(s) to use as the output frame index. By default will be
        inferred from the pandas parquet file metadata (if present). Use False
        to read all fields as columns.
    categories : list, dict or None
        For any fields listed here, if the parquet encoding is Dictionary,
        the column will be created with dtype category. Use only if it is
        guaranteed that the column is encoded as dictionary in all row-groups.
        If a list, assumes up to 2**16-1 labels; if a dict, specify the number
        of labels expected; if None, will load categories automatically for
        data written by dask/fastparquet, not otherwise.
    storage_options : dict
        Key/value pairs to be passed on to the file-system backend, if any.
    engine : str, default 'auto'
        Parquet reader library to use. Options include: 'auto', 'fastparquet',
        'pyarrow', 'pyarrow-dataset', and 'pyarrow-legacy'. Defaults to 'auto',
        which selects the FastParquetEngine if fastparquet is installed (and
        ArrowLegacyEngine otherwise).  If 'pyarrow-dataset' is specified, the
        ArrowDatasetEngine (which leverages the pyarrow.dataset API) will be used
        for newer PyArrow versions (>=1.0.0). If 'pyarrow' or 'pyarrow-legacy' are
        specified, the ArrowLegacyEngine will be used (which leverages the
        pyarrow.parquet.ParquetDataset API).
        NOTE: 'pyarrow-dataset' enables row-wise filtering, but requires
        pyarrow>=1.0. The behavior of 'pyarrow' will most likely change to
        ArrowDatasetEngine in a future release, and the 'pyarrow-legacy'
        option will be deprecated once the ParquetDataset API is deprecated.
    gather_statistics : bool or None (default).
        Gather the statistics for each dataset partition. By default,
        this will only be done if the _metadata file is available. Otherwise,
        statistics will only be gathered if True, because the footer of
        every file will be parsed (which is very slow on some systems).
    split_row_groups : bool or int
        Default is True if a _metadata file is available or if
        the dataset is composed of a single file (otherwise defult is False).
        If True, then each output dataframe partition will correspond to a single
        parquet-file row-group. If False, each partition will correspond to a
        complete file.  If a positive integer value is given, each dataframe
        partition will correspond to that number of parquet row-groups (or fewer).
        Only the "pyarrow" engine supports this argument.
    read_from_paths : bool or None (default)
        Only used by ``ArrowDatasetEngine`` when ``filters`` are specified.
        Determines whether the engine should avoid inserting large pyarrow
        (``ParquetFileFragment``) objects in the task graph.  If this option
        is True, ``read_partition`` will need to regenerate the appropriate
        fragment object from the path and row-group IDs.  This will reduce the
        size of the task graph, but will add minor overhead to ``read_partition``.
        By default (None), ``ArrowDatasetEngine`` will set this option to
        ``False`` when there are filters.
    chunksize : int, str
        The target task partition size.  If set, consecutive row-groups
        from the same file will be aggregated into the same output
        partition until the aggregate size reaches this value.
    **kwargs: dict (of dicts)
        Passthrough key-word arguments for read backend.
        The top-level keys correspond to the appropriate operation type, and
        the second level corresponds to the kwargs that will be passed on to
        the underlying ``pyarrow`` or ``fastparquet`` function.
        Supported top-level keys: 'dataset' (for opening a ``pyarrow`` dataset),
        'file' (for opening a ``fastparquet`` ``ParquetFile``), 'read' (for the
        backend read function), 'arrow_to_pandas' (for controlling the arguments
        passed to convert from a ``pyarrow.Table.to_pandas()``)

    Examples
    --------
    >>> df = dd.read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """

    if isinstance(columns, str):
        df = read_parquet(
            path,
            columns=[columns],
            filters=filters,
            categories=categories,
            index=index,
            storage_options=storage_options,
            engine=engine,
            gather_statistics=gather_statistics,
            split_row_groups=split_row_groups,
            read_from_paths=read_from_paths,
            chunksize=chunksize,
        )
        return df[columns]

    if columns is not None:
        columns = list(columns)

    name = "read-parquet-" + tokenize(
        path,
        columns,
        filters,
        categories,
        index,
        storage_options,
        engine,
        gather_statistics,
        split_row_groups,
        read_from_paths,
        chunksize,
    )

    if isinstance(engine, str):
        engine = get_engine(engine)

    if hasattr(path, "name"):
        path = stringify_path(path)
    fs, _, paths = get_fs_token_paths(path, mode="rb", storage_options=storage_options)

    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

    auto_index_allowed = False
    if index is None:
        # User is allowing auto-detected index
        auto_index_allowed = True
    if index and isinstance(index, str):
        index = [index]

    read_metadata_result = engine.read_metadata(
        fs,
        paths,
        categories=categories,
        index=index,
        gather_statistics=True if chunksize else gather_statistics,
        filters=filters,
        split_row_groups=split_row_groups,
        read_from_paths=read_from_paths,
        **kwargs,
    )

    # In the future, we may want to give the engine the
    # option to return a dedicated element for `common_kwargs`.
    # However, to avoid breaking the API, we just embed this
    # data in the first element of `parts` for now.
    # The logic below is inteded to handle backward and forward
    # compatibility with a user-defined engine.
    meta, statistics, parts, index = read_metadata_result[:4]
    common_kwargs = {}
    if len(read_metadata_result) > 4:
        # Engine may return common_kwargs as a separate element
        common_kwargs = read_metadata_result[4]
    elif len(parts):
        # If the engine does not return a dedicated
        # common_kwargs argument, it may be stored in
        # the first element of `parts`
        common_kwargs = parts[0].pop("common_kwargs", {})

    # Parse dataset statistics from metadata (if available)
    parts, divisions, index, index_in_columns = process_statistics(
        parts, statistics, filters, index, chunksize
    )

    # Account for index and columns arguments.
    # Modify `meta` dataframe accordingly
    meta, index, columns = set_index_columns(
        meta, index, columns, index_in_columns, auto_index_allowed
    )
    if meta.index.name == NONE_LABEL:
        meta.index.name = None

    subgraph = ParquetSubgraph(
        name,
        engine,
        fs,
        meta,
        columns,
        index,
        parts,
        kwargs,
        common_kwargs=common_kwargs,
    )

    # Set the index that was previously treated as a column
    if index_in_columns:
        meta = meta.set_index(index)
        if meta.index.name == NONE_LABEL:
            meta.index.name = None

    if len(divisions) < 2:
        # empty dataframe - just use meta
        subgraph = {(name, 0): meta}
        divisions = (None, None)

    return new_dd_object(subgraph, name, meta, divisions)


def read_parquet_part(fs, func, meta, part, columns, index, kwargs):
    """Read a part of a parquet dataset

    This function is used by `read_parquet`."""

    if isinstance(part, list):
        dfs = [
            func(fs, rg, columns.copy(), index, **toolz.merge(kwargs, kw))
            for (rg, kw) in part
        ]
        df = concat(dfs, axis=0)
    else:
        # NOTE: `kwargs` are the same for all parts, while `part_kwargs` may
        #       be different for each part.
        rg, part_kwargs = part
        df = func(fs, rg, columns, index, **toolz.merge(kwargs, part_kwargs))

    if meta.columns.name:
        df.columns.name = meta.columns.name
    columns = columns or []
    index = index or []
    df = df[[c for c in columns if c not in index]]
    if index == [NONE_LABEL]:
        df.index.name = None
    return df


def to_parquet(
    df,
    path,
    engine="auto",
    compression="default",
    write_index=True,
    append=False,
    overwrite=False,
    ignore_divisions=False,
    partition_on=None,
    storage_options=None,
    write_metadata_file=True,
    compute=True,
    compute_kwargs=None,
    schema=None,
    **kwargs,
):
    """Store Dask.dataframe to Parquet files

    Notes
    -----
    Each partition will be written to a separate file.

    Parameters
    ----------
    df : dask.dataframe.DataFrame
    path : string or pathlib.Path
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    engine : {'auto', 'fastparquet', 'pyarrow'}, default 'auto'
        Parquet library to use. If only one library is installed, it will use
        that one; if both, it will use 'fastparquet'.
    compression : string or dict, optional
        Either a string like ``"snappy"`` or a dictionary mapping column names
        to compressors like ``{"name": "gzip", "values": "snappy"}``. The
        default is ``"default"``, which uses the default compression for
        whichever engine is selected.
    write_index : boolean, optional
        Whether or not to write the index. Defaults to True.
    append : bool, optional
        If False (default), construct data-set from scratch. If True, add new
        row-group(s) to an existing data-set. In the latter case, the data-set
        must exist, and the schema must match the input data.
    overwrite : bool, optional
        Whether or not to remove the contents of `path` before writing the dataset.
        The default is False.  If True, the specified path must correspond to
        a directory (but not the current working directory).  This option cannot
        be set to True if `append=True`.
        NOTE: `overwrite=True` will remove the original data even if the current
        write operation fails.  Use at your own risk.
    ignore_divisions : bool, optional
        If False (default) raises error when previous divisions overlap with
        the new appended divisions. Ignored if append=False.
    partition_on : list, optional
        Construct directory-based partitioning by splitting on these fields'
        values. Each dask partition will result in one or more datafiles,
        there will be no global groupby.
    storage_options : dict, optional
        Key/value pairs to be passed on to the file-system backend, if any.
    write_metadata_file : bool, optional
        Whether to write the special "_metadata" file.
    compute : bool, optional
        If True (default) then the result is computed immediately. If False
        then a ``dask.delayed`` object is returned for future computation.
    compute_kwargs : dict, optional
        Options to be passed in to the compute method
    schema : Schema object, dict, or {"infer", None}, optional
        Global schema to use for the output dataset. Alternatively, a `dict`
        of pyarrow types can be specified (e.g. `schema={"id": pa.string()}`).
        For this case, fields excluded from the dictionary will be inferred
        from `_meta_nonempty`.  If "infer", the first non-empty and non-null
        partition will be used to infer the type for "object" columns. If
        None (default), we let the backend infer the schema for each distinct
        output partition. If the partitions produce inconsistent schemas,
        pyarrow will throw an error when writing the shared _metadata file.
        Note that this argument is ignored by the "fastparquet" engine.
    **kwargs :
        Extra options to be passed on to the specific backend.

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> dd.to_parquet(df, '/path/to/output/',...)  # doctest: +SKIP

    See Also
    --------
    read_parquet: Read parquet data to dask.dataframe
    """

    if compression == "default":
        if snappy is not None:
            compression = "snappy"
        else:
            compression = None

    partition_on = partition_on or []
    if isinstance(partition_on, str):
        partition_on = [partition_on]

    if set(partition_on) - set(df.columns):
        raise ValueError(
            "Partitioning on non-existent column. "
            "partition_on=%s ."
            "columns=%s" % (str(partition_on), str(list(df.columns)))
        )

    if isinstance(engine, str):
        engine = get_engine(engine)

    if hasattr(path, "name"):
        path = stringify_path(path)
    fs, _, _ = get_fs_token_paths(path, mode="wb", storage_options=storage_options)
    # Trim any protocol information from the path before forwarding
    path = fs._strip_protocol(path)

    if overwrite:
        if isinstance(fs, LocalFileSystem):
            working_dir = fs.expand_path(".")[0]
            if path == working_dir:
                raise ValueError(
                    "Cannot clear the contents of the current working directory!"
                )
        if append:
            raise ValueError("Cannot use both `overwrite=True` and `append=True`!")
        if fs.isdir(path):
            # Only remove path contents if
            # (1) The path exists
            # (2) The path is a directory
            # (3) The path is not the current working directory
            fs.rm(path, recursive=True)

    # Save divisions and corresponding index name. This is necessary,
    # because we may be resetting the index to write the file
    division_info = {"divisions": df.divisions, "name": df.index.name}
    if division_info["name"] is None:
        # As of 0.24.2, pandas will rename an index with name=None
        # when df.reset_index() is called.  The default name is "index",
        # but dask will always change the name to the NONE_LABEL constant
        if NONE_LABEL not in df.columns:
            division_info["name"] = NONE_LABEL
        elif write_index:
            raise ValueError(
                "Index must have a name if __null_dask_index__ is a column."
            )
        else:
            warnings.warn(
                "If read back by Dask, column named __null_dask_index__ "
                "will be set to the index (and renamed to None)."
            )

    # There are some "resrved" names that may be used as the default column
    # name after resetting the index. However, we don't want to treat it as
    # a "special" name if the string is already used as a "real" column name.
    reserved_names = []
    for name in ["index", "level_0"]:
        if name not in df.columns:
            reserved_names.append(name)

    # If write_index==True (default), reset the index and record the
    # name of the original index in `index_cols` (we will set the name
    # to the NONE_LABEL constant if it is originally `None`).
    # `fastparquet` will use `index_cols` to specify the index column(s)
    # in the metadata.  `pyarrow` will revert the `reset_index` call
    # below if `index_cols` is populated (because pyarrow will want to handle
    # index preservation itself).  For both engines, the column index
    # will be written to "pandas metadata" if write_index=True
    index_cols = []
    if write_index:
        real_cols = set(df.columns)
        none_index = list(df._meta.index.names) == [None]
        df = df.reset_index()
        if none_index:
            df.columns = [
                c if c not in reserved_names else NONE_LABEL for c in df.columns
            ]
        index_cols = [c for c in set(df.columns) - real_cols]
    else:
        # Not writing index - might as well drop it
        df = df.reset_index(drop=True)

    _to_parquet_kwargs = {
        "engine",
        "compression",
        "write_index",
        "append",
        "ignore_divisions",
        "partition_on",
        "storage_options",
        "write_metadata_file",
        "compute",
    }
    kwargs_pass = {k: v for k, v in kwargs.items() if k not in _to_parquet_kwargs}

    # Engine-specific initialization steps to write the dataset.
    # Possibly create parquet metadata, and load existing stuff if appending
    meta, schema, i_offset = engine.initialize_write(
        df,
        fs,
        path,
        append=append,
        ignore_divisions=ignore_divisions,
        partition_on=partition_on,
        division_info=division_info,
        index_cols=index_cols,
        schema=schema,
        **kwargs_pass,
    )

    # Use i_offset and df.npartitions to define file-name list
    filenames = ["part.%i.parquet" % (i + i_offset) for i in range(df.npartitions)]

    # Construct IO graph
    dsk = {}
    name = "to-parquet-" + tokenize(
        df,
        fs,
        path,
        append,
        ignore_divisions,
        partition_on,
        division_info,
        index_cols,
        schema,
    )
    part_tasks = []
    kwargs_pass["fmd"] = meta
    kwargs_pass["compression"] = compression
    kwargs_pass["index_cols"] = index_cols
    kwargs_pass["schema"] = schema
    for d, filename in enumerate(filenames):
        dsk[(name, d)] = (
            apply,
            engine.write_partition,
            [
                (df._name, d),
                path,
                fs,
                filename,
                partition_on,
                write_metadata_file,
            ],
            toolz.merge(kwargs_pass, {"head": True}) if d == 0 else kwargs_pass,
        )
        part_tasks.append((name, d))

    # Collect metadata and write _metadata
    if write_metadata_file:
        dsk[name] = (
            apply,
            engine.write_metadata,
            [
                part_tasks,
                meta,
                fs,
                path,
            ],
            {"append": append, "compression": compression},
        )
    else:
        dsk[name] = (lambda x: None, part_tasks)

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[df])
    out = Delayed(name, graph)

    if compute:
        if compute_kwargs is None:
            compute_kwargs = dict()
        out = out.compute(**compute_kwargs)
    return out


def create_metadata_file(
    paths,
    root_dir=None,
    out_dir=None,
    engine="pyarrow",
    storage_options=None,
    split_every=32,
    compute=True,
    compute_kwargs=None,
):
    """Construct a global _metadata file from a list of parquet files.

    Dask's read_parquet function is designed to leverage a global
    _metadata file whenever one is available.  The to_parquet
    function will generate this file automatically by default, but it
    may not exist if the dataset was generated outside of Dask.  This
    utility provides a mechanism to generate a _metadata file from a
    list of existing parquet files.

    NOTE: This utility is not yet supported for the "fastparquet" engine.

    Parameters
    ----------
    paths : list(string)
        List of files to collect footer metadata from.
    root_dir : string, optional
        Root directory of dataset.  The `file_path` fields in the new
        _metadata file will relative to this directory.  If None, a common
        root directory will be inferred.
    out_dir : string or False, optional
        Directory location to write the final _metadata file.  By default,
        this will be set to `root_dir`.  If False is specified, the global
        metadata will be returned as an in-memory object (and will not be
        written to disk).
    engine : str or Engine, default 'pyarrow'
        Parquet Engine to use. Only 'pyarrow' is supported if a string
        is passed.
    storage_options : dict, optional
        Key/value pairs to be passed on to the file-system backend, if any.
    split_every : int, optional
        The final metadata object that is written to _metadata can be much
        smaller than the list of footer metadata. In order to avoid the
        aggregation of all metadata within a single task, a tree reduction
        is used.  This argument specifies the maximum number of metadata
        inputs to be handled by any one task in the tree. Defaults to 32.
    compute : bool, optional
        If True (default) then the result is computed immediately. If False
        then a ``dask.delayed`` object is returned for future computation.
    compute_kwargs : dict, optional
        Options to be passed in to the compute method
    """

    # Get engine.
    # Note that "fastparquet" is not yet supported
    if isinstance(engine, str):
        if engine not in ("pyarrow", "arrow"):
            raise ValueError(
                f"{engine} is not a supported engine for create_metadata_file "
                "Try engine='pyarrow'."
            )
        engine = get_engine(engine)

    # Process input path list
    fs, _, paths = get_fs_token_paths(paths, mode="rb", storage_options=storage_options)
    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering
    ap_kwargs = {"root": root_dir} if root_dir else {}
    root_dir, fns = _analyze_paths(paths, fs, **ap_kwargs)
    out_dir = root_dir if out_dir is None else out_dir

    # Start constructing a raw graph
    dsk = {}
    name = "gen-metadata-" + tokenize(paths, fs)
    collect_name = "collect-" + name
    agg_name = "agg-" + name

    # Define a "collect" task for each file in the input list.
    # Each tasks will:
    #   1. Extract the footer metadata from a distinct file
    #   2. Populate the `file_path` field in the metadata
    #   3. Return the extracted/modified metadata
    for p, (fn, path) in enumerate(zip(fns, paths)):
        key = (collect_name, p, 0)
        dsk[key] = (engine.collect_file_metadata, path, fs, fn)

    # Build a reduction tree to aggregate all footer metadata
    # into a single metadata object.  Each task in the tree
    # will take in a list of metadata objects as input, and will
    # usually output a single (aggregated) metadata object.
    # The final task in the tree will write the result to disk
    # instead of returning it (this behavior is triggered by
    # passing a file path to `engine.aggregate_metadata`).
    parts = len(paths)
    widths = [parts]
    while parts > 1:
        parts = math.ceil(parts / split_every)
        widths.append(parts)
    height = len(widths)
    for depth in range(1, height):
        for group in range(widths[depth]):
            p_max = widths[depth - 1]
            lstart = split_every * group
            lstop = min(lstart + split_every, p_max)
            dep_task_name = collect_name if depth == 1 else agg_name
            node_list = [(dep_task_name, p, depth - 1) for p in range(lstart, lstop)]
            if depth == height - 1:
                assert group == 0
                dsk[name] = (engine.aggregate_metadata, node_list, fs, out_dir)
            else:
                dsk[(agg_name, group, depth)] = (
                    engine.aggregate_metadata,
                    node_list,
                    None,
                    None,
                )

    # There will be no aggregation tasks if there is only one file
    if len(paths) == 1:
        dsk[name] = (engine.aggregate_metadata, [(collect_name, 0, 0)], fs, out_dir)

    # Convert the raw graph to a `Delayed` object
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[])
    out = Delayed(name, graph)

    # Optionally compute the result
    if compute:
        if compute_kwargs is None:
            compute_kwargs = dict()
        out = out.compute(**compute_kwargs)
    return out


_ENGINES = {}


def get_engine(engine):
    """Get the parquet engine backend implementation.

    Parameters
    ----------
    engine : str, default 'auto'
        Backend parquet library to use. Options include: 'auto', 'fastparquet',
        'pyarrow', 'pyarrow-dataset', and 'pyarrow-legacy'. Defaults to 'auto',
        which selects the FastParquetEngine if fastparquet is installed (and
        ArrowLegacyEngine otherwise).  If 'pyarrow-dataset' is specified, the
        ArrowDatasetEngine (which leverages the pyarrow.dataset API) will be used
        for newer PyArrow versions (>=1.0.0). If 'pyarrow' or 'pyarrow-legacy' are
        specified, the ArrowLegacyEngine will be used (which leverages the
        pyarrow.parquet.ParquetDataset API).
        NOTE: 'pyarrow-dataset' enables row-wise filtering, but requires
        pyarrow>=1.0. The behavior of 'pyarrow' will most likely change to
        ArrowDatasetEngine in a future release, and the 'pyarrow-legacy'
        option will be deprecated once the ParquetDataset API is deprecated.
    gather_statistics : bool or None (default).

    Returns
    -------
    A dict containing a ``'read'`` and ``'write'`` function.
    """
    if engine in _ENGINES:
        return _ENGINES[engine]

    if engine == "auto":
        for eng in ["fastparquet", "pyarrow"]:
            try:
                return get_engine(eng)
            except RuntimeError:
                pass
        else:
            raise RuntimeError("Please install either fastparquet or pyarrow")

    elif engine == "fastparquet":
        import_required("fastparquet", "`fastparquet` not installed")
        from .fastparquet import FastParquetEngine

        _ENGINES["fastparquet"] = eng = FastParquetEngine
        return eng

    elif engine in ("pyarrow", "arrow", "pyarrow-legacy", "pyarrow-dataset"):
        pa = import_required("pyarrow", "`pyarrow` not installed")

        if LooseVersion(pa.__version__) < "0.13.1":
            raise RuntimeError("PyArrow version >= 0.13.1 required")

        if engine == "pyarrow-dataset" and LooseVersion(pa.__version__) >= "1.0.0":
            from .arrow import ArrowDatasetEngine

            _ENGINES[engine] = eng = ArrowDatasetEngine
        else:
            from .arrow import ArrowLegacyEngine

            _ENGINES[engine] = eng = ArrowLegacyEngine
        return eng

    else:
        raise ValueError(
            'Unsupported engine: "{0}".'.format(engine)
            + '  Valid choices include "pyarrow" and "fastparquet".'
        )


#####################
# Utility Functions #
#####################


def sorted_columns(statistics):
    """Find sorted columns given row-group statistics

    This finds all columns that are sorted, along with appropriate divisions
    values for those columns

    Returns
    -------
    out: List of {'name': str, 'divisions': List[str]} dictionaries
    """
    if not statistics:
        return []

    out = []
    for i, c in enumerate(statistics[0]["columns"]):
        if not all(
            "min" in s["columns"][i] and "max" in s["columns"][i] for s in statistics
        ):
            continue
        divisions = [c["min"]]
        max = c["max"]
        success = c["min"] is not None
        for stats in statistics[1:]:
            c = stats["columns"][i]
            if c["min"] is None:
                success = False
                break
            if c["min"] >= max:
                divisions.append(c["min"])
                max = c["max"]
            else:
                success = False
                break

        if success:
            divisions.append(max)
            assert divisions == sorted(divisions)
            out.append({"name": c["name"], "divisions": divisions})

    return out


def apply_filters(parts, statistics, filters):
    """Apply filters onto parts/statistics pairs

    Parameters
    ----------
    parts: list
        Tokens corresponding to row groups to read in the future
    statistics: List[dict]
        List of statistics for each part, including min and max values
    filters: Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]]
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``. This
        implements partition-level (hive) filtering only, i.e., to prevent the
        loading of some row-groups and/or files.

        Predicates can be expressed in disjunctive normal form (DNF). This means
        that the innermost tuple describes a single column predicate. These
        inner predicates are combined with an AND conjunction into a larger
        predicate. The outer-most list then combines all of the combined
        filters with an OR disjunction.

        Predicates can also be expressed as a List[Tuple]. These are evaluated
        as an AND conjunction. To express OR in predictates, one must use the
        (preferred) List[List[Tuple]] notation.

        Note that the "fastparquet" engine does not currently support DNF for
        the filtering of partitioned columns (List[Tuple] is required).
    Returns
    -------
    parts, statistics: the same as the input, but possibly a subset
    """

    def apply_conjunction(parts, statistics, conjunction):
        for column, operator, value in conjunction:
            out_parts = []
            out_statistics = []
            for part, stats in zip(parts, statistics):
                if "filter" in stats and stats["filter"]:
                    continue  # Filtered by engine
                try:
                    c = toolz.groupby("name", stats["columns"])[column][0]
                    min = c["min"]
                    max = c["max"]
                except KeyError:
                    out_parts.append(part)
                    out_statistics.append(stats)
                else:
                    if (
                        operator == "=="
                        and min <= value <= max
                        or operator == "<"
                        and min < value
                        or operator == "<="
                        and min <= value
                        or operator == ">"
                        and max > value
                        or operator == ">="
                        and max >= value
                        or operator == "in"
                        and any(min <= item <= max for item in value)
                    ):
                        out_parts.append(part)
                        out_statistics.append(stats)

            parts, statistics = out_parts, out_statistics

        return parts, statistics

    conjunction, *disjunction = filters if isinstance(filters[0], list) else [filters]

    out_parts, out_statistics = apply_conjunction(parts, statistics, conjunction)
    for conjunction in disjunction:
        for part, stats in zip(*apply_conjunction(parts, statistics, conjunction)):
            if part not in out_parts:
                out_parts.append(part)
                out_statistics.append(stats)

    return out_parts, out_statistics


def process_statistics(parts, statistics, filters, index, chunksize):
    """Process row-group column statistics in metadata
    Used in read_parquet.
    """
    index_in_columns = False
    if statistics:
        result = list(
            zip(
                *[
                    (part, stats)
                    for part, stats in zip(parts, statistics)
                    if stats["num-rows"] > 0
                ]
            )
        )
        parts, statistics = result or [[], []]
        if filters:
            parts, statistics = apply_filters(parts, statistics, filters)

        # Aggregate parts/statistics if we are splitting by row-group
        if chunksize:
            parts, statistics = aggregate_row_groups(parts, statistics, chunksize)

        out = sorted_columns(statistics)

        if index and isinstance(index, str):
            index = [index]
        if index and out:
            # Only one valid column
            out = [o for o in out if o["name"] in index]
        if index is not False and len(out) == 1:
            # Use only sorted column with statistics as the index
            divisions = out[0]["divisions"]
            if index is None:
                index_in_columns = True
                index = [out[0]["name"]]
            elif index != [out[0]["name"]]:
                raise ValueError("Specified index is invalid.\nindex: {}".format(index))
        elif index is not False and len(out) > 1:
            if any(o["name"] == NONE_LABEL for o in out):
                # Use sorted column matching NONE_LABEL as the index
                [o] = [o for o in out if o["name"] == NONE_LABEL]
                divisions = o["divisions"]
                if index is None:
                    index = [o["name"]]
                    index_in_columns = True
                elif index != [o["name"]]:
                    raise ValueError(
                        "Specified index is invalid.\nindex: {}".format(index)
                    )
            else:
                # Multiple sorted columns found, cannot autodetect the index
                warnings.warn(
                    "Multiple sorted columns found %s, cannot\n "
                    "autodetect index. Will continue without an index.\n"
                    "To pick an index column, use the index= keyword; to \n"
                    "silence this warning use index=False."
                    "" % [o["name"] for o in out],
                    RuntimeWarning,
                )
                index = False
                divisions = [None] * (len(parts) + 1)
        else:
            divisions = [None] * (len(parts) + 1)
    else:
        divisions = [None] * (len(parts) + 1)

    return parts, divisions, index, index_in_columns


def set_index_columns(meta, index, columns, index_in_columns, auto_index_allowed):
    """Handle index/column arguments, and modify `meta`
    Used in read_parquet.
    """
    ignore_index_column_intersection = False
    if columns is None:
        # User didn't specify columns, so ignore any intersection
        # of auto-detected values with the index (if necessary)
        ignore_index_column_intersection = True
        # Do not allow "un-named" fields to be read in as columns.
        # These were intended to be un-named indices at write time.
        _index = index or []
        columns = [
            c for c in meta.columns if c not in (None, NONE_LABEL) or c in _index
        ]

    if not set(columns).issubset(set(meta.columns)):
        raise ValueError(
            "The following columns were not found in the dataset %s\n"
            "The following columns were found %s"
            % (set(columns) - set(meta.columns), meta.columns)
        )

    if index:
        if isinstance(index, str):
            index = [index]
        if isinstance(columns, str):
            columns = [columns]

        if ignore_index_column_intersection:
            columns = [col for col in columns if col not in index]
        if set(index).intersection(columns):
            if auto_index_allowed:
                raise ValueError(
                    "Specified index and column arguments must not intersect"
                    " (set index=False or remove the detected index from columns).\n"
                    "index: {} | column: {}".format(index, columns)
                )
            else:
                raise ValueError(
                    "Specified index and column arguments must not intersect.\n"
                    "index: {} | column: {}".format(index, columns)
                )

        # Leaving index as a column in `meta`, because the index
        # will be reset below (in case the index was detected after
        # meta was created)
        if index_in_columns:
            meta = meta[columns + index]
        else:
            meta = meta[columns]

    else:
        meta = meta[list(columns)]

    return meta, index, columns


def aggregate_row_groups(parts, stats, chunksize):
    if not stats[0].get("file_path_0", None):
        return parts, stats

    parts_agg = []
    stats_agg = []
    chunksize = parse_bytes(chunksize)
    next_part, next_stat = [parts[0].copy()], stats[0].copy()
    for i in range(1, len(parts)):
        stat, part = stats[i], parts[i]
        if (stat["file_path_0"] == next_stat["file_path_0"]) and (
            (next_stat["total_byte_size"] + stat["total_byte_size"]) <= chunksize
        ):
            # Update part list
            next_part.append(part)

            # Update Statistics
            next_stat["total_byte_size"] += stat["total_byte_size"]
            next_stat["num-rows"] += stat["num-rows"]
            for col, col_add in zip(next_stat["columns"], stat["columns"]):
                if col["name"] != col_add["name"]:
                    raise ValueError("Columns are different!!")
                if "min" in col:
                    col["min"] = min(col["min"], col_add["min"])
                if "max" in col:
                    col["max"] = max(col["max"], col_add["max"])
        else:
            parts_agg.append(next_part)
            stats_agg.append(next_stat)
            next_part, next_stat = [part.copy()], stat.copy()

    parts_agg.append(next_part)
    stats_agg.append(next_stat)

    return parts_agg, stats_agg


DataFrame.to_parquet.__doc__ = to_parquet.__doc__
