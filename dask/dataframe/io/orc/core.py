import copy

from fsspec.core import get_fs_token_paths
from fsspec.utils import stringify_path

from ....base import compute_as_if_collection, tokenize
from ....highlevelgraph import HighLevelGraph
from ....layers import DataFrameIOLayer
from ....utils import apply
from ...core import DataFrame, Scalar, new_dd_object
from .utils import ORCEngine, collect_files


class ORCFunctionWrapper:
    """
    ORC Function-Wrapper Class
    Reads ORC data from disk to produce a partition.
    """

    def __init__(self, columns, engine, index, common_kwargs=None):
        self.columns = columns
        self.engine = engine
        self.index = index
        self.common_kwargs = common_kwargs or {}

    def project_columns(self, columns):
        """Return a new ORCFunctionWrapper object with
        a sub-column projection.
        """
        if columns == self.columns:
            return self
        func = copy.deepcopy(self)
        func.columns = columns
        return func

    def __call__(self, parts):
        _df = self.engine.read_partition(
            parts,
            self.columns,
            **self.common_kwargs,
        )
        if self.index:
            _df.set_index(self.index, inplace=True)
        return _df


def _get_engine(engine):
    # Get engine
    if engine == "pyarrow":
        from .arrow import ArrowORCEngine

        return ArrowORCEngine
    elif not issubclass(engine, ORCEngine):
        raise TypeError("engine must be 'pyarrow', or an ORCEngine object")
    return engine


def read_orc(
    path,
    engine="pyarrow",
    columns=None,
    index=None,
    filters=None,
    split_stripes=1,
    aggregate_files=None,
    storage_options=None,
    gather_statistics=True,
    allow_worker_gather=None,
    dataset_kwargs=None,
    read_kwargs=None,
):
    """Read dataframe from ORC file(s)

    Parameters
    ----------
    path: str or list(str)
        Location of file(s), which can be a full URL with protocol
        specifier, and may include glob character if a single string.
    engine: 'pyarrow' or ORCEngine
        Backend ORC engine to use for IO. Default is "pyarrow".
    columns: None or list(str)
        Columns to load. If None, loads all.
    index: str
        Column name to set as index.
    filters : Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
        List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
        Using this argument will NOT result in row-wise filtering of the final
        partitions. Filtering is only performed at the partition level, i.e.,
        to prevent the loading of some stripes and/or files.

        Filtering is only supported for directory-partitioned columns for the
        defualt ``ORCEngine`` backend. Predicates for any other columns will
        be ignored.

        For the "pyarrow" engine, predicates can be expressed in disjunctive
        normal form (DNF). This means that the innermost tuple describes a single
        column predicate. These inner predicates are combined with an AND
        conjunction into a larger predicate. The outer-most list then combines all
        of the combined filters with an OR disjunction.
    split_stripes: int or False
        Maximum number of ORC stripes to include in each output-DataFrame
        partition. Use False to specify a 1-to-1 mapping between files
        and partitions. Default is 1.
    aggregate_files : bool or str, default False
        Whether distinct file paths may be aggregated into the same output
        partition. A setting of True means that any two file paths may be
        aggregated into the same output partition, while False means that
        inter-file aggregation is prohibited. If the name of a partition
        column is specified, any file within the same partition directory
        (e.g. ``"/<aggregate_files>=*/"``) may be aggregated.
    storage_options: None or dict
        Further parameters to pass to the bytes backend.
    gather_statistics : bool, default True
        Whether to allow the engine to gather file and stripe statistics.
    allow_worker_gather : bool, optional
        Whether to allow the engine to parallelize the task of gathering
        and filtering ouput partition information. The default behavior
        may be engine dependent.
    dataset_kwargs : dict, optional
        Dictionary of key-word arguments to pass to the engine's
        ``get_dataset_info`` method.
    read_kwargs : dict, optional
        Dictionary of key-word arguments to pass to the engine's
        ``read_partition`` method.

    Returns
    -------
    Dask.DataFrame (even if there is only one column)

    Examples
    --------
    >>> df = dd.read_orc('https://github.com/apache/orc/raw/'
    ...                  'master/examples/demo-11-zlib.orc')  # doctest: +SKIP
    """

    # Get engine
    engine = _get_engine(engine)

    # Let engine convert the paths into a dictionary
    # of engine-specific datset information
    dataset_info = engine.get_dataset_info(
        path,
        columns=columns,
        index=index,
        filters=filters,
        gather_statistics=gather_statistics,
        dataset_kwargs=dataset_kwargs,
        storage_options=storage_options,
    )

    # Construct the `_meta` for the output collection.
    # Note that we do this before actually generating
    # the "plan" for output partitions.
    meta = engine.construct_output_meta(
        dataset_info,
        index=index,
        columns=columns,
        read_kwargs=read_kwargs,
    )

    # Construct the output-partition "plan"
    parts, divisions, common_kwargs = engine.construct_partition_plan(
        meta,
        dataset_info,
        filters=filters,
        split_stripes=split_stripes,
        aggregate_files=aggregate_files,
        gather_statistics=gather_statistics,
        allow_worker_gather=allow_worker_gather,
    )

    # Add read_kwargs to common_kwargs
    common_kwargs.update(read_kwargs or {})

    # Construct and return a Blockwise layer
    label = "read-orc-"
    output_name = label + tokenize(
        dataset_info,
        columns,
        index,
        split_stripes,
        aggregate_files,
        filters,
        dataset_kwargs,
        read_kwargs,
    )
    layer = DataFrameIOLayer(
        output_name,
        columns,
        parts,
        ORCFunctionWrapper(columns, engine, index, common_kwargs=common_kwargs),
        label=label,
    )
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    divisions = divisions or ([None] * (len(parts) + 1))
    return new_dd_object(graph, output_name, meta, divisions)


def to_orc(
    df,
    path,
    engine="pyarrow",
    write_index=True,
    append=False,
    partition_on=None,
    storage_options=None,
    compute=True,
    compute_kwargs=None,
):
    """Store Dask.dataframe to ORC files

    Notes
    -----
    Each partition will be written to a separate file.

    Parameters
    ----------
    df : dask.dataframe.DataFrame
    path : string or pathlib.Path
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    engine : 'pyarrow' or ORCEngine
        Parquet library to use. If only one library is installed, it will use
        that one; if both, it will use 'fastparquet'.
    write_index : boolean, default True
        Whether or not to write the index. Defaults to True.
    append : boolean, default False
        If False (default), construct data-set from scratch. If True, add new
        file(s) to an existing data-set (if one exists).
    partition_on : list, default None
        Construct directory-based partitioning by splitting on these fields'
        values. Each dask partition will result in one or more datafiles,
        there will be no global groupby.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    compute : bool, default True
        If True (default) then the result is computed immediately. If False
        then a ``dask.dataframe.Scalar`` object is returned for future computation.
    compute_kwargs : dict, default True
        Options to be passed in to the compute method

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> df.to_orc('/path/to/output/', ...)  # doctest: +SKIP

    See Also
    --------
    read_orc: Read ORC data to dask.dataframe
    """

    # Get engine
    engine = _get_engine(engine)

    if hasattr(path, "name"):
        path = stringify_path(path)
    fs, _, _ = get_fs_token_paths(path, mode="wb", storage_options=storage_options)
    # Trim any protocol information from the path before forwarding
    path = fs._strip_protocol(path)

    if not write_index:
        # Not writing index - might as well drop it
        df = df.reset_index(drop=True)

    # Use i_offset and df.npartitions to define file-name list.
    # For `append=True`, we use the total number of existing files
    # to define `i_offset`
    fs.mkdirs(path, exist_ok=True)
    i_offset = len(collect_files(path, fs)) if append else 0
    filenames = [f"part.{i+i_offset}.orc" for i in range(df.npartitions)]

    # Construct IO graph
    dsk = {}
    name = "to-orc-" + tokenize(
        df,
        fs,
        path,
        engine,
        write_index,
        partition_on,
        i_offset,
        storage_options,
    )
    part_tasks = []
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
            ],
        )
        part_tasks.append((name, d))
    final_name = "final-" + name
    dsk[(final_name, 0)] = (lambda x: None, part_tasks)
    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[df])

    # Compute or return future
    if compute:
        if compute_kwargs is None:
            compute_kwargs = dict()
        return compute_as_if_collection(DataFrame, graph, part_tasks, **compute_kwargs)
    return Scalar(graph, name, "")
