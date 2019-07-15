from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion

import toolz
import warnings

from ...core import DataFrame, new_dd_object
from ....bytes.compression import compress
from ....base import tokenize
from ....compatibility import PY3, string_types
from ....bytes.core import get_fs_token_paths
from ....bytes.utils import infer_storage_options
from ....utils import import_required, natural_sort_key

__all__ = ("read_parquet", "to_parquet")

# ----------------------------------------------------------------------
# User API


def read_parquet(
    path,
    columns=None,
    filters=None,
    categories=None,
    index=None,
    storage_options=None,
    engine="auto",
    gather_statistics=None,
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
    filters : list
        List of filters to apply, like ``[('x', '>', 0), ...]``. This
         implements row-group (partition) -level filtering only, i.e., to
        prevent the loading of some chunks of the data, and only if relevant
        statistics have been included in the metadata.
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
    engine : {'auto', 'fastparquet', 'pyarrow'}, default 'auto'
        Parquet reader library to use. If only one library is installed, it
        will use that one; if both, it will use 'fastparquet'
    gather_statistics : bool or None (default).
        Gather the statistics for each dataset partition. By default,
        this will only be done if the _metadata file is available. Otherwise,
        statistics will only be gathered if True, because the footer of
        every file will be parsed (which is very slow on some systems).

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
            [columns],
            filters,
            categories,
            index,
            storage_options,
            engine,
            gather_statistics,
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
    )

    if isinstance(engine, str):
        engine = get_engine(engine)

    fs, _, paths = get_fs_token_paths(path, mode="rb", storage_options=storage_options)

    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

    auto_index_allowed = False
    if index is None:
        # User is allowing auto-detected index
        auto_index_allowed = True

    index, meta, statistics, parts = engine.read_metadata(
        fs,
        paths,
        categories=categories,
        index=index,
        gather_statistics=gather_statistics,
        filters=filters,
    )

    ignore_index_column_intersection = False
    if columns is None:
        # User didn't specify columns, so ignore any intersection
        # of auto-detected values with the index (if necessary)
        ignore_index_column_intersection = True
        columns = meta.columns

    if not set(columns).issubset(set(meta.columns)):
        raise KeyError(
            "The following columns were not found in the dataset %s\n"
            "The following columns were found %s"
            % (set(columns) - set(meta.columns), meta.columns)
        )

    # Parse dataset statistics from metadata (if available)
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

        out = sorted_columns(statistics)

        if index and isinstance(index, str):
            index = [index]
        if index and out:
            # Only one valid column
            out = [o for o in out if o["name"] in index]
        if index is not False and len(out) == 1:
            # Use only sorted column with statistics as the index
            divisions = out[0]["divisions"]
            index = [out[0]["name"]]
        elif index is not False and len(out) > 1:
            if any(o["name"] == "index" for o in out):
                # Use sorted column named "index" as the index
                [o] = [o for o in out if o["name"] == "index"]
                divisions = o["divisions"]
                index = [o["name"]]
            else:
                # Multiple sorted columns found, cannot autodetect the index
                warnings.warn(
                    "Multiple sorted columns found, cannot autodetect index",
                    RuntimeWarning,
                )
                index = False
                divisions = [None] * (len(parts) + 1)
        else:
            divisions = [None] * (len(parts) + 1)
    else:
        divisions = [None] * (len(parts) + 1)

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
        meta = meta[columns + index]

    else:
        meta = meta[list(columns)]

    subgraph = {
        (name, i): (
            read_parquet_part,
            engine.read_partition,
            fs,
            meta,
            part["piece"],
            columns,
            index,
            part["kwargs"],
        )
        for i, part in enumerate(parts)
    }

    # Set the index that was previously treated as a column
    if index:
        meta = meta.set_index(index)

    if len(divisions) < 2:
        # empty dataframe - just use meta
        subgraph = {(name, 0): meta}
        divisions = (None, None)

    return new_dd_object(subgraph, name, meta, divisions)


def read_parquet_part(func, fs, meta, part, columns, index, kwargs):
    """ Read a part of a parquet dataset """
    kwargs["index"] = index
    df = func(fs, part, columns, **kwargs)
    if meta.columns.name:
        df.columns.name = meta.columns.name
    return df


def to_parquet(
    df,
    path,
    engine="auto",
    compression="default",
    write_index=True,
    append=False,
    ignore_divisions=False,
    partition_on=None,
    storage_options=None,
    write_metadata_file=True,
    compute=True,
    **kwargs
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
        Whether to create the special "_metadata" file.
    compute : bool, optional
        If True (default) then the result is computed immediately. If False
        then a ``dask.delayed`` object is returned for future computation.
    **kwargs
        Extra options to be passed on to the specific backend.

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> dd.to_parquet(df, '/path/to/output/',...)  # doctest: +SKIP

    See Also
    --------
    read_parquet: Read parquet data to dask.dataframe
    """
    from dask import delayed

    partition_on = partition_on or []
    if isinstance(partition_on, string_types):
        partition_on = [partition_on]

    if set(partition_on) - set(df.columns):
        raise ValueError(
            "Partitioning on non-existent column. "
            "partition_on=%s ."
            "columns=%s" % (str(partition_on), str(list(df.columns)))
        )

    if compression != "default":
        kwargs["compression"] = compression
    elif "snappy" in compress:
        kwargs["compression"] = "snappy"

    if isinstance(engine, str):
        engine = get_engine(engine)

    fs, _, _ = get_fs_token_paths(path, mode="wb", storage_options=storage_options)
    # Trim any protocol information from the path before forwarding
    # ideally, this should be done as a method of the file-system
    path = infer_storage_options(path)["path"]

    # Save divisions and corresponding index name. This is necessary,
    # because we may be resetting the index to write the file
    division_info = {"divisions": df.divisions, "name": df.index.name}
    if division_info["name"] is None:
        division_info["name"] = "index"

    # If write_index==True (default), reset the index and record the
    # name of the original index in `index_cols` (will be `index` if None).
    # `fastparquet` will use `index_cols` to specify the index column(s)
    # in the metadata.  `pyarrow` will revert the `reset_index` call
    # below if `index_cols` is populated (because pyarrow will want to handle
    # index preservation itself).  For both engines, the column index
    # will be written to "pandas metadata" if write_index=True
    index_cols = []
    if write_index:
        real_cols = set(df.columns)
        df = df.reset_index()
        index_cols = [c for c in set(df.columns).difference(real_cols)]

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
    meta, i_offset = engine.initialize_write(
        df,
        fs,
        path,
        append=append,
        ignore_divisions=ignore_divisions,
        partition_on=partition_on,
        division_info=division_info,
        index_cols=index_cols,
        **kwargs_pass
    )

    # Use i_offset and df.npartitions to define file-name list
    filenames = ["part.%i.parquet" % (i + i_offset) for i in range(df.npartitions)]

    # write parts
    dwrite = delayed(engine.write_partition)
    parts = [
        dwrite(
            d,
            path,
            fs,
            filename,
            partition_on,
            with_metadata=write_metadata_file,
            fmd=meta,
            index_cols=index_cols,
            **kwargs_pass
        )
        for d, filename in zip(df.to_delayed(), filenames)
    ]

    # single task to complete
    out = delayed(engine.write_metadata)(
        parts, meta, fs, path, append=append, **kwargs_pass
    )

    if compute:
        out = out.compute()
    return out


_ENGINES = {}


def get_engine(engine):
    """Get the parquet engine backend implementation.

    Parameters
    ----------
    engine : {'auto', 'fastparquet', 'pyarrow'}, default 'auto'
        Parquet reader library to use. Defaults to fastparquet if both are
        installed

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

    elif engine == "pyarrow" or engine == "arrow":
        pa = import_required("pyarrow", "`pyarrow` not installed")
        from .arrow import ArrowEngine

        if LooseVersion(pa.__version__) < "0.13.1":
            raise RuntimeError("PyArrow version >= 0.13.1 required")

        _ENGINES["pyarrow"] = eng = ArrowEngine
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
    """ Find sorted columns given row-group statistics

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
        success = True
        for stats in statistics[1:]:
            c = stats["columns"][i]
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
    """ Apply filters onto parts/statistics pairs

    Parameters
    ----------
    parts: list
        Tokens corresponding to row groups to read in the future
    statistics: List[dict]
        List of statistics for each part, including min and max values
    filters: List[Tuple[str, str, Any]]
        List like [('x', '>', 5), ('y', '==', 'Alice')]

    Returns
    -------
    parts, statistics: the same as the input, but possibly a subset
    """
    for column, operator, value in filters:
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
                ):
                    out_parts.append(part)
                    out_statistics.append(stats)

        parts, statistics = out_parts, out_statistics

    return parts, statistics


if PY3:
    DataFrame.to_parquet.__doc__ = to_parquet.__doc__
