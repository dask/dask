from __future__ import absolute_import, division, print_function

from distutils.version import LooseVersion
import warnings

import toolz

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
    gather_statistics=True,
    infer_divisions=False,
):
    """
    Read ParquetFile into a Dask DataFrame

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

    Parameters
    ----------
    path : string, list or fastparquet.ParquetFile
        Source directory for data, or path(s) to individual parquet files.
        Prefix with a protocol like ``s3://`` to read from alternative
        filesystems. To read from multiple files you can pass a globstring or a
        list of paths, with the caveat that they must all have the same
        protocol.
        Alternatively, also accepts a previously opened
        fastparquet.ParquetFile()
    columns : string, list or None (default)
        Field name(s) to read in as columns in the output. By default all
        non-index fields will be read (as determined by the pandas parquet
        metadata, if present). Provide a single field name instead of a list to
        read in the data as a Series.
    filters : list
        List of filters to apply, like ``[('x', '>', 0), ...]``. This implements
        row-group (partition) -level filtering only, i.e., to prevent the
        loading of some chunks of the data, and only if relevant statistics
        have been included in the metadata.
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
    infer_divisions : bool or None (default).
        By default, divisions are inferred if the read `engine` supports
        doing so efficiently and the `index` of the underlying dataset is
        sorted across the individual parquet files. Set to ``True`` to
        force divisions to be inferred in all cases. Note that this may
        require reading metadata from each file in the dataset, which may
        be expensive. Set to ``False`` to never infer divisions.

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

    fs, fs_token, paths = get_fs_token_paths(
        path, mode="rb", storage_options=storage_options
    )

    paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

    meta, statistics, parts = engine.read_metadata(
        fs,
        fs_token,
        paths,
        categories=categories,
        index=index,
        gather_statistics=gather_statistics,
    )

    if columns is None:
        columns = meta.columns

    if not set(columns).issubset(set(meta.columns)):
        raise KeyError(
            "The following columns were not found in the dataset %s\n"
            "The following columns were found %s"
            % (set(columns) - set(meta.columns), meta.columns)
        )

    if statistics:
        result = list(zip(
            *[
                (part, stats)
                for part, stats in zip(parts, statistics)
                if stats["num-rows"] > 0
            ]
        ))
        if result:
            parts, statistics = result
        else:
            parts, statistics = [], []
        if filters:
            parts, statistics = apply_filters(parts, statistics, filters)

        out = sorted_columns(statistics)

        if index and out:
            out = [o for o in out if o["name"] == index]  # only one valid column
        if index is not False and len(out) == 1:
            divisions = out[0]["divisions"]
            index = out[0]["name"]
        elif index is not False and len(out) > 1:
            if any(o["name"] == "index" for o in out):
                [o] = [o for o in out if o["name"] == "index"]
                divisions = o["divisions"]
                index = o["name"]
            else:
                warnings.warn(
                    "Multiple sorted columns found: "
                    + ", ".join(o["name"] for o in out)
                )
                divisions = [None] * (len(parts) + 1)
        else:
            divisions = [None] * (len(parts) + 1)
    else:
        divisions = [None] * (len(parts) + 1)

    if index and index not in columns:
        columns = list(columns) + [index]

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
    if index:
        meta = meta.set_index(index)

    return new_dd_object(subgraph, name, meta, divisions)


def read_parquet_part(func, fs, meta, part, columns, index, kwargs):
    """ Read a part of a parquet dataset """
    df = func(fs, part, columns, **kwargs)
    if not len(df):
        df = meta
    if index:
        df = df.set_index(index)
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
    path : string
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
        Whether or not to write the index. Defaults to True *if* divisions are
        known.
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
        Whether to create the special "_metadata" file,
    compute : bool, optional
        If True (default) then the result is computed immediately. If False
        then a ``dask.delayed`` object is returned for future computation.
    **kwargs
        Extra options to be passed on to the specific backend.

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> to_parquet('/path/to/output/', df, compression='snappy')  # doctest: +SKIP

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

    fs, fs_token, _ = get_fs_token_paths(
        path, mode="wb", storage_options=storage_options
    )
    # Trim any protocol information from the path before forwarding
    # ideally, this should be done as a method of the file-system
    path = infer_storage_options(path)["path"]

    if write_index:
        df = df.reset_index()

    # create parquet metadata, includes loading of existing stuff is appending
    meta, filenames = engine.create_metadata(df, fs, path, append=append,
             ignore_divisions=ignore_divisions, partition_on=partition_on)

    # write parts
    dwrite = delayed(engine.write_partition)
    parts = [dwrite(
        d, path, fs, filename, partition_on, with_metadata=write_metadata_file,
        fmd=meta, **kwargs)
        for d, filename in zip(df.to_delayed(), filenames)]

    # single task to complete
    out = delayed(engine.write_metadata)(parts, meta, fs, path, **kwargs)

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
        for eng in ["pyarrow", "fastparquet"]:
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

        if LooseVersion(pa.__version__) < "0.8.0":
            raise RuntimeError("PyArrow version >= 0.8.0 required")

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
