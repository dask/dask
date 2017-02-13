import pandas as pd
from toolz import first, partial

from ..core import DataFrame, Series
from ..utils import UNKNOWN_CATEGORIES
from ...base import tokenize, normalize_token
from ...compatibility import PY3
from ...delayed import delayed
from ...bytes.core import OpenFileCreator

try:
    import fastparquet
    from fastparquet import parquet_thrift
    from fastparquet.core import read_row_group_file
    from fastparquet.api import _pre_allocate
    from fastparquet.util import check_column_names
    default_encoding = parquet_thrift.Encoding.PLAIN
except:
    fastparquet = False
    default_encoding = None


def read_parquet(path, columns=None, filters=None, categories=None, index=None,
                 storage_options=None):
    """
    Read ParquetFile into a Dask DataFrame

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

    This uses the fastparquet project: http://fastparquet.readthedocs.io/en/latest

    Parameters
    ----------
    path : string
        Source directory for data.
        Prepend with protocol like ``s3://`` or ``hdfs://`` for remote data.
    columns: list or None
        List of column names to load
    filters: list
        List of filters to apply, like ``[('x', '>' 0), ...]``
    index: string or None
        Name of index column to use if that column is sorted
    categories: list or None
        For any fields listed here, if the parquet encoding is Dictionary,
        the column will be created with dtype category. Use only if it is
        guaranteed that the column is encoded as dictionary in all row-groups.
    storage_options : dict
        Key/value pairs to be passed on to the file-system backend, if any.

    Examples
    --------
    >>> df = read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """
    if fastparquet is False:
        raise ImportError("fastparquet not installed")
    if filters is None:
        filters = []
    myopen = OpenFileCreator(path, compression=None, text=False,
                             **(storage_options or {}))

    if isinstance(columns, list):
        columns = tuple(columns)

    try:
        pf = fastparquet.ParquetFile(path + myopen.fs.sep + '_metadata',
                                     open_with=myopen,
                                     sep=myopen.fs.sep)
    except:
        pf = fastparquet.ParquetFile(path, open_with=myopen, sep=myopen.fs.sep)

    check_column_names(pf.columns, categories)
    categories = categories or []
    name = 'read-parquet-' + tokenize(pf, columns, categories)

    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.helper)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    # Find an index among the partially sorted columns
    minmax = fastparquet.api.sorted_partitioned_columns(pf)

    if index is False:
        index_col = None
    elif len(minmax) == 1:
        index_col = first(minmax)
    elif len(minmax) > 1:
        if index:
            index_col = index
        elif 'index' in minmax:
            index_col = 'index'
        else:
            raise ValueError("Multiple possible indexes exist: %s.  "
                             "Please select one with index='index-name'"
                             % sorted(minmax))
    else:
        index_col = None

    if columns is None:
        all_columns = tuple(pf.columns + list(pf.cats))
    else:
        all_columns = columns
    if not isinstance(all_columns, tuple):
        out_type = Series
        all_columns = (all_columns,)
    else:
        out_type = DataFrame
    if index_col and index_col not in all_columns:
        all_columns = all_columns + (index_col,)

    dtypes = {k: ('category' if k in categories else v) for k, v in
              pf.dtypes.items() if k in all_columns}

    meta = pd.DataFrame({c: pd.Series([], dtype=d)
                        for (c, d) in dtypes.items()},
                        columns=[c for c in pf.columns if c in dtypes])

    for cat in categories:
        meta[cat] = pd.Series(pd.Categorical([],
                              categories=[UNKNOWN_CATEGORIES]))

    if index_col:
        meta = meta.set_index(index_col)

    if out_type == Series:
        assert len(meta.columns) == 1
        meta = meta[meta.columns[0]]

    dsk = {(name, i): (_read_parquet_row_group, myopen, pf.row_group_filename(rg),
                       index_col, all_columns, rg, out_type == Series,
                       categories, pf.helper, pf.cats, pf.dtypes)
           for i, rg in enumerate(rgs)}

    if index_col:
        divisions = list(minmax[index_col]['min']) + [minmax[index_col]['max'][-1]]
    else:
        divisions = (None,) * (len(rgs) + 1)

    return out_type(dsk, name, meta, divisions)


def _read_parquet_row_group(open, fn, index, columns, rg, series, categories,
                            helper, cs, dt, *args):
    if not isinstance(columns, (tuple, list)):
        columns = (columns,)
        series = True
    if index and index not in columns:
        columns = columns + type(columns)([index])
    df, views = _pre_allocate(rg.num_rows, columns, categories, index, cs, dt)
    read_row_group_file(fn, rg, columns, categories, helper, cs,
                        open=open, assign=views)

    if series:
        return df[df.columns[0]]
    else:
        return df


def to_parquet(path, df, compression=None, write_index=None, has_nulls=None,
               fixed_text=None, object_encoding=None, storage_options=None,
               append=False, ignore_divisions=False):
    """
    Store Dask.dataframe to Parquet files

    Notes
    -----
    Each partition will be written to a separate file.

    Parameters
    ----------
    path : string
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    df : Dask.dataframe
    compression : string or dict
        Either a string like "SNAPPY" or a dictionary mapping column names to
        compressors like ``{"name": "GZIP", "values": "SNAPPY"}``
    write_index : boolean
        Whether or not to write the index.  Defaults to True *if* divisions are
        known.
    has_nulls : bool, list or None
        Specifies whether to write NULLs information for columns. If bools,
        apply to all columns, if list, use for only the named columns, if None,
        use only for columns which don't have a sentinel NULL marker (currently
        object columns only).
    fixed_text : dict {col: int}
        For column types that are written as bytes (bytes, utf8 strings, or
        json and bson-encoded objects), if a column is included here, the
        data will be written in fixed-length format, which should be faster
        but can potentially result in truncation.
    object_encoding : dict {col: bytes|utf8|json|bson} or str
        For object columns, specify how to encode to bytes. If a str, same
        encoding is applied to all object columns.
    storage_options : dict
        Key/value pairs to be passed on to the file-system backend, if any.
    append: bool (False)
        If False, construct data-set from scratch; if True, add new row-group(s)
        to existing data-set. In the latter case, the data-set must exist,
        and the schema must match the input data.
    ignore_divisions: bool (False)
        If False raises error when previous divisions overlap with the new
        appended divisions. Ignored if append=False.

    This uses the fastparquet project: http://fastparquet.readthedocs.io/en/latest

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> to_parquet('/path/to/output/', df, compression='SNAPPY')  # doctest: +SKIP

    See Also
    --------
    read_parquet: Read parquet data to dask.dataframe
    """
    if fastparquet is False:
        raise ImportError("fastparquet not installed")

    myopen = OpenFileCreator(path, compression=None, text=False,
                             **(storage_options or {}))
    myopen.fs.mkdirs(path)
    sep = myopen.fs.sep
    metadata_fn = sep.join([path, '_metadata'])

    if write_index is True or write_index is None and df.known_divisions:
        new_divisions = df.divisions
        df = df.reset_index()
        index_col = df.columns[0]
    else:
        ignore_divisions = True

    object_encoding = object_encoding or 'utf8'
    if object_encoding == 'infer' or (isinstance(object_encoding, dict) and
                                      'infer' in object_encoding.values()):
        raise ValueError('"infer" not allowed as object encoding, '
                         'because this required data in memory.')
    fmd = fastparquet.writer.make_metadata(df._meta, has_nulls=has_nulls,
                                           fixed_text=fixed_text,
                                           object_encoding=object_encoding)

    if append:
        pf = fastparquet.api.ParquetFile(path, open_with=myopen)
        if pf.file_scheme != 'hive':
            raise ValueError('Requested file scheme is hive, '
                             'but existing file scheme is not.')
        elif set(pf.columns) != set(df.columns):
            raise ValueError('Appended columns not the same.\n'
                             'New: {} | Previous: {}'
                             .format(pf.columns, list(df.columns)))
        elif set(pf.dtypes.items()) != set(df.dtypes.items()):
            raise ValueError('Appended dtypes differ.\n{}'
                             .format(set(pf.dtypes.items()) ^
                                     set(df.dtypes.items())))
        # elif fmd.schema != pf.fmd.schema:
        #    raise ValueError('Appended schema differs.')
        else:
            df = df[pf.columns]

        fmd = pf.fmd
        i_offset = fastparquet.writer.find_max_part(fmd.row_groups)

        if not ignore_divisions:
            minmax = fastparquet.api.sorted_partitioned_columns(pf)
            divisions = list(minmax[index_col]['min']) + [
                minmax[index_col]['max'][-1]]

            if new_divisions[0] < divisions[-1]:
                raise ValueError(
                    'Appended divisions overlapping with the previous ones.\n'
                    'New: {} | Previous: {}'
                    .format(divisions[-1], new_divisions[0]))
    else:
        i_offset = 0

    partitions = df.to_delayed()
    filenames = ['part.%i.parquet' % i
                 for i in range(i_offset, len(partitions) + i_offset)]
    outfiles = [sep.join([path, fn]) for fn in filenames]

    writes = [delayed(fastparquet.writer.make_part_file)(
              myopen(outfile, 'wb'), partition, fmd.schema,
              compression=compression)
              for outfile, partition in zip(outfiles, partitions)]

    out = delayed(writes).compute()

    for fn, rg in zip(filenames, out):
        for chunk in rg.columns:
            chunk.file_path = fn
        fmd.row_groups.append(rg)

    fastparquet.writer.write_common_metadata(metadata_fn, fmd, open_with=myopen,
                                             no_row_groups=False)

    fn = sep.join([path, '_common_metadata'])
    fastparquet.writer.write_common_metadata(fn, fmd, open_with=myopen)


if fastparquet:
    @partial(normalize_token.register, fastparquet.ParquetFile)
    def normalize_ParquetFile(pf):
        return (type(pf), pf.fn, pf.sep) + normalize_token(pf.open)


if PY3:
    DataFrame.to_parquet.__doc__ = to_parquet.__doc__
