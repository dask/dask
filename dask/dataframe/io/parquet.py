from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from toolz import partial

from ..core import DataFrame, Series
from ..utils import UNKNOWN_CATEGORIES
from ...base import tokenize, normalize_token
from ...compatibility import PY3
from ...delayed import delayed
from ...bytes.core import get_fs_paths_myopen

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

try:
    import pyarrow.parquet as pyarrow_parquet
except:
    pyarrow_parquet = False


def _meta_from_dtypes(to_read_columns, file_columns, file_dtypes):
    meta = pd.DataFrame({c: pd.Series([], dtype=d)
                        for (c, d) in file_dtypes.items()},
                        columns=[c for c in file_columns
                                 if c in file_dtypes])
    return meta[list(to_read_columns)]

# ----------------------------------------------------------------------
# Fastparquet interface


def _read_fastparquet(fs, paths, myopen, columns=None, filters=None,
                      categories=None, index=None, storage_options=None):
    if filters is None:
        filters = []

    if isinstance(columns, list):
        columns = tuple(columns)

    if len(paths) > 1:
        pf = fastparquet.ParquetFile(paths, open_with=myopen, sep=myopen.fs.sep)
    else:
        try:
            pf = fastparquet.ParquetFile(paths[0] + fs.sep + '_metadata',
                                         open_with=myopen,
                                         sep=fs.sep)
        except:
            pf = fastparquet.ParquetFile(paths[0], open_with=myopen, sep=fs.sep)

    check_column_names(pf.columns, categories)
    name = 'read-parquet-' + tokenize(pf, columns, categories)

    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.schema)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    if index is False:
        index_col = None
    elif index is None:
        index_col = pf._get_index()
    else:
        index_col = index

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

    if categories is None:
        categories = pf.categories
    dtypes = pf._dtypes(categories)

    meta = _meta_from_dtypes(all_columns, tuple(pf.columns + list(pf.cats)),
                             dtypes)

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
                       categories, pf.schema, pf.cats, pf.dtypes)
           for i, rg in enumerate(rgs)}

    if index_col:
        minmax = fastparquet.api.sorted_partitioned_columns(pf)
        if index_col in minmax:
            divisions = (list(minmax[index_col]['min']) +
                         [minmax[index_col]['max'][-1]])
            divisions = [divisions[i] for i, rg in enumerate(pf.row_groups)
                         if rg in rgs] + [divisions[-1]]
        else:
            divisions = (None,) * (len(rgs) + 1)
    else:
        divisions = (None,) * (len(rgs) + 1)

    if isinstance(divisions[0], np.datetime64):
        divisions = [pd.Timestamp(d) for d in divisions]

    if not dsk:
        # empty dataframe
        dsk = {(name, 0): meta}
        divisions = (None, None)

    return out_type(dsk, name, meta, divisions)


def _read_parquet_row_group(open, fn, index, columns, rg, series, categories,
                            schema, cs, dt, *args):
    if not isinstance(columns, (tuple, list)):
        columns = (columns,)
        series = True
    if index and index not in columns:
        columns = columns + type(columns)([index])
    df, views = _pre_allocate(rg.num_rows, columns, categories, index, cs, dt)
    read_row_group_file(fn, rg, columns, categories, schema, cs,
                        open=open, assign=views)

    if series:
        return df[df.columns[0]]
    else:
        return df


# ----------------------------------------------------------------------
# PyArrow interface

def _read_pyarrow(fs, paths, file_opener, columns=None, filters=None,
                  categories=None, index=None):
    api = pyarrow_parquet

    if filters is not None:
        raise NotImplemented("Predicate pushdown not implemented")

    if categories is not None:
        raise NotImplemented("Categorical reads not yet implemented")

    if isinstance(columns, tuple):
        columns = list(columns)

    dataset = api.ParquetDataset(paths)
    schema = dataset.schema.to_arrow_schema()
    task_name = 'read-parquet-' + tokenize(dataset, columns)

    if columns is None:
        all_columns = schema.names
    else:
        all_columns = columns

    if not isinstance(all_columns, list):
        out_type = Series
        all_columns = [all_columns]
    else:
        out_type = DataFrame

    divisions = (None,) * (len(dataset.pieces) + 1)

    dtypes = _get_pyarrow_dtypes(schema)

    meta = _meta_from_dtypes(all_columns, schema.names, dtypes)

    task_plan = {
        (task_name, i): (_read_arrow_parquet_piece,
                         file_opener,
                         piece, all_columns,
                         out_type == Series,
                         dataset.partitions)
        for i, piece in enumerate(dataset.pieces)
    }

    return out_type(task_plan, task_name, meta, divisions)


def _get_pyarrow_dtypes(schema):
    dtypes = {}
    for i in range(len(schema)):
        field = schema[i]
        numpy_dtype = field.type.to_pandas_dtype()
        dtypes[field.name] = numpy_dtype

    return dtypes


def _read_arrow_parquet_piece(open_file_func, piece, columns, is_series,
                              partitions):
    with open_file_func(piece.path, mode='rb') as f:
        table = piece.read(columns=columns,  partitions=partitions,
                           file=f)
    df = table.to_pandas()

    if is_series:
        return df[df.columns[0]]
    else:
        return df


# ----------------------------------------------------------------------
# User read API

def read_parquet(path, columns=None, filters=None, categories=None, index=None,
                 storage_options=None, engine='auto'):
    """
    Read ParquetFile into a Dask DataFrame

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

    Parameters
    ----------
    path : string
        Source directory for data. May be a glob string.
        Prepend with protocol like ``s3://`` or ``hdfs://`` for remote data.
    columns: list or None
        List of column names to load
    filters: list
        List of filters to apply, like ``[('x', '>' 0), ...]``
    index: string or None (default) or False
        Name of index column to use if that column is sorted;
        False to force dask to not use any column as the index
    categories: list, dict or None
        For any fields listed here, if the parquet encoding is Dictionary,
        the column will be created with dtype category. Use only if it is
        guaranteed that the column is encoded as dictionary in all row-groups.
        If a list, assumes up to 2**16-1 labels; if a dict, specify the number
        of labels expected; if None, will load categories automatically for
        data written by dask/fastparquet, not otherwise.
    storage_options : dict
        Key/value pairs to be passed on to the file-system backend, if any.
    engine : {'auto', 'fastparquet', 'arrow'}, default 'auto'
        Parquet reader library to use. If only one library is installed, it
        will use that one; if both, it will use 'fastparquet'

    Examples
    --------
    >>> df = read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """
    fs, paths, file_opener = get_fs_paths_myopen(path, None, 'rb',
                                                 **(storage_options or {}))

    if engine == 'auto':
        if fastparquet:
            engine = 'fastparquet'
        elif pyarrow_parquet:
            engine = 'arrow'
        else:
            raise ImportError("Please install either fastparquet or pyarrow")
    elif engine == 'fastparquet':
        if not fastparquet:
            raise ImportError("fastparquet not installed")
    elif engine == 'arrow':
        if not pyarrow_parquet:
            raise ImportError("pyarrow not installed")
    else:
        raise ValueError('Unsupported engine type: {0}'.format(engine))

    if engine == 'fastparquet':
        return _read_fastparquet(fs, paths, file_opener, columns=columns,
                                 filters=filters,
                                 categories=categories, index=index)
    else:
        return _read_pyarrow(fs, paths, file_opener, columns=columns,
                             filters=filters,
                             categories=categories, index=index)


def to_parquet(path, df, compression=None, write_index=None, has_nulls=True,
               fixed_text=None, object_encoding=None, storage_options=None,
               append=False, ignore_divisions=False, partition_on=None,
               compute=True):
    """Store Dask.dataframe to Parquet files

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
    has_nulls : bool, list or 'infer'
        Specifies whether to write NULLs information for columns. If bools,
        apply to all columns, if list, use for only the named columns, if
        'infer', use only for columns which don't have a sentinel NULL marker
        (currently object columns only).
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
        If False, construct data-set from scratch; if True, add new
        row-group(s) to existing data-set. In the latter case, the data-set
        must exist, and the schema must match the input data.
    ignore_divisions: bool (False)
        If False raises error when previous divisions overlap with the new
        appended divisions. Ignored if append=False.
    partition_on: list
        Construct directory-based partitioning by splitting on these fields'
        values. Each dask partition will result in one or more datafiles,
        there will be no global groupby.
    compute: bool (True)
        If true (default) then we compute immediately.
        If False then we return a dask.delayed object for future computation.

    This uses the fastparquet project:
    http://fastparquet.readthedocs.io/en/latest

    Examples
    --------
    >>> df = dd.read_csv(...)  # doctest: +SKIP
    >>> to_parquet('/path/to/output/', df, compression='SNAPPY')  # doctest: +SKIP

    See Also
    --------
    read_parquet: Read parquet data to dask.dataframe
    """
    import fastparquet
    partition_on = partition_on or []

    fs, paths, myopen = get_fs_paths_myopen(path, None, 'wb',
                                            **(storage_options or {}))
    fs.mkdirs(path)
    sep = fs.sep
    metadata_fn = sep.join([path, '_metadata'])

    if write_index is True or write_index is None and df.known_divisions:
        new_divisions = df.divisions
        df = df.reset_index()
        index_col = df.columns[0]
    else:
        ignore_divisions = True
        index_col = None

    object_encoding = object_encoding or 'utf8'
    if object_encoding == 'infer' or (isinstance(object_encoding, dict) and
                                      'infer' in object_encoding.values()):
        raise ValueError('"infer" not allowed as object encoding, '
                         'because this required data in memory.')
    if set(partition_on) - set(df.columns):
        raise ValueError('Partitioning on non-existent column')
    fmd = fastparquet.writer.make_metadata(
        df._meta, has_nulls=has_nulls, fixed_text=fixed_text,
        object_encoding=object_encoding, index_cols=[index_col],
        ignore_columns=partition_on)

    if append:
        pf = fastparquet.api.ParquetFile(path, open_with=myopen, sep=sep)
        if pf.file_scheme != 'hive':
            raise ValueError('Requested file scheme is hive, '
                             'but existing file scheme is not.')
        elif set(pf.columns) != set(df.columns):
            raise ValueError('Appended columns not the same.\n'
                             'New: {} | Previous: {}'
                             .format(pf.columns, list(df.columns)))
        elif set(pf.dtypes.items()) != set(df.dtypes.iteritems()):
            raise ValueError('Appended dtypes differ.\n{}'
                             .format(set(pf.dtypes.items()) ^
                                     set(df.dtypes.iteritems())))
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

    if partition_on:
        writes = [delayed(fastparquet.writer.partition_on_columns)(
            partition, partition_on, path, filename, fmd, sep,
            compression, fs.open, fs.mkdirs)
            for filename, partition in zip(filenames, partitions)]
    else:
        writes = [delayed(fastparquet.writer.make_part_file)(
                  myopen(outfile, 'wb'), partition, fmd.schema,
                  compression=compression)
                  for outfile, partition in zip(outfiles, partitions)]

    if compute:
        writes = delayed(writes).compute()
        _write_metadata(writes, filenames, fmd, path, metadata_fn, myopen, sep)
    else:
        out = delayed(_write_metadata)(writes, filenames, fmd, path, metadata_fn,
                                       myopen, sep)
        return out


def _write_metadata(writes, filenames, fmd, path, metadata_fn, myopen, sep):
    """ Write Parquet metadata after writing all row groups

    See Also
    --------
    to_parquet
    """
    for fn, rg in zip(filenames, writes):
        if rg is not None:
            if isinstance(rg, list):
                for r in rg:
                    fmd.row_groups.append(r)
            else:
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


try:
    from pyarrow.parquet import ParquetDataset
except ImportError:
    pass
else:
    @partial(normalize_token.register, ParquetDataset)
    def normalize_PyArrowParquetDataset(ds):
        return (type(ds), ds.paths)


if PY3:
    DataFrame.to_parquet.__doc__ = to_parquet.__doc__
