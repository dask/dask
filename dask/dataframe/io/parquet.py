from __future__ import absolute_import, division, print_function

import copy
import json
import warnings

import numpy as np
import pandas as pd
from toolz import unique

from ..core import DataFrame, Series
from ..utils import UNKNOWN_CATEGORIES
from ...base import tokenize, normalize_token
from ...compatibility import PY3
from ...delayed import delayed
from ...bytes.core import get_fs_paths_myopen

__all__ = ('read_parquet', 'to_parquet')


def _meta_from_dtypes(to_read_columns, file_columns, file_dtypes, index_cols):
    meta = pd.DataFrame({c: pd.Series([], dtype=d)
                         for (c, d) in file_dtypes.items()},
                        columns=[c for c in file_columns
                                 if c in file_dtypes])
    df = meta[list(to_read_columns)]

    if not index_cols:
        return df
    if not isinstance(index_cols, list):
        index_cols = [index_cols]
    df = df.set_index(index_cols)
    if len(index_cols) == 1 and index_cols[0] == '__index_level_0__':
        df.index.name = None
    return df


# ----------------------------------------------------------------------
# Fastparquet interface


def _read_fastparquet(fs, paths, myopen, columns=None, filters=None,
                      categories=None, index=None, storage_options=None):
    import fastparquet
    from fastparquet.util import check_column_names
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
        except Exception:
            pf = fastparquet.ParquetFile(paths[0], open_with=myopen, sep=fs.sep)

    check_column_names(pf.columns, categories)
    name = 'read-parquet-' + tokenize(pf, columns, categories)

    rgs = [rg for rg in pf.row_groups if
           not (fastparquet.api.filter_out_stats(rg, filters, pf.schema)) and
           not (fastparquet.api.filter_out_cats(rg, filters))]

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
                             dtypes, index_col)

    for cat in categories:
        if cat in meta:
            meta[cat] = pd.Series(pd.Categorical([],
                                                 categories=[UNKNOWN_CATEGORIES]),
                                  index=meta.index)

    if out_type == Series:
        assert len(meta.columns) == 1
        meta = meta[meta.columns[0]]

    dsk = {(name, i): (_read_parquet_row_group, myopen, pf.row_group_filename(rg),
                       index_col, all_columns, rg, out_type == Series,
                       categories, pf.schema, pf.cats, pf.dtypes,
                       pf.file_scheme)
           for i, rg in enumerate(rgs)}

    if not dsk:
        # empty dataframe
        dsk = {(name, 0): meta}
        divisions = (None, None)
        return out_type(dsk, name, meta, divisions)

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

    return out_type(dsk, name, meta, divisions)


def _read_parquet_row_group(open, fn, index, columns, rg, series, categories,
                            schema, cs, dt, scheme, *args):
    from fastparquet.api import _pre_allocate
    from fastparquet.core import read_row_group_file
    if not isinstance(columns, (tuple, list)):
        columns = (columns,)
        series = True
    if index and index not in columns:
        columns = columns + type(columns)([index])
    df, views = _pre_allocate(rg.num_rows, columns, categories, index, cs, dt)
    read_row_group_file(fn, rg, columns, categories, schema, cs,
                        open=open, assign=views, scheme=scheme)

    if series:
        return df[df.columns[0]]
    else:
        return df


def _write_partition_fastparquet(df, fs, path, filename, fmd, compression,
                                 partition_on):
    from fastparquet.writer import partition_on_columns, make_part_file
    # Fastparquet mutates this in a non-threadsafe manner. For now we just copy
    # it before forwarding to fastparquet.
    fmd = copy.copy(fmd)
    if not len(df):
        # Write nothing for empty partitions
        rgs = None
    elif partition_on:
        rgs = partition_on_columns(df, partition_on, path, filename, fmd,
                                   fs.sep, compression, fs.open, fs.mkdirs)
    else:
        # Fastparquet current doesn't properly set `num_rows` in the output
        # metadata. Set it here to fix that.
        fmd.num_rows = len(df)
        with fs.open(fs.sep.join([path, filename]), 'wb') as fil:
            rgs = make_part_file(fil, df, fmd.schema, compression=compression,
                                 fmd=fmd)
    return rgs


def _write_fastparquet(df, path, write_index=None, append=False,
                       ignore_divisions=False, partition_on=None,
                       storage_options=None, compression=None, **kwargs):
    import fastparquet

    fs, paths, open_with = get_fs_paths_myopen(path, None, 'wb',
                                               **(storage_options or {}))
    fs.mkdirs(path)
    sep = fs.sep

    object_encoding = kwargs.pop('object_encoding', 'utf8')
    if object_encoding == 'infer' or (isinstance(object_encoding, dict) and 'infer' in object_encoding.values()):
        raise ValueError('"infer" not allowed as object encoding, '
                         'because this required data in memory.')

    divisions = df.divisions
    if write_index is True or write_index is None and df.known_divisions:
        df = df.reset_index()
        index_cols = [df.columns[0]]
    else:
        ignore_divisions = True
        index_cols = []

    if append:
        pf = fastparquet.api.ParquetFile(path, open_with=open_with, sep=sep)
        if pf.file_scheme not in ['hive', 'empty', 'flat']:
            raise ValueError('Requested file scheme is hive, '
                             'but existing file scheme is not.')
        elif ((set(pf.columns) != set(df.columns) - set(partition_on)) or (set(partition_on) != set(pf.cats))):
            raise ValueError('Appended columns not the same.\n'
                             'New: {} | Previous: {}'
                             .format(pf.columns, list(df.columns)))
        elif set(pf.dtypes[c] for c in pf.columns) != set(df[pf.columns].dtypes):
            raise ValueError('Appended dtypes differ.\n{}'
                             .format(set(pf.dtypes.items()) ^
                                     set(df.dtypes.iteritems())))
        else:
            df = df[pf.columns + partition_on]

        fmd = pf.fmd
        i_offset = fastparquet.writer.find_max_part(fmd.row_groups)

        if not ignore_divisions:
            minmax = fastparquet.api.sorted_partitioned_columns(pf)
            old_end = minmax[index_cols[0]]['max'][-1]
            if divisions[0] < old_end:
                raise ValueError(
                    'Appended divisions overlapping with the previous ones.\n'
                    'New: {} | Previous: {}'.format(old_end, divisions[0]))
    else:
        fmd = fastparquet.writer.make_metadata(df._meta,
                                               object_encoding=object_encoding,
                                               index_cols=index_cols,
                                               ignore_columns=partition_on,
                                               **kwargs)
        i_offset = 0

    filenames = ['part.%i.parquet' % (i + i_offset)
                 for i in range(df.npartitions)]

    write = delayed(_write_partition_fastparquet)
    writes = [write(part, fs, path, filename, fmd, compression, partition_on)
              for filename, part in zip(filenames, df.to_delayed())]

    return delayed(_write_metadata)(writes, filenames, fmd, path, open_with, sep)


def _write_metadata(writes, filenames, fmd, path, open_with, sep):
    """ Write Parquet metadata after writing all row groups

    See Also
    --------
    to_parquet
    """
    import fastparquet
    fmd = copy.copy(fmd)
    for fn, rg in zip(filenames, writes):
        if rg is not None:
            if isinstance(rg, list):
                for r in rg:
                    fmd.row_groups.append(r)
            else:
                for chunk in rg.columns:
                    chunk.file_path = fn
                fmd.row_groups.append(rg)

    fn = sep.join([path, '_metadata'])
    fastparquet.writer.write_common_metadata(fn, fmd, open_with=open_with,
                                             no_row_groups=False)

    fn = sep.join([path, '_common_metadata'])
    fastparquet.writer.write_common_metadata(fn, fmd, open_with=open_with)


# ----------------------------------------------------------------------
# PyArrow interface


def _read_pyarrow(fs, paths, file_opener, columns=None, filters=None,
                  categories=None, index=None):
    from ...bytes.core import get_pyarrow_filesystem
    import pyarrow.parquet as pq

    if filters is not None:
        raise NotImplementedError("Predicate pushdown not implemented")

    if categories is not None:
        raise NotImplementedError("Categorical reads not yet implemented")

    if isinstance(columns, tuple):
        columns = list(columns)

    dataset = pq.ParquetDataset(paths, filesystem=get_pyarrow_filesystem(fs))
    schema = dataset.schema.to_arrow_schema()
    has_pandas_metadata = schema.metadata is not None and b'pandas' in schema.metadata
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

    if index is False:
        index_cols = []
    elif index is None:
        if has_pandas_metadata:
            pandas_metadata = json.loads(schema.metadata[b'pandas'].decode('utf8'))
            index_cols = pandas_metadata.get('index_columns', [])
        else:
            index_cols = []
    else:
        index_cols = index if isinstance(index, list) else [index]

    if index_cols:
        all_columns = list(unique(all_columns + index_cols))

    dtypes = _get_pyarrow_dtypes(schema)

    meta = _meta_from_dtypes(all_columns, schema.names, dtypes, index_cols)

    if out_type == Series:
        assert len(meta.columns) == 1
        meta = meta[meta.columns[0]]

    if dataset.pieces:
        divisions = (None,) * (len(dataset.pieces) + 1)
        task_plan = {
            (task_name, i): (_read_pyarrow_parquet_piece,
                             file_opener,
                             piece,
                             all_columns,
                             index_cols,
                             out_type == Series,
                             dataset.partitions)
            for i, piece in enumerate(dataset.pieces)
        }
    else:
        divisions = (None, None)
        task_plan = {(task_name, 0): meta}

    return out_type(task_plan, task_name, meta, divisions)


def _get_pyarrow_dtypes(schema):
    dtypes = {}
    for i in range(len(schema)):
        field = schema[i]
        numpy_dtype = field.type.to_pandas_dtype()
        dtypes[field.name] = numpy_dtype

    return dtypes


def _read_pyarrow_parquet_piece(open_file_func, piece, columns, index_cols,
                                is_series, partitions):
    with open_file_func(piece.path, mode='rb') as f:
        table = piece.read(columns=columns, partitions=partitions,
                           use_pandas_metadata=True,
                           file=f)
    df = table.to_pandas()
    if (index_cols and df.index.name is None and
            len(df.columns.intersection(index_cols))):
        # Index should be set, but it isn't
        df = df.set_index(index_cols)
    elif not index_cols and df.index.name is not None:
        # Index shouldn't be set, but it is
        df = df.reset_index(drop=False)

    if is_series:
        return df[df.columns[0]]
    else:
        return df


def _write_pyarrow(df, path, write_index=None, append=False,
                   ignore_divisions=False, partition_on=None,
                   storage_options=None, **kwargs):
    if append:
        raise NotImplementedError("`append` not implemented for "
                                  "`engine='pyarrow'`")

    if write_index is None and df.known_divisions:
        write_index = True

    fs, paths, open_with = get_fs_paths_myopen(path, None, 'wb',
                                               **(storage_options or {}))
    fs.mkdirs(path)

    template = fs.sep.join([path, 'part.%i.parquet'])

    write = delayed(_write_partition_pyarrow)
    first_kwargs = kwargs.copy()
    first_kwargs['metadata_path'] = fs.sep.join([path, '_common_metadata'])
    writes = [write(part, open_with, path, fs, template % i, write_index, partition_on,
                    **(kwargs if i else first_kwargs))
              for i, part in enumerate(df.to_delayed())]
    return delayed(writes)


def _write_partition_pyarrow(df, open_with, path, fs, filename, write_index,
                             partition_on, metadata_path=None, **kwargs):
    import pyarrow as pa
    from pyarrow import parquet
    t = pa.Table.from_pandas(df, preserve_index=write_index)

    if partition_on:
        parquet.write_to_dataset(t, path, partition_cols=partition_on, filesystem=fs)
    else:
        with open_with(filename, 'wb') as fil:
            parquet.write_table(t, fil, **kwargs)

    if metadata_path is not None:
        with open_with(metadata_path, 'wb') as fil:
            kwargs.pop('compression', None)
            parquet.write_metadata(t.schema, fil, **kwargs)


# ----------------------------------------------------------------------
# User API


_ENGINES = {}


def get_engine(engine):
    """Get the parquet engine backend implementation.

    Parameters
    ----------
    engine : {'auto', 'fastparquet', 'pyarrow'}, default 'auto'
        Parquet reader library to use. Default is first installed in this list.

    Returns
    -------
    A dict containing a ``'read'`` and ``'write'`` function.
    """
    if engine in _ENGINES:
        return _ENGINES[engine]

    if engine == 'auto':
        for eng in ['fastparquet', 'pyarrow']:
            try:
                return get_engine(eng)
            except ImportError:
                pass
        else:
            raise ImportError("Please install either fastparquet or pyarrow")

    elif engine == 'fastparquet':
        try:
            import fastparquet
        except ImportError:
            raise ImportError("fastparquet not installed")

        @normalize_token.register(fastparquet.ParquetFile)
        def normalize_ParquetFile(pf):
            return (type(pf), pf.fn, pf.sep) + normalize_token(pf.open)

        _ENGINES['fastparquet'] = eng = {'read': _read_fastparquet,
                                         'write': _write_fastparquet}
        return eng

    elif engine == 'pyarrow':
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow not installed")

        @normalize_token.register(pq.ParquetDataset)
        def normalize_PyArrowParquetDataset(ds):
            return (type(ds), ds.paths)

        _ENGINES['pyarrow'] = eng = {'read': _read_pyarrow,
                                     'write': _write_pyarrow}
        return eng

    elif engine == 'arrow':
        warnings.warn("parquet with `engine='arrow'` is deprecated, "
                      "use `engine='pyarrow'` instead")
        return get_engine('pyarrow')

    else:
        raise ValueError('Unsupported engine type: {0}'.format(engine))


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
        List of filters to apply, like ``[('x', '>', 0), ...]``. This implements
        row-group (partition) -level filtering only, i.e., to prevent the
        loading of some chunks of the data, and only if relevant statistics
        have been included in the metadata.
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
    engine : {'auto', 'fastparquet', 'pyarrow'}, default 'auto'
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

    read = get_engine(engine)['read']

    return read(fs, paths, file_opener, columns=columns, filters=filters,
                categories=categories, index=index)


def to_parquet(df, path, engine='auto', compression='default', write_index=None,
               append=False, ignore_divisions=False, partition_on=None,
               storage_options=None, compute=True, **kwargs):
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
    # TODO: remove once deprecation cycle is finished
    if isinstance(path, DataFrame):
        warnings.warn("DeprecationWarning: The order of `df` and `path` in "
                      "`dd.to_parquet` has switched, please update your code")
        df, path = path, df

    partition_on = partition_on or []

    if set(partition_on) - set(df.columns):
        raise ValueError('Partitioning on non-existent column')

    if compression != 'default':
        kwargs['compression'] = compression

    write = get_engine(engine)['write']

    out = write(df, path, write_index=write_index, append=append,
                ignore_divisions=ignore_divisions, partition_on=partition_on,
                storage_options=storage_options, **kwargs)

    if compute:
        out.compute()
        return None
    return out


if PY3:
    DataFrame.to_parquet.__doc__ = to_parquet.__doc__
