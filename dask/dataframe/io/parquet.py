from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from toolz import first, partial

from ..core import DataFrame, Series
from ..utils import UNKNOWN_CATEGORIES
from ...base import tokenize, normalize_token
from ...compatibility import PY3
from ...delayed import delayed
from ...bytes.core import get_fs_paths_myopen

_REGISTERED_FASTPARQUET = False


class GenericParquetReader(object):

    def _meta_from_dtypes(self, to_read_columns, file_columns, file_dtypes):
        meta = pd.DataFrame({c: pd.Series([], dtype=d)
                            for (c, d) in file_dtypes.items()},
                            columns=[c for c in file_columns
                                     if c in file_dtypes])
        return meta[list(to_read_columns)]


# ----------------------------------------------------------------------
# Fastparquet interface


class FastparquetReader(GenericParquetReader):
    """
    Read ParquetFile into a Dask DataFrame

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

    This uses the fastparquet project:

        http://fastparquet.readthedocs.io/en/latest

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

    Examples
    --------
    >>> df = read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """
    _REGISTERED_TOKENIZE = False

    def __init__(self, fs, paths, file_opener, columns=None, filters=None,
                 categories=None, index=None):
        import fastparquet  # noqa
        self._ensure_registered()

        self.fs = fs
        self.paths = paths
        self.file_opener = file_opener

        self.filters = filters or []

        if isinstance(columns, list):
            columns = tuple(columns)

        self.columns = columns
        self.categories = categories
        self.index = index

        self.dataset = self._load_paths_and_metadata()
        self.task_name = 'read-parquet-' + tokenize(self.dataset, self.columns,
                                                    self.categories)

    def _ensure_registered(self):
        if self._REGISTERED_TOKENIZE:
            return
        import fastparquet

        @partial(normalize_token.register, fastparquet.ParquetFile)
        def normalize_ParquetFile(pf):
            return (type(pf), pf.fn, pf.sep) + normalize_token(pf.open)

        self._REGISTERED_TOKENIZE = True

    def get_pandas_deferred(self):
        row_groups = self._get_filtered_row_groups()

        (out_type, frame_meta, index_col, all_columns,
         categories, divisions) = self._resolve_metadata(row_groups)

        task_plan = {
            (self.task_name, i): (_read_parquet_row_group,
                                  self.file_opener,
                                  self.dataset.row_group_filename(rg),
                                  index_col,
                                  all_columns,
                                  rg,
                                  out_type == Series,
                                  categories,
                                  self.dataset.schema,
                                  self.dataset.cats,
                                  self.dataset.dtypes)
            for i, rg in enumerate(row_groups)
        }

        return out_type(task_plan, self.task_name, frame_meta, divisions)

    def _get_filtered_row_groups(self):
        import fastparquet.api as fpapi

        def _exclude_row_group(rg):
            return (fpapi.filter_out_stats(rg, self.filters,
                                           self.dataset.schema) or
                    fpapi.filter_out_cats(rg, self.filters))

        return [rg for rg in self.dataset.row_groups
                if not _exclude_row_group(rg)]

    def _load_paths_and_metadata(self):
        from fastparquet import ParquetFile
        from fastparquet.util import check_column_names

        path_sep = self.fs.sep

        if len(self.paths) > 1:
            pf = ParquetFile(self.paths, open_with=self.file_opener,
                             sep=path_sep)
        else:
            try:
                metadata_path = self.paths[0] + path_sep + '_metadata'
                pf = ParquetFile(metadata_path, open_with=self.file_opener,
                                 sep=path_sep)
            except:
                pf = ParquetFile(self.paths[0], open_with=self.file_opener,
                                 sep=path_sep)

        check_column_names(pf.columns, self.categories)
        return pf

    def _resolve_metadata(self, row_groups):
        pf = self.dataset

        if self.columns is None:
            all_columns = tuple(pf.columns + list(pf.cats))
        else:
            all_columns = self.columns

        if not isinstance(all_columns, tuple):
            out_type = Series
            all_columns = (all_columns,)
        else:
            out_type = DataFrame

        index_col, divisions = self._infer_index_and_divisions(row_groups)

        if index_col and index_col not in all_columns:
            all_columns = all_columns + (index_col,)

        categories = self.categories
        if categories is None:
            categories = pf.categories

        dtypes = pf._dtypes(categories)

        meta = self._meta_from_dtypes(all_columns, pf.columns, dtypes)

        for cat in categories:
            meta[cat] = pd.Series(pd.Categorical([],
                                  categories=[UNKNOWN_CATEGORIES]))

        if index_col:
            meta = meta.set_index(index_col)

        if out_type == Series:
            assert len(meta.columns) == 1
            meta = meta[meta.columns[0]]

        return out_type, meta, index_col, all_columns, categories, divisions

    def _infer_index_and_divisions(self, row_groups):
        import fastparquet.api as fpapi

        # Find an index among the partially sorted columns
        minmax = fpapi.sorted_partitioned_columns(self.dataset)

        if self.index is False:
            index_col = None
        elif len(minmax) == 1:
            index_col = first(minmax)
        elif len(minmax) > 1:
            if self.index:
                index_col = self.index
            elif 'index' in minmax:
                index_col = 'index'
            else:
                raise ValueError("Multiple possible indexes exist: %s.  "
                                 "Please select one with index='index-name'"
                                 % sorted(minmax))
        else:
            index_col = None

        if index_col:
            divisions = (list(minmax[index_col]['min']) +
                         [minmax[index_col]['max'][-1]])
        else:
            divisions = (None,) * (len(row_groups) + 1)

        if isinstance(divisions[0], np.datetime64):
            divisions = [pd.Timestamp(d) for d in divisions]

        return index_col, divisions


def _read_parquet_row_group(open, fn, index, columns, rg, series, categories,
                            schema, cs, dt, *args):
    from fastparquet.core import read_row_group_file
    from fastparquet.api import _pre_allocate

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
# pyarrow interfaceGenericParquetReader


class ArrowReader(GenericParquetReader):

    _REGISTERED_TOKENIZE = False

    def __init__(self, fs, paths, file_opener, columns=None, filters=None,
                 categories=None, index=None):
        import pyarrow.parquet as pq
        self.api = pq

        self.fs = fs
        self.paths = paths
        self.file_opener = file_opener

        if filters is not None:
            raise NotImplemented("Predicate pushdown not implemented")

        if categories is not None:
            raise NotImplemented("Categorical reads not yet implemented")

        if isinstance(columns, tuple):
            columns = list(columns)

        self.columns = columns

        self.dataset = self.api.ParquetDataset(self.paths)
        self.schema = self.dataset.schema.to_arrow_schema()
        self.task_name = 'read-parquet-' + tokenize(self.dataset, self.columns)

    def _ensure_registered(self):
        if self._REGISTERED_TOKENIZE:
            return

        @partial(normalize_token.register, self.api.ParquetDataset)
        def normalize_PyArrowParquetDataset(ds):
            return (type(ds), ds.paths)

        self._REGISTERED_TOKENIZE = True

    def get_pandas_deferred(self):
        pieces = self.dataset.pieces

        (out_type, frame_meta,
         all_columns, divisions) = self._resolve_metadata()

        task_plan = {
            (self.task_name, i): (_read_arrow_parquet_piece,
                                  self.file_opener,
                                  piece,
                                  all_columns,
                                  out_type == Series,
                                  self.dataset.partitions)
            for i, piece in enumerate(pieces)
        }

        return out_type(task_plan, self.task_name, frame_meta, divisions)

    def _resolve_metadata(self):
        if self.columns is None:
            all_columns = self.schema.names
        else:
            all_columns = self.columns

        if not isinstance(all_columns, list):
            out_type = Series
            all_columns = [all_columns]
        else:
            out_type = DataFrame

        divisions = (None,) * (len(self.dataset.pieces) + 1)

        dtypes = self._get_schema_expected_dtypes()
        meta = self._meta_from_dtypes(all_columns, self.schema.names, dtypes)
        return out_type, meta, all_columns, divisions

    def _get_schema_expected_dtypes(self):
        dtypes = {}
        for i in range(len(self.schema)):
            field = self.schema[i]
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
                 storage_options=None, engine='fastparquet'):
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
    engine : {'fastparquet', 'arrow'}, default 'fastparquet'
        Parquet reader library to use

    Examples
    --------
    >>> df = read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """
    fs, paths, file_opener = get_fs_paths_myopen(path, None, 'rb',
                                                 **(storage_options or {}))

    if engine == 'fastparquet':
        klass = FastparquetReader
    elif engine == 'arrow':
        klass = ArrowReader
    else:
        raise ValueError('Unsupported engine: {0}'.format(engine))

    reader = klass(fs, paths, file_opener, columns=columns,
                   filters=filters,
                   categories=categories, index=index)

    return reader.get_pandas_deferred()


def to_parquet(path, df, compression=None, write_index=None, has_nulls=None,
               fixed_text=None, object_encoding=None, storage_options=None,
               append=False, ignore_divisions=False):
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
        If False, construct data-set from scratch; if True, add new
        row-group(s) to existing data-set. In the latter case, the data-set
        must exist, and the schema must match the input data.
    ignore_divisions: bool (False)
        If False raises error when previous divisions overlap with the new
        appended divisions. Ignored if append=False.

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

    object_encoding = object_encoding or 'utf8'
    if object_encoding == 'infer' or (isinstance(object_encoding, dict) and
                                      'infer' in object_encoding.values()):
        raise ValueError('"infer" not allowed as object encoding, '
                         'because this required data in memory.')
    fmd = fastparquet.writer.make_metadata(df._meta, has_nulls=has_nulls,
                                           fixed_text=fixed_text,
                                           object_encoding=object_encoding)

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

    writes = [delayed(fastparquet.writer.make_part_file)(
              myopen(outfile, 'wb'), partition, fmd.schema,
              compression=compression)
              for outfile, partition in zip(outfiles, partitions)]

    out = delayed(writes).compute()

    for fn, rg in zip(filenames, out):
        if rg is not None:
            for chunk in rg.columns:
                chunk.file_path = fn
            fmd.row_groups.append(rg)

    if len(fmd.row_groups) == 0:
        raise ValueError("All partitions were empty")
    fastparquet.writer.write_common_metadata(metadata_fn, fmd, open_with=myopen,
                                             no_row_groups=False)

    fn = sep.join([path, '_common_metadata'])
    fastparquet.writer.write_common_metadata(fn, fmd, open_with=myopen)


if PY3:
    DataFrame.to_parquet.__doc__ = to_parquet.__doc__
