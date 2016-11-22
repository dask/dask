import struct

import pandas as pd
from toolz import first, partial

from ..core import DataFrame, Series
from ..utils import make_meta
from ...base import compute, tokenize, normalize_token
from ...delayed import delayed
from ...bytes.core import OpenFileCreator

try:
    import fastparquet
    from fastparquet import parquet_thrift
    from fastparquet.core import read_row_group_file
    default_encoding = parquet_thrift.Encoding.PLAIN
except:
    fastparquet = False
    default_encoding = None


def read_parquet(path, columns=None, filters=None, categories=None, index=None):
    """
    Read Dask DataFrame from ParquetFile

    This reads a directory of Parquet data into a Dask.dataframe, one file per
    partition.  It selects the index among the sorted columns if any exist.

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
    myopen = OpenFileCreator(path, compression=None, text=False)

    if isinstance(columns, list):
        columns = tuple(columns)

    try:
        pf = fastparquet.ParquetFile(path + myopen.fs.sep + '_metadata',
                                     open_with=myopen,
                                     sep=myopen.fs.sep)
    except:
        pf = fastparquet.ParquetFile(path, open_with=myopen, sep=myopen.fs.sep)

    name = 'read-parquet-' + tokenize(pf, columns, categories)

    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.helper)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    # get category values from first row-group
    categories = categories or []
    cats = pf.grab_cats(categories)
    categories = [cat for cat in categories if cats.get(cat, None) is not None]

    # Find an index among the partially sorted columns
    minmax = fastparquet.api.sorted_partitioned_columns(pf)

    if index is False:
        index_col = None
    elif len(minmax) > 1:
        if index:
            index_col = index
        else:
            raise ValueError("Multiple possible indexes exist: %s.  "
                             "Please select one with index='index-name'"
                             % sorted(minmax))
    elif len(minmax) == 1:
        index_col = first(minmax)
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

    # TODO: if categories vary from one rg to next, need to cope
    dtypes = {k: ('category' if k in (categories or []) else v) for k, v in
              pf.dtypes.items() if k in all_columns}

    meta = make_meta(dtypes)
    for cat in categories:
        meta[cat] = pd.Series(pd.Categorical([], categories=cats[cat]))

    if index_col:
        meta = meta.set_index(index_col)

    if out_type == Series:
        assert len(meta.columns) == 1
        meta = meta[meta.columns[0]]

    dsk = {(name, i): (read_parquet_row_group, myopen, pf.row_group_filename(rg),
                       index_col, all_columns, rg, out_type == Series,
                       categories, pf.helper, pf.cats)
           for i, rg in enumerate(rgs)}

    if index_col:
        divisions = list(minmax[index_col]['min']) + [minmax[index_col]['max'][-1]]
    else:
        divisions = (None,) * (len(rgs) + 1)

    return out_type(dsk, name, meta, divisions)


def read_parquet_row_group(open, fn, index, columns, rg, series, *args):
    if not isinstance(columns, (tuple, list)):
        columns = (columns,)
        series = True
    if index and index not in columns:
        columns = columns + type(columns)([index])
    df = read_row_group_file(fn, rg, columns, *args, open=open)
    if index:
        df = df.set_index(index)

    if series:
        return df[df.columns[0]]
    else:
        return df


def to_parquet(path, df, encoding=default_encoding, compression=None,
               write_index=None):
    """
    Write Dask.dataframe to parquet

    Notes
    -----
    Each partition will be written to a separate file.

    Parameters
    ----------
    path : string
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    df : Dask.dataframe
    encoding : parquet_thrift.Encoding
    compression : string or dict
        Either a string like "SNAPPY" or a dictionary mapping column names to
        compressors like ``{"name": "GZIP", "values": "SNAPPY"}``
    write_index : boolean
        Whether or not to write the index.  Defaults to True *if* divisions are
        known.

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

    myopen = OpenFileCreator(path, compression=None, text=False)
    myopen.fs.mkdirs(path)
    sep = myopen.fs.sep
    metadata_fn = sep.join([path, '_metadata'])

    if write_index is True or write_index is None and df.known_divisions:
        df = df.reset_index()

    fmd = fastparquet.writer.make_metadata(df._meta_nonempty)

    partitions = df.to_delayed()
    filenames = ['part.%i.parquet' % i for i in range(len(partitions))]
    outfiles = [sep.join([path, fn]) for fn in filenames]

    writes = [delayed(fastparquet.writer.make_part_file)(
              myopen(outfile, 'wb'), partition, fmd.schema,
              compression=compression)
              for outfile, partition in zip(outfiles, partitions)]

    out = compute(*writes)

    for fn, rg in zip(filenames, out):
        for chunk in rg.columns:
            chunk.file_path = fn
        fmd.row_groups.append(rg)

    with myopen(metadata_fn, mode='wb') as f:
        f.write(b'PAR1')
        foot_size = fastparquet.writer.write_thrift(f, fmd)
        f.write(struct.pack(b"<i", foot_size))
        f.write(b'PAR1')
        f.close()

    with myopen(sep.join([path, '_common_metadata']), mode='wb') as f:
        fastparquet.writer.write_common_metadata(f, fmd)


if fastparquet:
    @partial(normalize_token.register, fastparquet.ParquetFile)
    def normalize_ParquetFile(pf):
        return (type(pf), pf.fn, pf.sep) + normalize_token(pf.open)
