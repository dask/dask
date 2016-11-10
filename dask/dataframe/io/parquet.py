import os
import struct

from toolz import first

import dask
from dask import delayed, compute
from dask.bytes.core import open_files, OpenFileCreator
import dask.dataframe as dd
from dask import delayed

try:
    import fastparquet
    from fastparquet import parquet_thrift
    default_encoding = parquet_thrift.Encoding.PLAIN
except:
    fastparquet = False
    default_encoding = None


def read_parquet(path, columns=None, filters=[], categories=None, index=None,
        **kwargs):
    """ Read Dask DataFrame from ParquetFile

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

    Examples
    --------
    >>> df = read_parquet('s3://bucket/my-parquet-data')  # doctest: +SKIP

    See Also
    --------
    to_parquet
    """
    if fastparquet is False:
        raise ImportError("fastparquet not installed")
    myopen = OpenFileCreator(path, compression=None, text=False)

    try:
        pf = fastparquet.ParquetFile(path + '/_metadata',
                                     open_with=myopen,
                                     sep=myopen.fs.sep)
        root = path
    except:
        pf = fastparquet.ParquetFile(path, open_with=myopen, sep=myopen.fs.sep)
        root = os.path.dirname(path)  # TODO: this might fail on S3 + Windows

    columns = columns or (pf.columns + list(pf.cats))
    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.helper)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    parts = [delayed(pf.read_row_group_file)(rg, columns, categories, **kwargs)
             for rg in rgs]

    # TODO: if categories vary from one rg to next, need to cope
    dtypes = {k: ('category' if k in (categories or []) else v) for k, v in
              pf.dtypes.items() if k in columns}

    df = dd.from_delayed(parts, meta=dtypes)

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

    if index_col:
        divisions = list(minmax[index_col]['min']) + [minmax[index_col]['max'][-1]]
        df = df.set_index(index_col, sorted=True, divisions=divisions)

    return df


def to_parquet(path, df, encoding=default_encoding, compression=None,
        write_index=None):
    """
    Write Dask.dataframe to parquet

    Notes
    -----
    Each partition will be written to a separte file.

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
    >>> df = dd.read_csv(...)
    >>> to_parquet('/path/to/output/', df, compression='SNAPPY')  # doctest: +SKIP

    See Also
    --------
    read_parquet: Read parquet data to dask.dataframe
    """
    if fastparquet is False:
        raise ImportError("fastparquet not installed")

    sep = '/'
    # mkdirs(filename)
    metadata_fn = sep.join([path, '_metadata'])

    myopen = OpenFileCreator(path, compression=None, text=False)
    myopen.fs.mkdirs(path)

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
