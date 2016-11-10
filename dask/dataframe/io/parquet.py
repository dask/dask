import os
import struct

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


def parquet_to_dask_dataframe(url, columns=None, filters=[],
                              categories=None, **kwargs):
    if fastparquet is False:
        raise ImportError("fastparquet not installed")
    myopen = OpenFileCreator(url, compression=None, text=False)

    try:
        pf = fastparquet.ParquetFile(url + '/_metadata',
                                     open_with=myopen,
                                     sep=myopen.fs.sep)
        root = url
    except:
        pf = fastparquet.ParquetFile(url, open_with=myopen, sep=myopen.fs.sep)
        root = os.path.dirname(url)  # TODO: this might fail on S3 + Windows

    columns = columns or (pf.columns + list(pf.cats))
    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.helper)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    infiles = ['/'.join([root, rg.columns[0].file_path])
               for rg in pf.row_groups]
    tot = [delayed(pf.read_row_group_file)(rg, columns, categories,
                                           **kwargs)
           for rg, infile in zip(rgs, infiles)]
    if len(tot) == 0:
        raise ValueError("All partitions failed filtering")
    dtypes = {k: ('category' if k in (categories or []) else v) for k, v in
              pf.dtypes.items() if k in columns}

    # TODO: if categories vary from one rg to next, need to cope
    return dd.from_delayed(tot, meta=dtypes)


def dask_dataframe_to_parquet(
        url, df, encoding=default_encoding, compression=None):
    """Same signature as write, but with file_scheme always hive-like, each
    data partition becomes a row group in a separate file.
    """
    if fastparquet is False:
        raise ImportError("fastparquet not installed")

    sep = '/'
    # mkdirs(filename)
    metadata_fn = sep.join([url, '_metadata'])

    myopen = OpenFileCreator(url, compression=None, text=False)
    myopen.fs.mkdirs(url)

    fmd = fastparquet.writer.make_metadata(df.head(10))

    partitions = df.to_delayed()
    filenames = ['part.%i.parquet' % i for i in range(len(partitions))]
    outfiles = [sep.join([url, fn]) for fn in filenames]

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

    with myopen(sep.join([url, '_common_metadata']), mode='wb') as f:
        fastparquet.writer.write_common_metadata(f, fmd)
