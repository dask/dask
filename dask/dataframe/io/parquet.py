import struct
from dask.bytes.core import open_files

try:
    import fastparquet
    from fastparquet import parquet_thrift
except:
    fastparquet = False


def parquet_to_dask_dataframe(url, columns=None, filters=[],
                              categories=None, **kwargs):
    if fastparquet is False:
        raise ImportError("fastparquet not installed")
    import dask.dataframe as dd
    from dask import delayed
    try:
        f = open_files(url+'/_metadata')[0].compute()
        pf = fastparquet.ParquetFile(f)
        root = url
    except:
        f = open_files(url)[0].compute()
        pf = ParquetFile(f)
        root = os.path.dirname(url)

    columns = columns or (pf.columns + list(pf.cats))
    rgs = [rg for rg in pf.row_groups if
           not(fastparquet.api.filter_out_stats(rg, filters, pf.helper)) and
           not(fastparquet.api.filter_out_cats(rg, filters))]

    infiles = [open_files('/'.join([root, rg.columns[0].file_path]))[0]
                          for rg in pf.row_groups]
    tot = [delayed(pf.read_row_group)(rg, columns, categories,
                                      infile=infile, **kwargs)
           for rg, infile in zip(rgs, infiles)]
    if len(tot) == 0:
        raise ValueError("All partitions failed filtering")
    dtypes = {k: v for k, v in pf.dtypes.items() if k in columns}

    # TODO: if categories vary from one rg to next, need to cope
    return dd.from_delayed(tot, meta=dtypes)


def dask_dataframe_to_parquet(url, data,
        encoding=parquet_thrift.Encoding.PLAIN, compression=None):
    """Same signature as write, but with file_scheme always hive-like, each
    data partition becomes a row group in a separate file.
    """
    sep = '/'
    from dask import delayed, compute
    # mkdirs(filename)
    fn = sep.join([url, '_metadata'])
    fmd = fastparquet.writer.make_metadata(data.head(10))

    delayed_parts = data.to_delayed()
    parts = ['part.%i.parquet' % i for i in range(len(delayed_parts))]
    outfiles = [open_files(sep.join([url, part]), mode='wb')[0] for part in parts]
    dfunc = delayed(fastparquet.writer.make_part_file)
    out = compute(*(dfunc(outfile, data, fmd.schema, compression=compression)
                    for data, outfile in zip(delayed_parts, outfiles)))

    for part, rg in zip(parts, out):
        for chunk in rg.columns:
            chunk.file_path = part
        fmd.row_groups.append(rg)

    f = open_files(fn, mode='wb')[0].compute()
    try:
        f.write(b'PAR1')
        foot_size = fastparquet.writer.write_thrift(f, fmd)
        f.write(struct.pack(b"<i", foot_size))
        f.write(b'PAR1')
    finally:
        f.close()

    f = open_files(sep.join([url, '_common_metadata']), mode='wb')[0].compute()

    fastparquet.writer.write_common_metadata(f, fmd)
