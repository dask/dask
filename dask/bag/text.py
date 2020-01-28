import io
import os

from toolz import concat

from ..utils import system_encoding, parse_bytes
from ..delayed import delayed
from ..bytes import open_files, read_bytes
from .core import from_delayed

delayed = delayed(pure=True)


def read_text(
    urlpath,
    blocksize=None,
    compression="infer",
    encoding=system_encoding,
    errors="strict",
    linedelimiter=os.linesep,
    collection=True,
    storage_options=None,
    files_per_partition=None,
    include_path=False,
):
    """ Read lines from text files

    Parameters
    ----------
    urlpath : string or list
        Absolute or relative filepath(s). Prefix with a protocol like ``s3://``
        to read from alternative filesystems. To read from multiple files you
        can pass a globstring or a list of paths, with the caveat that they
        must all have the same protocol.
    blocksize: None, int, or str
        Size (in bytes) to cut up larger files.  Streams by default.
        Can be ``None`` for streaming, an integer number of bytes, or a string
        like "128MiB"
    compression: string
        Compression format like 'gzip' or 'xz'.  Defaults to 'infer'
    encoding: string
    errors: string
    linedelimiter: string
    collection: bool, optional
        Return dask.bag if True, or list of delayed values if false
    storage_options: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.
    files_per_partition: None or int
        If set, group input files into partitions of the requested size,
        instead of one partition per file. Mutually exclusive with blocksize.
    include_path: bool
        Whether or not to include the path along with the bag.
        Default is False.

    Examples
    --------
    >>> b = read_text('myfiles.1.txt')  # doctest: +SKIP
    >>> b = read_text('myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('myfiles.*.txt.gz')  # doctest: +SKIP
    >>> b = read_text('s3://bucket/myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('s3://key:secret@bucket/myfiles.*.txt')  # doctest: +SKIP
    >>> b = read_text('hdfs://namenode.example.com/myfiles.*.txt')  # doctest: +SKIP

    Parallelize a large file by providing the number of uncompressed bytes to
    load into each partition.

    >>> b = read_text('largefile.txt', blocksize='10MB')  # doctest: +SKIP

    Get file paths of the bag by setting include_path=True

    >>> b, paths = read_text('myfiles.*.txt', include_path=True) # doctest: +SKIP
    >>> paths # doctest: +SKIP
    ['myfiles.0.txt', 'myfiles.1.txt']

    Returns
    -------
    content : dask.bag.Bag or list
        dask.bag.Bag if collection is True or list of Delayed lists otherwise
    paths : list of strings, only included if include_path is True
        List of the of the same length as the partition count of content if
        collection is True (i.e. content is a dask.bag.Bag), or of same length
        as content if collection is False (i.e. content is a list of Delayed
        lists).
        Each item is a string representing the file path of the corresponding
        bag partition / Delayed list.

    See Also
    --------
    from_sequence: Build bag from Python sequence
    """
    if blocksize is not None and files_per_partition is not None:
        raise ValueError("Only one of blocksize or files_per_partition can be set")
    if isinstance(blocksize, str):
        blocksize = parse_bytes(blocksize)

    paths = None

    files = open_files(
        urlpath,
        mode="rt",
        encoding=encoding,
        errors=errors,
        compression=compression,
        **(storage_options or {})
    )
    if blocksize is None:
        if files_per_partition is None:
            if include_path:
                paths = [fil.path for fil in files]
            blocks = [delayed(list)(delayed(file_to_blocks)(fil)) for fil in files]
        else:
            if include_path:
                raise ValueError(
                    "Cannot set include_path when files_per_partition is set."
                )
            blocks = []
            for start in range(0, len(files), files_per_partition):
                block_files = files[start : (start + files_per_partition)]
                block_lines = delayed(concat)(delayed(map)(file_to_blocks, block_files))
                blocks.append(block_lines)
    else:
        o = read_bytes(
            urlpath,
            delimiter=linedelimiter.encode(),
            blocksize=blocksize,
            sample=False,
            compression=compression,
            include_path=include_path,
            **(storage_options or {})
        )
        blocks = o[1]
        if include_path:
            paths = list(
                concat([[path] * len(blocks[i]) for i, path in enumerate(o[2])])
            )
        blocks = [delayed(decode)(b, encoding, errors) for b in concat(blocks)]
        print(paths, blocks)

    if not blocks:
        raise ValueError("No files found", urlpath)

    if collection:
        blocks = from_delayed(blocks)

    if include_path:
        return blocks, paths

    return blocks


def file_to_blocks(lazy_file):
    with lazy_file as f:
        for line in f:
            yield line


def decode(block, encoding, errors):
    text = block.decode(encoding, errors)
    lines = io.StringIO(text)
    return list(lines)
