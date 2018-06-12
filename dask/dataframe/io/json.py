from __future__ import absolute_import

import io
import pandas as pd
from dask.bytes import open_files, read_bytes
import dask


def to_json(df, url_path, orient='records', lines=None, storage_options=None,
            compute=True, encoding='utf-8', errors='strict',
            compression=None, **kwargs):
    """Write dataframe into JSON text files

    This utilises ``pandas.DataFrame.to_json()``, and most parameters are
    passed through - see its docstring.

    Differences: orient is 'records' by default, with lines=True; this
    produces the kind of JSON output that is most common in big-data
    applications, and which can be chunked when reading (see ``read_json()``).

    Parameters
    ----------
    df: dask.DataFrame
        Data to save
    url_path: str, list of str
        Location to write to. If a string, and there are more than one
        partitions in df, should include a glob character to expand into a
        set of file names, or provide a ``name_function=`` parameter.
        Supports protocol specifications such as ``"s3://"``.
    encoding, errors:
        The text encoding to implement, e.g., "utf-8" and how to respond
        to errors in the conversion (see ``str.encode()``).
    orient, lines, kwargs
        passed to pandas; if not specified, lines=True when orient='records',
        False otherwise.
    storage_options: dict
        Passed to backend file-system implementation
    compute: bool
        If true, immediately executes. If False, returns a set of delayed
        objects, which can be computed at a later time.
    encoding, errors:
        Text conversion, ``see str.encode()``
    compression : string or None
        String like 'gzip' or 'xz'.
    """
    if lines is None:
        lines = orient == 'records'
    if orient != 'records' and lines:
        raise ValueError('Line-delimited JSON is only available with'
                         'orient="records".')
    kwargs['orient'] = orient
    kwargs['lines'] = lines and orient == 'records'
    outfiles = open_files(
        url_path, 'wt', encoding=encoding,
        errors=errors,
        name_function=kwargs.pop('name_function', None),
        num=df.npartitions,
        compression=compression,
        **(storage_options or {})
    )
    parts = [dask.delayed(write_json_partition)(d, outfile, kwargs)
             for outfile, d in zip(outfiles, df.to_delayed())]
    if compute:
        dask.compute(parts)
        return [f.path for f in outfiles]
    else:
        return parts


def write_json_partition(df, openfile, kwargs):
    with openfile as f:
        df.to_json(f, **kwargs)


def read_json(url_path, orient='records', lines=None, storage_options=None,
              blocksize=None, sample=2**20, encoding='utf-8', errors='strict',
              compression='infer', **kwargs):
    """Create a dataframe from a set of JSON files

    This utilises ``pandas.read_json()``, and most parameters are
    passed through - see its docstring.

    Differences: orient is 'records' by default, with lines=True; this
    is appropriate for line-delimited "JSON-lines" data, the kind of JSON output
    that is most common in big-data scenarios, and which can be chunked when
    reading (see ``read_json()``). All other options require blocksize=None,
    i.e., one partition per input file.


    Parameters
    ----------
    url_path: str, list of str
        Location to read from. If a string, can include a glob character to
        find a set of file names.
        Supports protocol specifications such as ``"s3://"``.
    encoding, errors:
        The text encoding to implement, e.g., "utf-8" and how to respond
        to errors in the conversion (see ``str.encode()``).
    orient, lines, kwargs
        passed to pandas; if not specified, lines=True when orient='records',
        False otherwise.
    storage_options: dict
        Passed to backend file-system implementation
    blocksize: None or int
        If None, files are not blocked, and you get one partition per input
        file. If int, which can only be used for line-delimited JSON files,
        each partition will be approximately this size in bytes, to the nearest
        newline character.
    sample: int
        Number of bytes to pre-load, to provide an empty dataframe structure
        to any blocks wihout data. Only relevant is using blocksize.
    encoding, errors:
        Text conversion, ``see bytes.decode()``
    compression : string or None
        String like 'gzip' or 'xz'.

    Returns
    -------
    dask.DataFrame

    Examples
    --------
    Load single file

    >>> dd.read_json('myfile.1.json')  # doctest: +SKIP

    Load multiple files

    >>> dd.read_json('myfile.*.json')  # doctest: +SKIP

    >>> dd.read_json(['myfile.1.json', 'myfile.2.json'])  # doctest: +SKIP

    Load large line-delimited JSON files using partitions of approx
    256MB size

    >> dd.read_json('data/file*.csv', blocksize=2**28)
    """
    import dask.dataframe as dd
    if lines is None:
        lines = orient == 'records'
    if orient != 'records' and lines:
        raise ValueError('Line-delimited JSON is only available with'
                         'orient="records".')
    if blocksize and (orient != 'records' or not lines):
        raise ValueError("JSON file chunking only allowed for JSON-lines"
                         "input (orient='records', lines=True).")
    storage_options = storage_options or {}
    if blocksize:
        first, chunks = read_bytes(url_path, b'\n', blocksize=blocksize,
                                   sample=sample, compression=compression,
                                   **storage_options)
        chunks = list(dask.core.flatten(chunks))
        first = read_json_chunk(first, encoding, errors, kwargs)
        parts = [dask.delayed(read_json_chunk)(
            chunk, encoding, errors, kwargs, meta=first[:0]
        ) for chunk in chunks]

    else:
        files = open_files(url_path, 'rt', encoding=encoding, errors=errors,
                           compression=compression, **storage_options)
        parts = [dask.delayed(read_json_file)(f, orient, lines, kwargs)
                 for f in files]
    return dd.from_delayed(parts)


def read_json_chunk(chunk, encoding, errors, kwargs, meta=None):
    s = io.StringIO(chunk.decode(encoding, errors))
    s.seek(0)
    df = pd.read_json(s, orient='records', lines=True, **kwargs)
    if meta is not None and df.empty:
        return meta
    else:
        return df


def read_json_file(f, orient, lines, kwargs):
    with f as f:
        return pd.read_json(f, orient=orient, lines=lines, **kwargs)
