import io
import pandas as pd
from dask.bytes import open_files, read_bytes
from dask import delayed
import dask.dataframe as dd
import dask
from ...core import flatten


def to_json(df, url_path, orient='records', lines=True, storage_options=None,
            compute=True, **kwargs):
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
        passed to pandas
    storage_options: dict
        Passed to backend file-system implementation
    compute: bool
        If true, immediately executes. If False, returns a set of delayed
        objects, which can be computed at a later time.
    """
    kwargs['orient'] = orient
    kwargs['lines'] = lines
    outfiles = open_files(
        url_path, 'wt', encoding=kwargs.pop('encoding', 'utf-8'),
        errors=kwargs.pop('errors', 'strict'),
        name_function=kwargs.pop('name_funciton', None),
        num=df.npartitions,
        **(storage_options or {})
    )
    parts = [delayed(_part_to_file)(d, outfile, kwargs)
             for outfile, d in zip(outfiles, df.to_delayed())]
    if compute:
        return dask.compute(parts)
    else:
        return parts


def _part_to_file(df, openfile, kwargs):
    with openfile as f:
        df.to_json(f, **kwargs)


def read_json(url_path, orient='records', lines=True, storage_options=None,
              blocksize=2**27, **kwargs):
    """Create a dataframe from a set of JSON files

    This utilises ``pandas.read_json()``, and most parameters are
    passed through - see its docstring.

    Differences: orient is 'records' by default, with lines=True; this
    is apropriate for "JSON-lines" data, the kind of JSON output that is most
    common in big-data scenarios, and which can be chunked when reading
    (see ``read_json()``). All other options require blocksize=None, i.e.,
    one partition per input file.


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
        passed to pandas
    storage_options: dict
        Passed to backend file-system implementation
    """
    if blocksize and (orient != 'records' or not lines):
        raise ValueError("JSON file chunking only allowed for JSON-lines"
                         "input (orient='records', lines=True).")
    encoding = kwargs.pop('encoding', 'utf-8')
    errors = kwargs.pop('errors', 'strict')
    storage_options = storage_options or {}
    if blocksize:
        _, chunks = read_bytes(url_path, b'\n', blocksize, sample=False,
                               **storage_options)
        parts = [delayed(_chunk_to_partition)(chunk, encoding, errors, kwargs)
                 for chunk in flatten(chunks)]
    else:
        files = open_files(url_path, 'rt', encoding=encoding, errors=errors,
                           **storage_options)
        parts = [delayed(_file_to_partition)(f, orient, lines, kwargs)
                 for f in files]
    return dd.from_delayed(parts)


def _chunk_to_partition(chunk, encoding, errors, kwargs):
    s = io.StringIO(chunk.decode(encoding, errors))
    s.seek(0)
    return pd.read_json(s, orient='records', lines=True, **kwargs)


def _file_to_partition(f, orient, lines, kwargs):
    with f as f:
        return pd.read_json(f, orient=orient, lines=lines, **kwargs)
