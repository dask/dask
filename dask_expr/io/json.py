import pandas as pd
from dask.dataframe.utils import insert_meta_param_description

from dask_expr import from_legacy_dataframe
from dask_expr._backends import dataframe_creation_dispatch


@dataframe_creation_dispatch.register_inplace("pandas")
@insert_meta_param_description
def read_json(
    url_path,
    orient="records",
    lines=None,
    storage_options=None,
    blocksize=None,
    sample=2**20,
    encoding="utf-8",
    errors="strict",
    compression="infer",
    meta=None,
    engine=pd.read_json,
    include_path_column=False,
    path_converter=None,
    **kwargs,
):
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
        to any blocks without data. Only relevant when using blocksize.
    encoding, errors:
        Text conversion, ``see bytes.decode()``
    compression : string or None
        String like 'gzip' or 'xz'.
    engine : callable or str, default ``pd.read_json``
        The underlying function that dask will use to read JSON files. By
        default, this will be the pandas JSON reader (``pd.read_json``).
        If a string is specified, this value will be passed under the ``engine``
        key-word argument to ``pd.read_json`` (only supported for pandas>=2.0).
    include_path_column : bool or str, optional
        Include a column with the file path where each row in the dataframe
        originated. If ``True``, a new column is added to the dataframe called
        ``path``. If ``str``, sets new column name. Default is ``False``.
    path_converter : function or None, optional
        A function that takes one argument and returns a string. Used to convert
        paths in the ``path`` column, for instance, to strip a common prefix from
        all the paths.
    $META

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
    from dask.dataframe.io.json import read_json

    df = read_json(
        url_path,
        orient=orient,
        lines=lines,
        storage_options=storage_options,
        blocksize=blocksize,
        sample=sample,
        encoding=encoding,
        errors=errors,
        compression=compression,
        meta=meta,
        engine=engine,
        include_path_column=include_path_column,
        path_converter=path_converter,
        **kwargs,
    )
    return from_legacy_dataframe(df)


def to_json(
    df,
    url_path,
    orient="records",
    lines=None,
    storage_options=None,
    compute=True,
    encoding="utf-8",
    errors="strict",
    compression=None,
    compute_kwargs=None,
    name_function=None,
    **kwargs,
):
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
    compute_kwargs : dict, optional
        Options to be passed in to the compute method
    compression : string or None
        String like 'gzip' or 'xz'.
    name_function : callable, default None
        Function accepting an integer (partition index) and producing a
        string to replace the asterisk in the given filename globstring.
        Should preserve the lexicographic order of partitions.
    """
    from dask.dataframe.io.json import to_json

    return to_json(
        df.to_legacy_dataframe(),
        url_path,
        orient=orient,
        lines=lines,
        storage_options=storage_options,
        compute=compute,
        encoding=encoding,
        errors=errors,
        compression=compression,
        compute_kwargs=compute_kwargs,
        name_function=name_function,
        **kwargs,
    )
