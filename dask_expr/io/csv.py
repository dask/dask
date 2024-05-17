import functools
import operator

from dask_expr._expr import Projection
from dask_expr._util import _convert_to_list
from dask_expr.io.io import BlockwiseIO, PartitionsFiltered


class ReadCSV(PartitionsFiltered, BlockwiseIO):
    _parameters = [
        "filename",
        "columns",
        "header",
        "dtype_backend",
        "_partitions",
        "storage_options",
        "kwargs",
        "_series",
        "dataframe_backend",
    ]
    _defaults = {
        "columns": None,
        "header": "infer",
        "dtype_backend": None,
        "kwargs": None,
        "_partitions": None,
        "storage_options": None,
        "_series": False,
        "dataframe_backend": "pandas",
    }
    _absorb_projections = True

    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_csv

        return read_csv

    @functools.cached_property
    def _ddf(self):
        from dask import config

        # Temporary hack to simplify logic
        with config.set({"dataframe.backend": self.dataframe_backend}):
            kwargs = (
                {"dtype_backend": self.dtype_backend}
                if self.dtype_backend is not None
                else {}
            )
            if self.kwargs is not None:
                kwargs.update(self.kwargs)

            columns = _convert_to_list(self.operand("columns"))
            if columns is None:
                pass
            elif "include_path_column" in self.kwargs:
                flag = self.kwargs["include_path_column"]
                if flag is True:
                    column_to_remove = "path"
                elif isinstance(flag, str):
                    column_to_remove = flag
                else:
                    column_to_remove = None

                columns = [c for c in columns if c != column_to_remove]

                if not columns:
                    meta = self.operation(
                        self.filename,
                        header=self.header,
                        storage_options=self.storage_options,
                        **kwargs,
                    )._meta
                    columns = [list(meta.columns)[0]]

            usecols = kwargs.pop("usecols", None)
            if usecols is not None and columns is not None:
                columns = [col for col in columns if col in usecols]
            elif usecols:
                columns = usecols

            return self.operation(
                self.filename,
                usecols=columns,
                header=self.header,
                storage_options=self.storage_options,
                **kwargs,
            )

    @functools.cached_property
    def _meta(self):
        return self._ddf._meta

    def _simplify_up(self, parent, dependents):
        if isinstance(parent, Projection):
            kwargs = self.kwargs
            # int usecols are positional, so block projections
            if kwargs.get("usecols", None) is not None and isinstance(
                kwargs.get("usecols")[0], int
            ):
                return
        return super()._simplify_up(parent, dependents)

    @functools.cached_property
    def columns(self):
        columns_operand = self.operand("columns")
        if columns_operand is None:
            try:
                return list(self._ddf._meta.columns)
            except AttributeError:
                return []
        else:
            return _convert_to_list(columns_operand)

    def _divisions(self):
        return self._ddf.divisions

    @functools.cached_property
    def _tasks(self):
        return list(self._ddf.dask.to_dict().values())

    def _filtered_task(self, index: int):
        if self._series:
            return (operator.getitem, self._tasks[index], self.columns[0])
        return self._tasks[index]


class ReadTable(ReadCSV):
    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_table

        return read_table


class ReadFwf(ReadCSV):
    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_fwf

        return read_fwf


def to_csv(
    df,
    filename,
    single_file=False,
    encoding="utf-8",
    mode="wt",
    name_function=None,
    compression=None,
    compute=True,
    scheduler=None,
    storage_options=None,
    header_first_partition_only=None,
    compute_kwargs=None,
    **kwargs,
):
    """
    Store Dask DataFrame to CSV files

    One filename per partition will be created. You can specify the
    filenames in a variety of ways.

    Use a globstring::

    >>> df.to_csv('/path/to/data/export-*.csv')  # doctest: +SKIP

    The * will be replaced by the increasing sequence 0, 1, 2, ...

    ::

        /path/to/data/export-0.csv
        /path/to/data/export-1.csv

    Use a globstring and a ``name_function=`` keyword argument.  The
    name_function function should expect an integer and produce a string.
    Strings produced by name_function must preserve the order of their
    respective partition indices.

    >>> from datetime import date, timedelta
    >>> def name(i):
    ...     return str(date(2015, 1, 1) + i * timedelta(days=1))

    >>> name(0)
    '2015-01-01'
    >>> name(15)
    '2015-01-16'

    >>> df.to_csv('/path/to/data/export-*.csv', name_function=name)  # doctest: +SKIP

    ::

        /path/to/data/export-2015-01-01.csv
        /path/to/data/export-2015-01-02.csv
        ...

    You can also provide an explicit list of paths::

    >>> paths = ['/path/to/data/alice.csv', '/path/to/data/bob.csv', ...]  # doctest: +SKIP
    >>> df.to_csv(paths) # doctest: +SKIP

    You can also provide a directory name:

    >>> df.to_csv('/path/to/data') # doctest: +SKIP

    The files will be numbered 0, 1, 2, (and so on) suffixed with '.part':

    ::

        /path/to/data/0.part
        /path/to/data/1.part

    Parameters
    ----------
    df : dask.DataFrame
        Data to save
    filename : string or list
        Absolute or relative filepath(s). Prefix with a protocol like ``s3://``
        to save to remote filesystems.
    single_file : bool, default False
        Whether to save everything into a single CSV file. Under the
        single file mode, each partition is appended at the end of the
        specified CSV file.
    encoding : string, default 'utf-8'
        A string representing the encoding to use in the output file.
    mode : str, default 'w'
        Python file mode. The default is 'w' (or 'wt'), for writing
        a new file or overwriting an existing file in text mode. 'a'
        (or 'at') will append to an existing file in text mode or
        create a new file if it does not already exist. See :py:func:`open`.
    name_function : callable, default None
        Function accepting an integer (partition index) and producing a
        string to replace the asterisk in the given filename globstring.
        Should preserve the lexicographic order of partitions. Not
        supported when ``single_file`` is True.
    compression : string, optional
        A string representing the compression to use in the output file,
        allowed values are 'gzip', 'bz2', 'xz',
        only used when the first argument is a filename.
    compute : bool, default True
        If True, immediately executes. If False, returns a set of delayed
        objects, which can be computed at a later time.
    storage_options : dict
        Parameters passed on to the backend filesystem class.
    header_first_partition_only : bool, default None
        If set to True, only write the header row in the first output
        file. By default, headers are written to all partitions under
        the multiple file mode (``single_file`` is False) and written
        only once under the single file mode (``single_file`` is True).
        It must be True under the single file mode.
    compute_kwargs : dict, optional
        Options to be passed in to the compute method
    kwargs : dict, optional
        Additional parameters to pass to :meth:`pandas.DataFrame.to_csv`.

    Returns
    -------
    The names of the file written if they were computed right away.
    If not, the delayed tasks associated with writing the files.

    Raises
    ------
    ValueError
        If ``header_first_partition_only`` is set to False or
        ``name_function`` is specified when ``single_file`` is True.

    See Also
    --------
    fsspec.open_files
    """
    from dask.dataframe.io.csv import to_csv as _to_csv

    return _to_csv(
        df.to_legacy_dataframe(),
        filename,
        single_file=single_file,
        encoding=encoding,
        mode=mode,
        name_function=name_function,
        compression=compression,
        compute=compute,
        scheduler=scheduler,
        storage_options=storage_options,
        header_first_partition_only=header_first_partition_only,
        compute_kwargs=compute_kwargs,
        **kwargs,
    )
