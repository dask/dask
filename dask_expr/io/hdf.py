from dask_expr import from_legacy_dataframe


def read_hdf(
    pattern,
    key,
    start=0,
    stop=None,
    columns=None,
    chunksize=1000000,
    sorted_index=False,
    lock=True,
    mode="r",
):
    from dask.dataframe.io import read_hdf as _read_hdf

    df = _read_hdf(
        pattern,
        key,
        start=start,
        stop=stop,
        columns=columns,
        chunksize=chunksize,
        sorted_index=sorted_index,
        lock=lock,
        mode=mode,
    )
    return from_legacy_dataframe(df)


def to_hdf(
    df,
    path,
    key,
    mode="a",
    append=False,
    scheduler=None,
    name_function=None,
    compute=True,
    lock=None,
    dask_kwargs=None,
    **kwargs,
):
    """Store Dask Dataframe to Hierarchical Data Format (HDF) files

    This is a parallel version of the Pandas function of the same name.  Please
    see the Pandas docstring for more detailed information about shared keyword
    arguments.

    This function differs from the Pandas version by saving the many partitions
    of a Dask DataFrame in parallel, either to many files, or to many datasets
    within the same file.  You may specify this parallelism with an asterix
    ``*`` within the filename or datapath, and an optional ``name_function``.
    The asterix will be replaced with an increasing sequence of integers
    starting from ``0`` or with the result of calling ``name_function`` on each
    of those integers.

    This function only supports the Pandas ``'table'`` format, not the more
    specialized ``'fixed'`` format.

    Parameters
    ----------
    path : string, pathlib.Path
        Path to a target filename. Supports strings, ``pathlib.Path``, or any
        object implementing the ``__fspath__`` protocol. May contain a ``*`` to
        denote many filenames.
    key : string
        Datapath within the files.  May contain a ``*`` to denote many locations
    name_function : function
        A function to convert the ``*`` in the above options to a string.
        Should take in a number from 0 to the number of partitions and return a
        string. (see examples below)
    compute : bool
        Whether or not to execute immediately.  If False then this returns a
        ``dask.Delayed`` value.
    lock : bool, Lock, optional
        Lock to use to prevent concurrency issues.  By default a
        ``threading.Lock``, ``multiprocessing.Lock`` or ``SerializableLock``
        will be used depending on your scheduler if a lock is required. See
        dask.utils.get_scheduler_lock for more information about lock
        selection.
    scheduler : string
        The scheduler to use, like "threads" or "processes"
    **other:
        See pandas.to_hdf for more information

    Examples
    --------
    Save Data to a single file

    >>> df.to_hdf('output.hdf', '/data')            # doctest: +SKIP

    Save data to multiple datapaths within the same file:

    >>> df.to_hdf('output.hdf', '/data-*')          # doctest: +SKIP

    Save data to multiple files:

    >>> df.to_hdf('output-*.hdf', '/data')          # doctest: +SKIP

    Save data to multiple files, using the multiprocessing scheduler:

    >>> df.to_hdf('output-*.hdf', '/data', scheduler='processes') # doctest: +SKIP

    Specify custom naming scheme.  This writes files as
    '2000-01-01.hdf', '2000-01-02.hdf', '2000-01-03.hdf', etc..

    >>> from datetime import date, timedelta
    >>> base = date(year=2000, month=1, day=1)
    >>> def name_function(i):
    ...     ''' Convert integer 0 to n to a string '''
    ...     return base + timedelta(days=i)

    >>> df.to_hdf('*.hdf', '/data', name_function=name_function) # doctest: +SKIP

    Returns
    -------
    filenames : list
        Returned if ``compute`` is True. List of file names that each partition
        is saved to.
    delayed : dask.Delayed
        Returned if ``compute`` is False. Delayed object to execute ``to_hdf``
        when computed.

    See Also
    --------
    read_hdf:
    to_parquet:
    """
    from dask.dataframe.io import to_hdf as _to_hdf

    return _to_hdf(
        df.to_legacy_dataframe(),
        path,
        key,
        mode=mode,
        append=append,
        scheduler=scheduler,
        name_function=name_function,
        compute=compute,
        lock=lock,
        dask_kwargs=dask_kwargs,
        **kwargs,
    )
