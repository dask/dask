from collections import defaultdict

from ....utils import natural_sort_key


class ORCEngine:
    """The API necessary to provide a new ORC reader/writer"""

    @classmethod
    def get_dataset_info(
        cls,
        path,
        columns=None,
        index=None,
        filters=None,
        gather_statistics=True,
        dataset_kwargs=None,
        storage_options=None,
    ):
        """Return general ORC-dataset information

        Parameters
        ----------
        path: str or list(str)
            Location of file(s), which can be a full URL with protocol
            specifier, and may include glob character if a single string.
        columns: None or list(str)
            Columns to load. If None, loads all.
        index: str
            Column name to set as index.
        filters : Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
            List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
            Filtering granularity depends on the engine, but is typically
            ath the directory or stripe level (not row-wise).
        gather_statistics : bool, default True
            Whether to allow the engine to gather file and stripe statistics.
        dataset_kwargs : dict, optional
            Dictionary of key-word arguments to pass to the engine's
            ``get_dataset_info`` method.
        storage_options: None or dict
            Further parameters to pass to the file-system backend.

        Returns
        -------
        dataset_info : dict
            A dictionary of general dataset information that the engine may
            use in ``construct_output_meta`` and ``construct_partition_plan``.
        """
        raise NotImplementedError()

    @classmethod
    def read_partition(cls, fs, part, columns, **kwargs):
        raise NotImplementedError()

    @classmethod
    def write_partition(cls, df, path, fs, filename, partition_on, **kwargs):
        raise NotImplementedError


def _is_data_file_path(path, fs, ignore_prefix=None, require_suffix=None):
    # Check that we are not ignoring this path/dir
    if ignore_prefix and path.startswith(ignore_prefix):
        return False
    # If file, check that we are allowing this suffix
    if fs.isfile(path) and require_suffix and path.endswith(require_suffix):
        return False
    return True


def collect_files(root, fs, ignore_prefix="_", require_suffix=None):

    # First, check if we are dealing with a file
    if fs.isfile(root):
        if _is_data_file_path(
            root,
            fs,
            ignore_prefix=ignore_prefix,
            require_suffix=require_suffix,
        ):
            return [root]
        return []

    # Otherwise, recursively handle each item in
    # the current `root` directory
    all_paths = []
    for sub in fs.ls(root):
        all_paths += collect_files(
            sub,
            fs,
            ignore_prefix=ignore_prefix,
            require_suffix=require_suffix,
        )

    return all_paths


def collect_partitions(file_list, root, fs, partition_sep="=", dtypes=None):

    # Always sort files by `natural_sort_key` to ensure
    # files within the same directory partition are together
    files = sorted(file_list, key=natural_sort_key)

    # Construct partitions
    parts = []
    root_len = len(root.split(fs.sep))
    dtypes = dtypes or {}
    unique_parts = defaultdict(set)
    for path in files:
        # Strip root and file name
        _path = path.split(fs.sep)[root_len:-1]
        partition = []
        for d in _path:
            _split = d.split(partition_sep)
            if len(_split) == 2:
                col = _split[0]
                partition.append(
                    (
                        _split[0],
                        dtypes[col](_split[1]) if col in dtypes else _split[1],
                    )
                )
        if partition:
            for (k, v) in partition:
                unique_parts[k].add(v)
            parts.append(partition)

    return files, parts, {k: list(v) for k, v in unique_parts.items()}
