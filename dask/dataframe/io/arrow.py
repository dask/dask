"""Arrow Dataset reader"""

from collections.abc import Mapping

import pyarrow.dataset as ds

from ...base import tokenize
from ..core import DataFrame, new_dd_object


class ArrowDatasetSubgraph(Mapping):
    """
    Subgraph for reading Arrow Datasets.

    Enables optimiziations (see optimize_read_arrow_dataset).
    """

    def __init__(self, name, parts, fs, schema, partitioning, format, columns, filter, meta):
        self.name = name
        self.parts = parts
        self.fs = fs
        self.schema = schema
        self.partitioning = partitioning
        self.format = format
        self.columns = columns
        self.filter = filter
        self.meta = meta

    def __repr__(self):
        return "ArrowDatasetSubgraph<name='{}', n_parts={}>".format(
            self.name, len(self.parts)
        )

    def __getitem__(self, key):
        try:
            name, i = key
        except ValueError:
            # too many / few values to unpack
            raise KeyError(key) from None

        if name != self.name:
            raise KeyError(key)

        if i < 0 or i >= len(self.parts):
            raise KeyError(key)

        part = self.parts[i]

        return (
            read_arrow_dataset_part, 
            part,
            self.fs,
            self.schema, 
            self.partitioning, 
            self.format, 
            self.columns, 
            self.filter,
        )

    def __len__(self):
        return len(self.parts)

    def __iter__(self):
        for i in range(len(self)):
            yield (self.name, i)


def get_paths_and_expressions(path, partitioning):
    """
    Split path and infer expressions. Assumes that path is a single file path.

    Returns list of paths and expressions, as expected by FilesystemSource.
    """
    paths = []
    exprs = []

    for i in range(len(path.split("/")) - 1, 0, -1):

        path_dir = path.rsplit("/", maxsplit=i)[0]
        path_dir_last = path_dir.split("/")[-1]
        expr_dir = partitioning.parse(path_dir_last)

        paths.append(path_dir)
        exprs.append(expr_dir)

    paths.append(path)
    exprs.append(ds.scalar(True))
    return paths, exprs


def read_arrow_dataset_part(
        path, fs, schema, partitioning, format, columns, filter
):
    if fs is None:
        from pyarrow.fs import LocalFileSystem
        fs = LocalFileSystem()
    paths, exprs = get_paths_and_expressions(path, partitioning)
    source = ds.FileSystemSource(schema, None, format, fs, paths, exprs)
    dataset = ds.Dataset([source], schema)
    table = dataset.to_table(columns=columns, filter=filter, use_threads=False)
    return table.to_pandas()


def read_arrow_dataset(path, partitioning, columns=None, filter=None):

    dataset = ds.dataset(path, partitioning=partitioning)
    source = dataset.sources[0]
    schema = dataset.schema
    files = source.files
    format = ds.ParquetFileFormat()

    # meta = next(dataset.to_batches()).to_pandas()
    meta = schema.empty_table().to_pandas()
    if columns is not None:
        meta = meta[columns]
    name = "read-arrow-dataset-" + tokenize(path, partitioning, columns, filter)

    # tasks = [(read_arrow_dataset_part, f, None, schema, partitioning, format, columns, filter) for f in files]
    # dsk = {(name, i): tasks[i] for i in range(len(tasks))}
    # return DataFrame(dsk, name, meta, tuple([None]*(len(dsk) + 1)))

    subgraph = ArrowDatasetSubgraph(
        name, files, None, schema, partitioning, format, columns, filter, meta,
    )

    return new_dd_object(subgraph, name, meta, tuple([None]*(len(files) + 1)))
