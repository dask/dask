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

    def __init__(self, name, parts, fs, schema, format, columns, filter, meta):
        self.name = name
        self.parts = parts
        self.fs = fs
        self.schema = schema
        # self.partitioning = partitioning
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
            # self.partitioning,
            self.format,
            self.columns,
            self.filter,
        )

    def __len__(self):
        return len(self.parts)

    def __iter__(self):
        for i in range(len(self)):
            yield (self.name, i)


def destructure(fragment):
    """
    Convert file path and partition expression in list of corresponding
    paths / expressions as expected by the FileSystemDataset constructor.
    """
    expr = fragment.partition_expression
    path = fragment.path

    segments = path.split('/')
    basename = segments.pop()

    segment_to_expr = {}
    segment_to_expr[basename] = ds.ScalarExpression(True)

    while type(expr) is ds.AndExpression:
        segment_to_expr[segments.pop()] = expr.right_operand
        expr = expr.left_operand

    segment_to_expr[segments.pop()] = expr

    base_dir = '/'.join(segments)

    paths = []
    exprs = []

    path = base_dir

    for segment in list(segment_to_expr.keys())[::-1]:
        path = "/".join([path, segment])
        paths.append(path)
        exprs.append(segment_to_expr[segment])

    #return base_dir, segment_to_expr, basename
    return paths, exprs


def read_arrow_dataset_part(
        fragment, fs, schema, format, columns, filter
):
    if fs is None:
        from pyarrow.fs import LocalFileSystem
        fs = LocalFileSystem()

    paths, exprs = destructure(fragment)

    dataset = ds.FileSystemDataset(schema, None, format, fs, paths, exprs)
    table = dataset.to_table(columns=columns, filter=filter, use_threads=False)
    return table.to_pandas()


def read_arrow_dataset(path, partitioning=None, columns=None, filter=None,
                       filesystem=None, format="parquet"):

    if format == "ipc" and filesystem is None:
        from pyarrow.fs import LocalFileSystem
        filesystem = LocalFileSystem(use_mmap=True)

    dataset = ds.dataset(
        path, partitioning=partitioning, filesystem=filesystem, format=format
    )
    schema = dataset.schema
    format = dataset.format

    if filesystem is None:
        from pyarrow.fs import LocalFileSystem
        filesystem = LocalFileSystem()

    # files = source.files
    fragments = list(dataset.get_fragments(filter=filter))

    # meta = next(dataset.to_batches()).to_pandas()
    meta = schema.empty_table().to_pandas()
    if columns is not None:
        meta = meta[columns]
    name = "read-arrow-dataset-" + tokenize(path, partitioning, columns, filter)

    subgraph = ArrowDatasetSubgraph(
        name, fragments, filesystem, schema, format, columns, filter, meta,
    )

    return new_dd_object(subgraph, name, meta, tuple([None]*(len(fragments) + 1)))
