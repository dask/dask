"""Arrow Dataset reader"""

from collections.abc import Mapping

from ...base import tokenize
from ..core import DataFrame, new_dd_object


class ArrowDatasetSubgraph(Mapping):
    """
    Subgraph for reading Arrow Datasets.

    Enables optimiziations (see optimize_read_arrow_dataset).
    """

    def __init__(self, name, parts, fs, schema, format, columns, filter, kwargs, meta):
        self.name = name
        self.parts = parts
        self.fs = fs
        self.schema = schema
        self.format = format
        self.columns = columns
        self.filter = filter
        self.kwargs = kwargs
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
        kwargs = self.kwargs[i]

        return (
            read_arrow_dataset_part,
            part,
            self.fs,
            self.schema,
            self.format,
            self.columns,
            self.filter,
            kwargs,
        )

    def __len__(self):
        return len(self.parts)

    def __iter__(self):
        for i in range(len(self)):
            yield (self.name, i)


def read_arrow_dataset_part(fragment, fs, schema, format, columns, filter, kwargs):
    import pyarrow
    from pyarrow.fs import LocalFileSystem

    if fs is None:
        fs = LocalFileSystem()

    if pyarrow.__version__ == "0.17.0":
        # work-around need to pass columns selection to the fragment
        # for released pyarrow 0.17
        new_fragment = format.make_fragment(
            fragment.path,
            fs,
            schema=schema,
            columns=columns,
            filter=filter,
            partition_expression=fragment.partition_expression,
            **kwargs,
        )
        return new_fragment.to_table(use_threads=False).to_pandas()
    else:
        # works with pyarrow master
        new_fragment = format.make_fragment(
            fragment.path,
            fs,
            partition_expression=fragment.partition_expression,
            **kwargs,
        )
        return new_fragment.to_table(
            use_threads=False, schema=schema, columns=columns, filter=filter
        ).to_pandas()

def read_arrow_dataset(
    path,
    partitioning=None,
    columns=None,
    filter=None,
    filesystem=None,
    format="parquet",
    split_row_groups=False,
):
    """
    Read a dataset using Arrow Datasets.

    Support the formats supported by pyarrow, which right now is Parquet
    and Feather.
    """
    import pyarrow.dataset as ds
    from pyarrow.fs import LocalFileSystem

    if format == "ipc" and filesystem is None:
        filesystem = LocalFileSystem(use_mmap=True)

    # dataset discovery
    dataset = ds.dataset(
        path, partitioning=partitioning, filesystem=filesystem, format=format
    )

    # get all fragments and properties
    schema = dataset.schema
    format = dataset.format

    fragments = list(dataset.get_fragments(filter=filter))
    kwargs = [{}] * len(fragments)
    if isinstance(format, ds.ParquetFileFormat) and split_row_groups:
        fragments = [f_rg for f in fragments for f_rg in f.get_row_group_fragments()]
        kwargs = [dict(row_groups=f.row_groups) for f in fragments]

    if len(fragments):
        filesystem = fragments[0].filesystem

    # create dask object
    meta = schema.empty_table().to_pandas()
    if columns is not None:
        meta = meta[columns]
    name = "read-arrow-dataset-" + tokenize(path, partitioning, columns, filter)

    subgraph = ArrowDatasetSubgraph(
        name, fragments, filesystem, schema, format, columns, filter, kwargs, meta,
    )

    return new_dd_object(subgraph, name, meta, tuple([None] * (len(fragments) + 1)))
