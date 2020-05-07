"""Arrow Dataset reader"""

from collections.abc import Mapping

from ...methods import concat
from ...utils import clear_known_categories
from ....utils import natural_sort_key

from .arrow import ArrowEngine, _get_meta_from_schema


class ArrowDatasetSubgraph(Mapping):
    """
    Subgraph for reading Arrow Datasets.

    Enables optimiziations (see optimize_read_arrow_dataset).
    """

    def __init__(
        self, name, engine, meta, parts, schema, columns, filter, index, kwargs
    ):
        self.name = name
        self.engine = engine
        self.meta = meta
        self.parts = parts
        self.schema = schema
        self.columns = columns
        self.filter = filter
        self.index = index
        self.kwargs = kwargs

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
        # TODO check kwargs
        # kwargs = self.kwargs[i]

        return (
            read_parquet_part,
            self.engine.read_partition,
            self.meta,
            part,
            self.schema,
            self.columns,
            self.filter,
            self.index,
            self.kwargs,
        )

    def __len__(self):
        return len(self.parts)

    def __iter__(self):
        for i in range(len(self)):
            yield (self.name, i)


class ArrowDatasetEngine(ArrowEngine):
    # sublass ArrowEngine to inherit the writing functionality

    @staticmethod
    def read_metadata(
        fs,
        paths,
        categories=None,
        index=None,
        gather_statistics=None,
        filters=None,
        split_row_groups=True,
        **kwargs,
    ):
        import pyarrow.dataset as ds
        from pyarrow.fs import LocalFileSystem
        from pyarrow.parquet import _filters_to_expression

        # dataset discovery
        # TODO support filesystems
        if len(paths) == 1:
            # list of 1 directory path is not supported
            paths = paths[0]
        dataset = ds.dataset(
            paths, partitioning="hive", filesystem=None, format="parquet"
        )

        # get all (filtered) fragments
        if filters is not None:
            filter = _filters_to_expression(filters)
        else:
            filter = None

        fragments = list(dataset.get_fragments(filter=filter))

        # numeric rather than glob ordering
        # TODO how does this handle different partitioned directories?
        fragments = sorted(fragments, key=lambda f: natural_sort_key(f.path))

        if split_row_groups:
            fragments = [
                f_rg for f in fragments for f_rg in f.get_row_group_fragments(filter)
            ]

        # create dask object
        schema = dataset.schema
        # meta = schema.empty_table().to_pandas()
        # breakpoint()
        meta, categories = _get_meta_from_schema(
            schema, index, categories, [], check_partitions=False
        )

        kwargs = {"categories": categories}
        return meta, fragments, schema, filter, kwargs

    @staticmethod
    def read_partition(
        fragment, schema, columns, filter, index, categories=None, **kwargs
    ):
        if isinstance(index, list):
            for level in index:
                # unclear if we can use set ops here. I think the order matters.
                # Need the membership test to avoid duplicating index when
                # we slice with `columns` later on.
                if level not in columns:
                    columns.append(level)

        # works with pyarrow master, not with released pyarrow 0.17
        df = fragment.to_table(
            use_threads=False, schema=schema, columns=columns, filter=filter
        ).to_pandas(categories=categories)

        # Note that `to_pandas(ignore_metadata=False)` means
        # pyarrow will use the pandas metadata to set the index.

        # TODO below is copied from existing ArrowEngine method -> could be
        # split out in a helper function
        columns_and_parts = columns
        index_in_columns_and_parts = set(df.index.names).issubset(
            set(columns_and_parts)
        )
        if not index:
            if index_in_columns_and_parts:
                # User does not want to set index and a desired
                # column/partition has been set to the index
                df.reset_index(drop=False, inplace=True)
            else:
                # User does not want to set index and an
                # "unwanted" column has been set to the index
                df.reset_index(drop=True, inplace=True)
        else:
            if set(df.index.names) != set(index) and index_in_columns_and_parts:
                # The wrong index has been set and it contains
                # one or more desired columns/partitions
                df.reset_index(drop=False, inplace=True)
            elif index_in_columns_and_parts:
                # The correct index has already been set
                index = False
                columns_and_parts = list(
                    set(columns_and_parts).difference(set(df.index.names))
                )
        df = df[list(columns_and_parts)]

        if index:
            df = df.set_index(index)
        return df


def read_parquet_part(func, meta, part, schema, columns, filter, index, kwargs):
    """ Read a part of a parquet dataset

    This function is used by `read_parquet`."""
    if isinstance(part, list):
        dfs = [func(fr, schema, columns.copy(), filter, index, **kwargs) for fr in part]
        df = concat(dfs, axis=0)
    else:
        df = func(part, schema, columns.copy(), filter, index, **kwargs)

    if meta.columns.name:
        df.columns.name = meta.columns.name

    # TODO is this needed?
    # columns = columns or []
    # index = index or []
    # return df[[c for c in columns if c not in index]]
    return df
