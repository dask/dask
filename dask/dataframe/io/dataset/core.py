import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec.core import get_fs_token_paths

from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer


class DatasetEngine:
    def get_collection_mapping(
        self, columns, filters, index, calculate_divisions, storage_options
    ):
        """Returns required inputs to from_map"""
        raise NotImplementedError


class ReadArrowFileFragment:
    """Generic class for converting pyarrow file fragments to partitions"""

    def __init__(self, columns, filters, index, dataset_options):
        self.columns = columns
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        self.filters = ds_filters
        self.index = index
        self.dataset_options = dataset_options

    def project_columns(self, columns):
        if columns == self.columns:
            return self
        return type(self)(columns, self.filters)

    def __call__(self, fragment):
        if isinstance(fragment, str):
            df = (
                ds.dataset(fragment, **self.dataset_options)
                .to_table(filter=self.filters, columns=self.columns)
                .to_pandas()
            )
        else:
            df = fragment.to_table(
                filter=self.filters, columns=self.columns
            ).to_pandas()
        if self.index:
            return df.set_index(self.index)
        return df


class ArrowDataset(DatasetEngine):
    def __init__(
        self,
        partitioning="hive",
        filesystem=None,
        **dataset_options,
    ):
        self.dataset_options = dataset_options
        self.partitioning = partitioning
        self.filesystem = filesystem

    def get_dataset(self, path, columns, filters, storage_options, mode="rb"):
        """Returns an engine-specific dataset object"""

        # Check if we already have a file-system object
        if self.filesystem is None:
            self.filesystem = get_fs_token_paths(
                path, mode=mode, storage_options=storage_options
            )[0]

        return ds.dataset(
            path,
            partitioning=self.partitioning,
            filesystem=self.filesystem,
            **self.dataset_options,
        )

    def create_meta(self, dataset, index):
        """Returns an empty-DataFrame meta object"""

        # Start with simple schema -> pandas mapping
        meta = dataset.schema.empty_table().to_pandas()
        # Set index if necessary
        if index:
            return meta.set_index(index)
        return meta

    def get_read_function(self, columns, filters, index):
        """Returns a function to convert a fragment to a DataFrame partition"""

        return ReadArrowFileFragment(
            columns,
            filters,
            index,
            dataset_options={
                "partitioning": self.partitioning,
                "filesystem": self.filesystem,
                **self.dataset_options,
            },
        )

    def get_fragments(self, dataset, filters):
        ds_filters = None
        if filters is not None:
            ds_filters = pq._filters_to_expression(filters)
        return list(dataset.get_fragments(ds_filters))

    def get_divisions(self, fragments, index):
        raise ValueError(f"calculate_divisions=True not supported for {type(self)}")

    def get_collection_mapping(
        self, path, columns, filters, index, calculate_divisions, storage_options
    ):

        # Get dataset
        dataset = self.get_dataset(path, columns, filters, storage_options)

        # Create meta
        meta = self.create_meta(dataset, index)

        # Get fragments
        fragments = self.get_fragments(dataset, filters)

        # Extract divisions
        if calculate_divisions:
            divisions = self.get_divisions(fragments, index)
        else:
            divisions = (None,) * (len(fragments) + 1)

        # Get IO function
        io_func = self.get_read_function(columns, filters, index)

        return io_func, fragments, meta, divisions


class ArrowParquetDataset(ArrowDataset):
    def __init__(self, **kwargs):
        super().__init__(format="parquet", **kwargs)

    def get_fragments(self, dataset, filters):
        # For Parquet, we can convert fragments back to paths
        # to avoid large graph sizes
        return [f.path for f in super().get_fragments(dataset, filters)]


def get_engine(engine, **engine_options):
    if engine == "pyarrow":
        return ArrowDataset(**engine_options)
    elif engine in ("orc", "pyarrow-orc"):
        return ArrowDataset(format="orc", **engine_options)
    elif engine in ("parquet", "pyarrow-parquet"):
        return ArrowParquetDataset(**engine_options)
    elif not isinstance(engine, DatasetEngine):
        raise ValueError
    return engine


def from_dataset(
    path,
    columns=None,
    filters=None,
    index=None,
    calculate_divisions=False,
    engine="pyarrow",
    engine_options=None,
    storage_options=None,
):

    # Create output-collection name
    label = "from-dataset-"
    input_kwargs = {
        "columns": columns,
        "filters": filters,
        "index": index,
        "calculate_divisions": calculate_divisions,
        "engine": engine,
        "engine_options": engine_options,
        "storage_options": storage_options,
    }
    output_name = label + tokenize(path, **input_kwargs)

    # Get dataset engine
    engine = get_engine(engine, **(engine_options or {}))

    # Get collection-mapping information
    io_func, fragments, meta, divisions = engine.get_collection_mapping(
        path,
        columns,
        filters,
        index,
        calculate_divisions,
        storage_options,
    )

    # Special Case (Empty DataFrame)
    if len(divisions) < 2:
        io_func = lambda x: x
        fragments = [meta]
        divisions = (None, None)

    # Create Blockwise layer
    # TODO: use `from_map`
    layer = DataFrameIOLayer(
        output_name,
        columns,
        fragments,
        io_func,
        label=label,
        creation_info={
            "func": from_dataset,
            "args": (path,),
            "kwargs": input_kwargs,
        },
    )
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)
