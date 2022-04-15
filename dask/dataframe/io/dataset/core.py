from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer


class ReadFunction:
    """Wrapper-function class for reading data"""

    def project_columns(self, columns):
        if columns == self.columns:
            return self
        return type(self)(columns, self.filters)

    def __call__(self, source):
        raise NotImplementedError


class DatasetEngine:
    def get_collection_mapping(
        self,
        columns=None,
        filters=None,
        index=None,
        calculate_divisions=False,
        storage_options=None,
    ):
        """Returns required inputs to from_map"""
        raise NotImplementedError


def get_engine(engine, **engine_options):
    if engine == "pyarrow":
        from dask.dataframe.io.dataset.arrow import ArrowDataset

        return ArrowDataset(**engine_options)
    elif engine == "pyarrow-parquet":
        from dask.dataframe.io.dataset.arrow import ArrowParquetDataset

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
    storage_options=None,
    **engine_options,
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
