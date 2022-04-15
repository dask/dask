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
        path,
        columns=None,
        filters=None,
        index=None,
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
    elif isinstance(engine, DatasetEngine):
        # DatasetEngine class is already initialized
        if engine_options:
            raise ValueError(
                f"engine_options not supported if the engine is already "
                f"intialized. Pass in the engine class name, or remove "
                f"the engine_options: {engine_options}."
            )
        return engine

    return engine(**engine_options)


def from_dataset(
    path,
    columns=None,
    filters=None,
    index=None,
    engine="pyarrow",
    **engine_options,
):
    """Create a Dask-DataFrame collection from a stored dataset

    WARNING: This API is completely unstable and experimental!!

    Parameters
    ----------
    path : str or list
        Source directory for data, or path(s) to individual parquet files.
        Prefix with a protocol like ``s3://`` to read from alternative
        filesystems. To read from multiple files you can pass a globstring or a
        list of paths, with the caveat that they must all have the same
        protocol.
    columns : str or list, default None
        Field name(s) to read in as columns in the output. By default all
        non-index fields will be read (as determined by the pandas parquet
        metadata, if present). Provide a single field name instead of a list to
        read in the data as a Series.
    filters : Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
        List of filters to apply, like ``[[('col1', '==', 0), ...], ...]``.
        Predicates should be expressed in disjunctive normal form (DNF). This
        means that the innermost tuple describes a single column predicate.
        These inner predicates are combined with an AND conjunction into a
        larger predicate. The outer-most list then combines all of the
        combined filters with an OR disjunction.
    index : str or False, default None
        Field name to use as the output frame index. By default, the index may
        be inferred from file metadata (if present). Use False to read all
        fields as columns.
    engine : str or DatasetEngine, default 'pyarrow'
        DatasetEngine class to use. Defaults to `ArrowDataset`, which supports
        “parquet”, “ipc”/”arrow”/”feather”, “csv”, and “orc” (the specific
        file format should be specified with the ``format=`` argument). For
        Parquet-formatted data, `engine="pyarrow-parquet"` is recommended
        (which is the same as `engine=ArrowParquetDataset`).
    **engine_options :
        Engine-specific options to use to initialize ``engine``.

    See Also
    --------
    dask.dataframe.io.dataset.arrow.ArrowDataset
    dask.dataframe.io.dataset.arrow.ArrowParquetDataset
    """

    # Create output-collection name
    label = "from-dataset-"
    input_kwargs = {
        "columns": columns,
        "filters": filters,
        "index": index,
        "engine": engine,
        "engine_options": engine_options,
    }
    output_name = label + tokenize(path, **input_kwargs)

    # Get dataset engine
    engine = get_engine(engine, **(engine_options or {}))

    # Get collection-mapping information
    io_func, fragments, meta, divisions = engine.get_collection_mapping(
        path,
        columns=columns,
        filters=filters,
        index=index,
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
